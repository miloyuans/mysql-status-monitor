package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/spf13/viper"
)

type Config struct {
	MySQLDSN           string  `mapstructure:"mysql_dsn"`
	TelegramToken      string  `mapstructure:"telegram_token"`
	TelegramChatID     string  `mapstructure:"telegram_chat_id"`
	MonitorInterval    string  `mapstructure:"monitor_interval"`
	CPUThreshold       float64 `mapstructure:"cpu_threshold"`
	MemThreshold       float64 `mapstructure:"mem_threshold"`
	NetIOThreshold     uint64  `mapstructure:"net_io_threshold"`
	DiskIOThreshold    uint64  `mapstructure:"disk_io_threshold"`
	DiskUsageThreshold float64 `mapstructure:"disk_usage_threshold"`
	SlowQueryThreshold int     `mapstructure:"slow_query_threshold"`
}

type Alert struct {
	Message string
}

func main() {
	// Load configuration from file
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if config.TelegramToken == "" || config.TelegramChatID == "" {
		log.Fatal("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID are required")
	}

	monitorInterval, err := time.ParseDuration(config.MonitorInterval)
	if err != nil {
		log.Fatalf("Invalid monitor_interval: %v", err)
	}

	ticker := time.NewTicker(monitorInterval)
	for range ticker.C {
		alerts := checkAll(config)
		if len(alerts) > 0 {
			sendTelegramAlert(alerts, config.TelegramToken, config.TelegramChatID)
		}
	}
}

func loadConfig() (Config, error) {
	var config Config
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.SetDefault("mysql_dsn", "user:password@tcp(127.0.0.1:3306)/")
	viper.SetDefault("telegram_token", "")
	viper.SetDefault("telegram_chat_id", "")
	viper.SetDefault("monitor_interval", "1m")
	viper.SetDefault("cpu_threshold", 80.0)
	viper.SetDefault("mem_threshold", 10.0)
	viper.SetDefault("net_io_threshold", 100000000) // 100MB/s
	viper.SetDefault("disk_io_threshold", 50000000)  // 50MB/s
	viper.SetDefault("disk_usage_threshold", 80.0)
	viper.SetDefault("slow_query_threshold", 1)

	if err := viper.ReadInConfig(); err != nil {
		return config, fmt.Errorf("error reading config file: %w", err)
	}

	if err := viper.Unmarshal(&config); err != nil {
		return config, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return config, nil
}

func checkAll(config Config) []Alert {
	var alerts []Alert

	// Check CPU
	cpuUsage, err := getCPUUsage()
	if err == nil && cpuUsage > config.CPUThreshold {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**CPU Usage Abnormal:** %.2f%% (threshold: %.2f%%)", cpuUsage, config.CPUThreshold)})
	}

	// Check Memory
	memFreePercent, err := getMemFreePercent()
	if err == nil && memFreePercent < config.MemThreshold {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**Memory Free Abnormal:** %.2f%% (threshold: %.2f%%)", memFreePercent, config.MemThreshold)})
	}

	// Check Network IO
	netIO, err := getNetIO()
	if err == nil && netIO > config.NetIOThreshold {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**Network IO Abnormal:** %d bytes/sec (threshold: %d)", netIO, config.NetIOThreshold)})
	}

	// Check Disk IO
	diskIO, err := getDiskIO()
	if err == nil && diskIO > config.DiskIOThreshold {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**Disk IO Abnormal:** %d bytes/sec (threshold: %d)", diskIO, config.DiskIOThreshold)})
	}

	// Check Disk Usage
	diskUsage, err := getDiskUsage("/")
	if err == nil && diskUsage > config.DiskUsageThreshold {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**Disk Usage Abnormal:** %.2f%% (threshold: %.2f%%)", diskUsage, config.DiskUsageThreshold)})
	}

	// MySQL checks
	db, err := sql.Open("mysql", config.MySQLDSN)
	if err != nil {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**MySQL Connection Failed:** %s", sanitizeMarkdown(err.Error()))})
		return alerts
	}
	defer db.Close()

	// Check if running
	if err := db.Ping(); err != nil {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**MySQL Not Running:** %s", sanitizeMarkdown(err.Error()))})
	}

	// Check slave status
	slaveStatus, err := checkSlaveStatus(db)
	if err == nil && (slaveStatus["Slave_IO_Running"] != "Yes" || slaveStatus["Slave_SQL_Running"] != "Yes") {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**MySQL Slave Abnormal:** IO: %s, SQL: %s", sanitizeMarkdown(slaveStatus["Slave_IO_Running"]), sanitizeMarkdown(slaveStatus["Slave_SQL_Running"]))})
	}

	// Check deadlocks
	deadlocks, err := checkDeadlocks(db)
	if err == nil && deadlocks > 0 {
		alerts = append(alerts, Alert{Message: fmt.Sprintf("**MySQL Deadlocks Detected:** %d", deadlocks)})
	}

	// Check slow queries
	slowQueries, err := checkSlowQueries(db)
	if err == nil && slowQueries > config.SlowQueryThreshold {
		alertMsg := fmt.Sprintf("**MySQL Slow Queries:** %d (threshold: %d)", slowQueries, config.SlowQueryThreshold)
		topSlowSQLs, err := getTopSlowQueries(db)
		if err == nil && len(topSlowSQLs) > 0 {
			alertMsg += "\nTop 3 Slowest SQL:\n"
			for i, sql := range topSlowSQLs {
				alertMsg += fmt.Sprintf("%d. %s\n", i+1, sanitizeMarkdown(sql))
			}
		}
		alerts = append(alerts, Alert{Message: alertMsg})
	}

	return alerts
}

func getCPUUsage() (float64, error) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return 0, err
	}
	if len(percent) > 0 {
		return percent[0], nil
	}
	return 0, fmt.Errorf("no CPU data")
}

func getMemFreePercent() (float64, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return float64(v.Available) * 100 / float64(v.Total), nil
}

func getNetIO() (uint64, error) {
	ioCounters, err := net.IOCounters(false)
	if err != nil {
		return 0, err
	}
	if len(ioCounters) > 0 {
		time.Sleep(time.Second)
		ioCounters2, err := net.IOCounters(false)
		if err != nil {
			return 0, err
		}
		return (ioCounters2[0].BytesSent + ioCounters2[0].BytesRecv) - (ioCounters[0].BytesSent + ioCounters[0].BytesRecv), nil
	}
	return 0, fmt.Errorf("no net data")
}

func getDiskIO() (uint64, error) {
	ioCounters, err := disk.IOCounters()
	if err != nil {
		return 0, err
	}
	var totalRead, totalWrite uint64
	for _, io := range ioCounters {
		totalRead += io.ReadBytes
		totalWrite += io.WriteBytes
	}
	time.Sleep(time.Second)
	ioCounters2, err := disk.IOCounters()
	if err != nil {
		return 0, err
	}
	var totalRead2, totalWrite2 uint64
	for _, io := range ioCounters2 {
		totalRead2 += io.ReadBytes
		totalWrite2 += io.WriteBytes
	}
	return (totalRead2 - totalRead) + (totalWrite2 - totalWrite), nil
}

func getDiskUsage(path string) (float64, error) {
	u, err := disk.Usage(path)
	if err != nil {
		return 0, err
	}
	return u.UsedPercent, nil
}

func checkSlaveStatus(db *sql.DB) (map[string]string, error) {
	rows, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, fmt.Errorf("no slave status")
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err = rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	status := make(map[string]string)
	for i, col := range cols {
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			status[col] = string(b)
		} else {
			status[col] = fmt.Sprintf("%v", val)
		}
	}
	return status, nil
}

func checkDeadlocks(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'").Scan(&count)
	return count, err
}

func checkSlowQueries(db *sql.DB) (int, error) {
	var slowQueries int
	var variableName string
	err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Slow_queries'").Scan(&variableName, &slowQueries)
	if err != nil {
		return 0, err
	}
	return slowQueries, nil
}

func getTopSlowQueries(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT sql_text, query_time FROM mysql.slow_log ORDER BY query_time DESC LIMIT 3")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var topSQLs []string
	for rows.Next() {
		var sqlText string
		var queryTime float64
		if err := rows.Scan(&sqlText, &queryTime); err != nil {
			return nil, err
		}
		topSQLs = append(topSQLs, fmt.Sprintf("SQL: %s, Time: %.2fs", sqlText, queryTime))
	}
	return topSQLs, rows.Err()
}

// sanitizeMarkdown escapes special Markdown characters to prevent parsing errors
func sanitizeMarkdown(s string) string {
	specialChars := []string{"_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"}
	for _, char := range specialChars {
		s = strings.ReplaceAll(s, char, "\\"+char)
	}
	return s
}

func sendTelegramAlert(alerts []Alert, telegramToken, telegramChatID string) {
	// Merge duplicate alerts
	uniqueAlerts := make(map[string]struct{})
	var uniqueList []Alert
	for _, alert := range alerts {
		if _, exists := uniqueAlerts[alert.Message]; !exists {
			uniqueAlerts[alert.Message] = struct{}{}
			uniqueList = append(uniqueList, alert)
		}
	}

	var sb strings.Builder
	sb.WriteString("**System Alert**\n\n")
	for _, alert := range uniqueList {
		sb.WriteString(fmt.Sprintf("`%s`\n\n", alert.Message))
	}

	log.Printf("Sending Telegram message: %s", sb.String())

	type TelegramMessage struct {
		ChatID    string `json:"chat_id"`
		Text      string `json:"text"`
		ParseMode string `json:"parse_mode"`
	}

	msg := TelegramMessage{
		ChatID:    telegramChatID,
		Text:      sb.String(),
		ParseMode: "Markdown",
	}

	body, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal Telegram message: %v", err)
		return
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", telegramToken)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to send alert: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Telegram error: %s", string(bodyBytes))
	}
}
