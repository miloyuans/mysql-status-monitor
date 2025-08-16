package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	SlowQueryLogPath   string  `mapstructure:"slow_query_log_path"`
}

type Alert struct {
	Message string
}

type SlowQuery struct {
	SQLText   string
	QueryTime float64
}

type AlertTracker struct {
	sync.Mutex
	lastSent map[string]time.Time
}

func (at *AlertTracker) CanSend(message string, now time.Time) bool {
	at.Lock()
	defer at.Unlock()
	last, exists := at.lastSent[message]
	if !exists || now.Sub(last) >= 15*time.Minute {
		at.lastSent[message] = now
		return true
	}
	return false
}

var alertTracker = &AlertTracker{
	lastSent: make(map[string]time.Time),
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
			sendTelegramAlert(alerts, config)
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
	viper.SetDefault("slow_query_log_path", "/var/log/mysql/mysql-slow.log")

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
	now := time.Now()

	// Check CPU
	cpuUsage, err := getCPUUsage()
	if err == nil && cpuUsage > config.CPUThreshold {
		message := fmt.Sprintf("**CPU使用率异常:** %.2f%% (阈值: %.2f%%)", cpuUsage, config.CPUThreshold)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check Memory
	memFreePercent, err := getMemFreePercent()
	if err == nil && memFreePercent < config.MemThreshold {
		message := fmt.Sprintf("**内存空闲异常:** %.2f%% (阈值: %.2f%%)", memFreePercent, config.MemThreshold)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check Network IO
	netIO, err := getNetIO()
	if err == nil && netIO > config.NetIOThreshold {
		message := fmt.Sprintf("**网络IO异常:** %d bytes/sec (阈值: %d)", netIO, config.NetIOThreshold)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check Disk IO
	diskIO, err := getDiskIO()
	if err == nil && diskIO > config.DiskIOThreshold {
		message := fmt.Sprintf("**磁盘IO异常:** %d bytes/sec (阈值: %d)", diskIO, config.DiskIOThreshold)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check Disk Usage
	diskUsage, err := getDiskUsage("/")
	if err == nil && diskUsage > config.DiskUsageThreshold {
		message := fmt.Sprintf("**磁盘使用率异常:** %.2f%% (阈值: %.2f%%)", diskUsage, config.DiskUsageThreshold)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// MySQL checks
	db, err := sql.Open("mysql", config.MySQLDSN)
	if err != nil {
		message := fmt.Sprintf("**MySQL连接失败:** %s", sanitizeMarkdown(err.Error()))
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
		return alerts
	}
	defer db.Close()

	// Check if running
	if err := db.Ping(); err != nil {
		message := fmt.Sprintf("**MySQL未运行:** %s", sanitizeMarkdown(err.Error()))
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check slave status
	slaveStatus, err := checkSlaveStatus(db)
	if err == nil && (slaveStatus["Slave_IO_Running"] != "Yes" || slaveStatus["Slave_SQL_Running"] != "Yes") {
		message := fmt.Sprintf("**MySQL从库异常:** IO: %s, SQL: %s", sanitizeMarkdown(slaveStatus["Slave_IO_Running"]), sanitizeMarkdown(slaveStatus["Slave_SQL_Running"]))
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check deadlocks
	deadlocks, err := checkDeadlocks(db)
	if err == nil && deadlocks > 0 {
		message := fmt.Sprintf("**MySQL检测到死锁:** %d", deadlocks)
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
	}

	// Check slow queries
	slowQueries, err := checkSlowQueries(db)
	if err == nil && slowQueries > config.SlowQueryThreshold {
		filename := fmt.Sprintf("slow_queries_%s.txt", time.Now().Format("20060102150405"))
		message := fmt.Sprintf("**MySQL慢查询异常:** %d (阈值: %d)\n慢查询详情见附件 %s", slowQueries, config.SlowQueryThreshold, filename)
		if alertTracker.CanSend(message, now) {
			alertMsg := message
			topSlowSQLs, err := getTopSlowQueries(config.SlowQueryLogPath)
			if err == nil && len(topSlowSQLs) > 0 {
				// Save top 5 queries to file
				err = saveSlowQueriesToFile(topSlowSQLs, filename)
				if err != nil {
					alertMsg += fmt.Sprintf("\n**保存慢查询文件错误:** %s", sanitizeMarkdown(err.Error()))
				}
			} else if err != nil {
				alertMsg += fmt.Sprintf("\n**慢查询日志错误:** %s", sanitizeMarkdown(err.Error()))
			}
			alerts = append(alerts, Alert{Message: alertMsg})
		}
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

func getTopSlowQueries(logPath string) ([]SlowQuery, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("无法打开慢查询日志: %w", err)
	}
	defer file.Close()

	var queries []SlowQuery
	var currentQuery strings.Builder
	var queryTime float64
	queryTimeRegex := regexp.MustCompile(`# Query_time: (\d+\.\d+).*?\n`)
	sqlStartRegex := regexp.MustCompile(`^(SELECT|INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE).*`)
	nonSQLRegex := regexp.MustCompile(`^(use |SET |throttle:).*`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("Parsing slow query log line: %s", line)
		if queryTimeRegex.MatchString(line) {
			// Extract query time
			matches := queryTimeRegex.FindStringSubmatch(line)
			if len(matches) > 1 {
				queryTime, _ = strconv.ParseFloat(matches[1], 64)
				log.Printf("Parsed Query_time: %.3f", queryTime)
			}
		} else if sqlStartRegex.MatchString(line) {
			// Save previous query if exists
			if currentQuery.Len() > 0 {
				queries = append(queries, SlowQuery{
					SQLText:   strings.TrimSpace(currentQuery.String()),
					QueryTime: queryTime,
				})
				log.Printf("Added query: %s, Time: %.3f", currentQuery.String(), queryTime)
				currentQuery.Reset()
			}
			currentQuery.WriteString(line + "\n")
		} else if !nonSQLRegex.MatchString(line) && currentQuery.Len() > 0 {
			// Continue building current query, excluding non-SQL lines
			currentQuery.WriteString(line + "\n")
		}
	}

	// Add the last query
	if currentQuery.Len() > 0 {
		queries = append(queries, SlowQuery{
			SQLText:   strings.TrimSpace(currentQuery.String()),
			QueryTime: queryTime,
		})
		log.Printf("Added final query: %s, Time: %.3f", currentQuery.String(), queryTime)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取慢查询日志错误: %w", err)
	}

	// Sort queries by query time (descending) and take top 5
	for i := 0; i < len(queries)-1; i++ {
		for j := i + 1; j < len(queries); j++ {
			if queries[i].QueryTime < queries[j].QueryTime {
				queries[i], queries[j] = queries[j], queries[i]
			}
		}
	}

	if len(queries) > 5 {
		queries = queries[:5]
	}

	log.Printf("Top queries: %+v", queries)
	return queries, nil
}

func saveSlowQueriesToFile(queries []SlowQuery, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("无法创建慢查询文件: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for i, query := range queries {
		_, err := writer.WriteString(fmt.Sprintf("Query %d: (执行时间: %.3fs)\n%s\n\n", i+1, query.QueryTime, formatSQL(query.SQLText)))
		if err != nil {
			return fmt.Errorf("写入慢查询文件错误: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("刷新慢查询文件错误: %w", err)
	}
	return nil
}

// formatSQL formats SQL text for better readability
func formatSQL(sql string) string {
	// Remove extra newlines and trim
	sql = strings.TrimSpace(sql)
	// Split into lines and format
	lines := strings.Split(sql, "\n")
	var formatted []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			// Add indentation for non-first lines
			if len(formatted) > 0 {
				trimmed = "  " + trimmed
			}
			// Truncate long lines to avoid Telegram message size limit
			if len(trimmed) > 1000 {
				trimmed = trimmed[:1000] + "..."
			}
			formatted = append(formatted, trimmed)
		}
	}
	formattedSQL := strings.Join(formatted, "\n")
	// Ensure file size is manageable (Telegram limit: 50MB)
	if len(formattedSQL) > 1000000 {
		formattedSQL = formattedSQL[:1000000] + "..."
	}
	return formattedSQL
}

// sanitizeMarkdown escapes special Markdown characters to prevent parsing errors
func sanitizeMarkdown(s string) string {
	specialChars := []string{"_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"}
	for _, char := range specialChars {
		s = strings.ReplaceAll(s, char, "\\"+char)
	}
	// Additional escaping for quotes and backticks to handle SQL edge cases
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "`", "\\`")
	return s
}

func sendTelegramAlert(alerts []Alert, config Config) {
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
	sb.WriteString("**系统告警**\n\n")
	for _, alert := range uniqueList {
		sb.WriteString(fmt.Sprintf("`%s`\n\n", alert.Message))
	}

	message := sb.String()
	log.Printf("Sending Telegram message (length: %d bytes): %s", len(message), message)

	// Prepare form data for file upload
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add text message
	err := writer.WriteField("chat_id", config.TelegramChatID)
	if err != nil {
		log.Printf("Failed to add chat_id to form: %v", err)
		return
	}
	err = writer.WriteField("text", message)
	if err != nil {
		log.Printf("Failed to add text to form: %v", err)
		return
	}
	err = writer.WriteField("parse_mode", "Markdown")
	if err != nil {
		log.Printf("Failed to add parse_mode to form: %v", err)
		return
	}

	// Add slow_queries_YYYYMMDDHHMMSS.txt if it exists
	filename := fmt.Sprintf("slow_queries_%s.txt", time.Now().Format("20060102150405"))
	if _, err := os.Stat(filename); err == nil {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Failed to open %s: %v", filename, err)
			return
		}
		defer file.Close()

		part, err := writer.CreateFormFile("document", filename)
		if err != nil {
			log.Printf("Failed to create form file: %v", err)
			return
		}
		_, err = io.Copy(part, file)
		if err != nil {
			log.Printf("Failed to copy file to form: %v", err)
			return
		}
	}

	err = writer.Close()
	if err != nil {
		log.Printf("Failed to close form writer: %v", err)
		return
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", config.TelegramToken)
	resp, err := http.Post(url, writer.FormDataContentType(), body)
	if err != nil {
		log.Printf("Failed to send Telegram document: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Telegram error: %s", string(bodyBytes))
	}

	// Clean up slow_queries_YYYYMMDDHHMMSS.txt
	if err := os.Remove(filename); err != nil {
		log.Printf("Failed to remove %s: %v", filename, err)
	}
}
