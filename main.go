package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
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
	NetIOThreshold     float64 `mapstructure:"net_io_threshold"`  // Changed to float64 for GB/s
	DiskIOThreshold    float64 `mapstructure:"disk_io_threshold"` // Changed to float64 for GB/s
	DiskUsageThreshold float64 `mapstructure:"disk_usage_threshold"`
	SlowQueryThreshold int     `mapstructure:"slow_query_threshold"`
	SlowQueryLogPath   string  `mapstructure:"slow_query_log_path"`
	SlowQueryFilePath  string  `mapstructure:"slow_query_file_path"`
	ClusterName        string  `mapstructure:"cluster_name"`
}

type Alert struct {
	Message  string
	Filename string
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

var lastSlowQueryContent string // Store content of the last slow query file
var lastSlowQueries int        // Store the last slow query count for delta tracking

func main() {
	// Load configuration from file
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if config.TelegramToken == "" || config.TelegramChatID == "" {
		log.Fatal("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID are required")
	}

	if config.ClusterName == "" {
		log.Fatal("CLUSTER_NAME is required")
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
	viper.SetDefault("net_io_threshold", 0.1) // 100MB/s = 0.1GB/s
	viper.SetDefault("disk_io_threshold", 0.05) // 50MB/s = 0.05GB/s
	viper.SetDefault("disk_usage_threshold", 80.0)
	viper.SetDefault("slow_query_threshold", 1)
	viper.SetDefault("slow_query_log_path", "/var/log/mysql/mysql-slow.log")
	viper.SetDefault("slow_query_file_path", "/var/log/system-monitor/")
	viper.SetDefault("cluster_name", "")

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
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v", err)
		hostname = "unknown"
	}
	hostname = sanitizeFilename(hostname)

	// Initialize alert message builder
	var alertMsg strings.Builder
	alertMsg.WriteString(fmt.Sprintf("**服务告警 [%s]**\n\n", config.ClusterName))
	alertMsg.WriteString(fmt.Sprintf("**服务环境**: %s\n", config.ClusterName))
	alertMsg.WriteString(fmt.Sprintf("**主机名**: %s\n", hostname))

	// Check CPU
	cpuUsage, err := getCPUUsage()
	cpuStatus := "正常"
	if err == nil && cpuUsage > config.CPUThreshold {
		cpuStatus = fmt.Sprintf("异常: %.2f%% (阈值: %.2f%%)", cpuUsage, config.CPUThreshold)
	} else if err != nil {
		cpuStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**CPU使用率**: %s\n", cpuStatus))

	// Check Memory
	memFreePercent, err := getMemFreePercent()
	memStatus := "正常"
	if err == nil && memFreePercent < config.MemThreshold {
		memStatus = fmt.Sprintf("异常: %.2f%% (阈值: %.2f%%)", memFreePercent, config.MemThreshold)
	} else if err != nil {
		memStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**内存剩余率**: %s\n", memStatus))

	// Check Network IO
	netIO, err := getNetIO()
	netIOStatus := "正常"
	if err == nil && netIO > config.NetIOThreshold {
		netIOStatus = fmt.Sprintf("异常: %.3f GB/s (阈值: %.3f GB/s)", netIO, config.NetIOThreshold)
	} else if err != nil {
		netIOStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**网络IO使用率**: %s\n", netIOStatus))

	// Check Disk IO
	diskIO, err := getDiskIO()
	diskIOStatus := "正常"
	if err == nil && diskIO > config.DiskIOThreshold {
		diskIOStatus = fmt.Sprintf("异常: %.3f GB/s (阈值: %.3f GB/s)", diskIO, config.DiskIOThreshold)
	} else if err != nil {
		diskIOStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**磁盘IO使用率**: %s\n", diskIOStatus))

	// Check Disk Usage
	diskUsage, err := getDiskUsage("/")
	diskUsageStatus := "正常"
	if err == nil && diskUsage > config.DiskUsageThreshold {
		diskUsageStatus = fmt.Sprintf("异常: %.2f%% (阈值: %.2f%%)", diskUsage, config.DiskUsageThreshold)
	} else if err != nil {
		diskUsageStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**磁盘使用率**: %s\n", diskUsageStatus))

	// MySQL checks
	db, err := sql.Open("mysql", config.MySQLDSN)
	if err != nil {
		alertMsg.WriteString(fmt.Sprintf("**数据库连接状态**: 失败: %s\n", sanitizeMarkdown(err.Error())))
		alertMsg.WriteString("**数据库主从状态**: 未知\n")
		alertMsg.WriteString("**数据库死锁状态**: 未知\n")
		alertMsg.WriteString("**数据库慢查询状态**: 未知\n")
		alertMsg.WriteString("**数据库慢查询语句详情文件**: 无\n")
		message := alertMsg.String()
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
		return alerts
	}
	defer db.Close()

	// Check if running
	if err := db.Ping(); err != nil {
		alertMsg.WriteString(fmt.Sprintf("**数据库运行状态**: 未运行: %s\n", sanitizeMarkdown(err.Error())))
		alertMsg.WriteString("**数据库主从状态**: 未知\n")
		alertMsg.WriteString("**数据库死锁状态**: 未知\n")
		alertMsg.WriteString("**数据库慢查询状态**: 未知\n")
		alertMsg.WriteString("**数据库慢查询语句详情文件**: 无\n")
		message := alertMsg.String()
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message})
		}
		return alerts
	}

	// Check slave status
	slaveStatus, err := checkSlaveStatus(db)
	slaveStatusStr := "正常"
	if err == nil && (slaveStatus["Slave_IO_Running"] != "Yes" || slaveStatus["Slave_SQL_Running"] != "Yes") {
		slaveStatusStr = fmt.Sprintf("异常: IO: %s, SQL: %s", sanitizeMarkdown(slaveStatus["Slave_IO_Running"]), sanitizeMarkdown(slaveStatus["Slave_SQL_Running"]))
	} else if err != nil {
		slaveStatusStr = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**数据库主从状态**: %s\n", slaveStatusStr))

	// Check deadlocks
	deadlocks, err := checkDeadlocks(db)
	deadlockStatus := "正常"
	if err == nil && deadlocks > 0 {
		deadlockStatus = fmt.Sprintf("异常: %d", deadlocks)
	} else if err != nil {
		deadlockStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**数据库死锁状态**: %s\n", deadlockStatus))

	// Check slow queries
	slowQueries, err := checkSlowQueries(db)
	slowQueryStatus := "正常"
	var filename string
	if err == nil && slowQueries > config.SlowQueryThreshold {
		totalQueryTime := 0.0
		topSlowSQLs, err := getTopSlowQueries(config.SlowQueryLogPath)
		if err == nil && len(topSlowSQLs) > 0 {
			for _, query := range topSlowSQLs {
				totalQueryTime += query.QueryTime
			}
			filename = filepath.Join(config.SlowQueryFilePath, fmt.Sprintf("slow_queries_%s_%s.txt", now.Format("20060102150405"), hostname))
			err = saveSlowQueriesToFile(topSlowSQLs, filename)
			if err != nil {
				slowQueryStatus = fmt.Sprintf("异常: %d (阈值: %d, 总执行时间: %.3fs, 保存文件错误: %s)", slowQueries, config.SlowQueryThreshold, totalQueryTime, sanitizeMarkdown(err.Error()))
			} else {
				// Check file content for duplicates
				newContent, err := os.ReadFile(filename)
				if err != nil {
					slowQueryStatus = fmt.Sprintf("异常: %d (阈值: %d, 总执行时间: %.3fs, 读取文件错误: %s)", slowQueries, config.SlowQueryThreshold, totalQueryTime, sanitizeMarkdown(err.Error()))
				} else if string(newContent) != lastSlowQueryContent {
					lastSlowQueryContent = string(newContent)
					slowQueryStatus = fmt.Sprintf("异常: %d (阈值: %d, 总执行时间: %.3fs)", slowQueries, config.SlowQueryThreshold, totalQueryTime)
				} else {
					log.Printf("Skipping slow query alert: content identical to previous")
					if err := os.Remove(filename); err != nil {
						log.Printf("Failed to remove %s: %v", filename, err)
					}
					filename = ""
					slowQueryStatus = fmt.Sprintf("异常: %d (阈值: %d, 总执行时间: %.3fs, 重复内容已忽略)", slowQueries, config.SlowQueryThreshold, totalQueryTime)
				}
			}
		} else if err != nil {
			slowQueryStatus = fmt.Sprintf("异常: %d (阈值: %d, 日志错误: %s)", slowQueries, config.SlowQueryThreshold, sanitizeMarkdown(err.Error()))
		}
	} else if err != nil {
		slowQueryStatus = fmt.Sprintf("错误: %s", sanitizeMarkdown(err.Error()))
	}
	alertMsg.WriteString(fmt.Sprintf("**数据库慢查询状态**: %s\n", slowQueryStatus))

	// Slow query file
	slowQueryFileStatus := "无"
	if filename != "" {
		slowQueryFileStatus = filepath.Base(filename)
	}
	alertMsg.WriteString(fmt.Sprintf("**数据库慢查询语句详情文件**: %s\n", slowQueryFileStatus))

	// Send alert if there are issues
	message := alertMsg.String()
	if cpuStatus != "正常" || memStatus != "正常" || netIOStatus != "正常" || diskIOStatus != "正常" || diskUsageStatus != "正常" ||
		slaveStatusStr != "正常" || deadlockStatus != "正常" || slowQueryStatus != "正常" {
		if alertTracker.CanSend(message, now) {
			alerts = append(alerts, Alert{Message: message, Filename: filename})
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

func getNetIO() (float64, error) {
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
		bytesPerSec := (ioCounters2[0].BytesSent + ioCounters2[0].BytesRecv) - (ioCounters[0].BytesSent + ioCounters[0].BytesRecv)
		return float64(bytesPerSec) / 1_073_741_824, nil // Convert bytes to GB
	}
	return 0, fmt.Errorf("no net data")
}

func getDiskIO() (float64, error) {
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
	bytesPerSec := (totalRead2 - totalRead) + (totalWrite2 - totalWrite)
	return float64(bytesPerSec) / 1_073_741_824, nil // Convert bytes to GB
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
	delta := slowQueries - lastSlowQueries
	lastSlowQueries = slowQueries
	log.Printf("Slow queries: %d, Last: %d, Delta: %d", slowQueries, lastSlowQueries, delta)
	return delta, nil
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
	// Ensure directory exists
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("无法创建目录 %s: %w", dir, err)
	}

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

	// Check file size (Telegram limit: 50MB)
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return fmt.Errorf("无法获取文件大小: %w", err)
	}
	if fileInfo.Size() > 50*1024*1024 {
		return fmt.Errorf("慢查询文件 %s 超过50MB限制", filename)
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
	// Ensure message size is manageable (Telegram limit: ~4096 characters for text)
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

// sanitizeFilename replaces invalid characters for file names
func sanitizeFilename(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, ":", "_")
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

	for _, alert := range uniqueList {
		// Prepare form data
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		// Add text message
		err := writer.WriteField("chat_id", config.TelegramChatID)
		if err != nil {
			log.Printf("Failed to add chat_id to form: %v", err)
			continue
		}
		err = writer.WriteField("text", alert.Message)
		if err != nil {
			log.Printf("Failed to add text to form: %v", err)
			continue
		}
		err = writer.WriteField("parse_mode", "Markdown")
		if err != nil {
			log.Printf("Failed to add parse_mode to form: %v", err)
			continue
		}

		// Add file if it exists
		hasFile := false
		if alert.Filename != "" {
			if _, err := os.Stat(alert.Filename); err == nil {
				file, err := os.Open(alert.Filename)
				if err != nil {
					log.Printf("Failed to open %s: %v", alert.Filename, err)
					continue
				}
				defer file.Close()

				part, err := writer.CreateFormFile("document", filepath.Base(alert.Filename))
				if err != nil {
					log.Printf("Failed to create form file: %v", err)
					continue
				}
				_, err = io.Copy(part, file)
				if err != nil {
					log.Printf("Failed to copy file to form: %v", err)
					continue
				}
				hasFile = true
			} else {
				log.Printf("File %s does not exist: %v", alert.Filename, err)
			}
		}

		err = writer.Close()
		if err != nil {
			log.Printf("Failed to close form writer: %v", err)
			continue
		}

		// Choose API endpoint
		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.TelegramToken)
		if hasFile {
			url = fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", config.TelegramToken)
		}

		// Retry up to 3 times
		for retries := 0; retries < 3; retries++ {
			resp, err := http.Post(url, writer.FormDataContentType(), body)
			if err != nil {
				log.Printf("Failed to send Telegram request (attempt %d): %v", retries+1, err)
				time.Sleep(time.Second * time.Duration(retries+1))
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("Telegram error (attempt %d): %s", retries+1, string(bodyBytes))
				// Fallback to plain text if Markdown fails
				if strings.Contains(string(bodyBytes), "can't parse entities") {
					body = &bytes.Buffer{}
					writer = multipart.NewWriter(body)
					writer.WriteField("chat_id", config.TelegramChatID)
					writer.WriteField("text", alert.Message)
					writer.Close()
					resp, err = http.Post(fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.TelegramToken), writer.FormDataContentType(), body)
					if err != nil {
						log.Printf("Failed to send plain text Telegram request (attempt %d): %v", retries+1, err)
						time.Sleep(time.Second * time.Duration(retries+1))
						continue
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						bodyBytes, _ = io.ReadAll(resp.Body)
						log.Printf("Telegram plain text error (attempt %d): %s", retries+1, string(bodyBytes))
					}
				}
				time.Sleep(time.Second * time.Duration(retries+1))
				continue
			}
			// Clean up file only after successful send
			if hasFile && alert.Filename != "" {
				if err := os.Remove(alert.Filename); err != nil {
					log.Printf("Failed to remove %s: %v", alert.Filename, err)
				}
			}
			log.Printf("Successfully sent Telegram alert: %s", alert.Message)
			break
		}
	}
}
