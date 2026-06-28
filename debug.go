package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// ==================== 调试模块 ====================

// DebugModule 调试模块，按需加载
type DebugModule struct {
	enabled bool
}

var Debug *DebugModule

// InitDebug 初始化调试模块
func InitDebug(enabled bool) {
	Debug = &DebugModule{enabled: enabled}
	if enabled {
		log.Println("[Debug] 调试模块已启用")
	}
}

// IsEnabled 检查调试模式是否启用
func (d *DebugModule) IsEnabled() bool {
	return d != nil && d.enabled
}

// LogDebug 调试日志输出
func (d *DebugModule) LogDebug(format string, args ...interface{}) {
	if !d.IsEnabled() {
		return
	}
	msg := fmt.Sprintf(format, args...)
	log.Printf("[DEBUG] %s\n", msg)
}

// LogContext 调试输出Ctx内容
func (d *DebugModule) LogContext(ctx *Ctx) {
	if !d.IsEnabled() {
		return
	}

	data, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		log.Printf("[DEBUG] 序列化Ctx失败: %v\n", err)
		return
	}
	log.Printf("[DEBUG] Ctx详情:\n%s\n", string(data))
}

// LogJudgeResult 调试输出判题结果
func (d *DebugModule) LogJudgeResult(userID, groupID int64, result JudgeResult) {
	if !d.IsEnabled() {
		return
	}
	log.Printf("[DEBUG] 判题结果: userID=%d groupID=%d IsValid=%v Reason=%s\n",
		userID, groupID, result.IsValid, result.Reason)
}

// LogRoute 调试输出路由决策
func (d *DebugModule) LogRoute(ctx *Ctx, target string, reason string) {
	if !d.IsEnabled() {
		return
	}
	log.Printf("[DEBUG] 路由决策: userID=%d groupID=%d → %s (原因: %s)\n",
		ctx.UserID, ctx.GroupID, target, reason)
}

// DumpGoroutines 输出当前协程信息
func (d *DebugModule) DumpGoroutines() {
	if !d.IsEnabled() {
		return
	}

	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	log.Printf("[DEBUG] 协程堆栈:\n%s\n", buf[:n])
}

// DumpMemoryStats 输出内存统计
func (d *DebugModule) DumpMemoryStats() {
	if !d.IsEnabled() {
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Printf("[DEBUG] 内存统计:\n")
	log.Printf("  Alloc = %v MB\n", m.Alloc/1024/1024)
	log.Printf("  TotalAlloc = %v MB\n", m.TotalAlloc/1024/1024)
	log.Printf("  Sys = %v MB\n", m.Sys/1024/1024)
	log.Printf("  NumGC = %v\n", m.NumGC)
	log.Printf("  Goroutines = %d\n", runtime.NumGoroutine())
}

// StartPeriodicDump 启动定期状态dump
func (d *DebugModule) StartPeriodicDump(interval time.Duration) {
	if !d.IsEnabled() {
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Debug] 定期dump goroutine panic: %v", r)
			}
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			d.DumpMemoryStats()
		}
	}()

	log.Printf("[Debug] 定期状态dump已启动，间隔: %v\n", interval)
}

// ==================== 日志文件输出模块 ====================

// initFileLogger 初始化日志文件输出
// 创建logs目录，设置log输出同时写入文件和stdout
func initFileLogger() error {
	// 创建logs目录
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return fmt.Errorf("创建logs目录失败: %w", err)
	}

	// 生成日志文件名（按日期）
	logFileName := filepath.Join(logsDir, fmt.Sprintf("gows_%s.log", time.Now().Format("2006-01-02")))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("打开日志文件失败: %w", err)
	}

	// 设置log同时输出到文件和stdout
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 启动日志文件按天轮转检查
	go rotateLogFile(logFile, logsDir)

	return nil
}

// rotateLogFile 每天检查并轮转日志文件
func rotateLogFile(currentFile *os.File, logsDir string) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		// 检查是否跨天
		expectedName := filepath.Join(logsDir, fmt.Sprintf("gows_%s.log", now.Format("2006-01-02")))
		currentName := currentFile.Name()

		if expectedName != currentName {
			// 关闭旧文件
			currentFile.Close()

			// 打开新文件
			newFile, err := os.OpenFile(expectedName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				log.Printf("[Log] 打开新日志文件失败: %v\n", err)
				continue
			}

			// 更新log输出
			multiWriter := io.MultiWriter(os.Stdout, newFile)
			log.SetOutput(multiWriter)
			currentFile = newFile

			log.Printf("[Log] 日志文件已轮转至: %s\n", expectedName)
		}
	}
}
