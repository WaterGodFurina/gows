package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	ConfigFilePath = "WebuiData.json"
	WebUIPort      = 8080
)

var configUpdateMu = struct {
	locked chan struct{}
}{locked: make(chan struct{}, 1)}

// ==================== 数据库可视化数据结构 ====================

// WhitelistEntry 白名单条目（用于WebUI展示）
type WhitelistEntry struct {
	ID             int64   `json:"id"`
	Type           string  `json:"type"`             // "user" 或 "group"
	QQNumber       int64   `json:"qq_number"`        // 用户QQ号或群号
	ExpireAt       int64   `json:"expire_at"`        // 到期时间(Unix时间戳)
	DaysRemaining  float64 `json:"days_remaining"`   // 剩余天数
	PaidTotalCents int     `json:"paid_total_cents"` // 已支付总额(分)
	Eula           int     `json:"eula"`             // Eula状态(仅用户)
	EulaAgreedAt   int64   `json:"eula_agreed_at"`   // Eula同意时间(仅用户)
}

// ==================== 配置文件读写 ====================

// LoadConfig 从 WebuiData.json 加载配置到 GlobalConfig
func LoadConfig() error {
	data, err := os.ReadFile(ConfigFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// 配置文件不存在，使用默认配置并创建文件
			log.Println("[WebUI] 配置文件不存在，创建默认配置")
			defaultCfg := Config{
				PokeHandler: "koishi",
			}
			GlobalConfig.UpdateConfig(defaultCfg)
			return SaveConfig()
		}
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}

	GlobalConfig.UpdateConfig(cfg)
	log.Println("[WebUI] 配置加载成功")
	return nil
}

// SaveConfig 将 GlobalConfig 序列化写入 WebuiData.json
func SaveConfig() error {
	cfg := GlobalConfig.GetConfig()

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	tmpPath := ConfigFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("写入临时配置文件失败: %w", err)
	}
	if err := os.Rename(tmpPath, ConfigFilePath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("替换配置文件失败: %w", err)
	}

	return nil
}

func ioReadAllAndClose(rc io.ReadCloser) ([]byte, error) {
	defer rc.Close()
	return io.ReadAll(rc)
}

// writeJSONError 返回 JSON 格式的错误响应，确保前端 r.json() 不会因纯文本而解析失败
func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": message,
	}); err != nil {
		log.Printf("[WebUI] 写入JSON错误响应失败: %v\n", err)
	}
}

// ==================== HTTP API 处理 ====================

// handleGetConfig 获取当前配置
func handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		return
	}

	cfg := GlobalConfig.GetConfig()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(cfg); err != nil {
		log.Printf("[WebUI] 返回配置失败: %v\n", err)
	}
}

// handleUpdateConfig 更新配置
func handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	select {
	case configUpdateMu.locked <- struct{}{}:
		defer func() { <-configUpdateMu.locked }()
	default:
		writeJSONError(w, http.StatusConflict, "已有配置更新进行中，请稍后重试")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := ioReadAllAndClose(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("读取请求体失败: %v", err))
		return
	}

	var newCfg Config
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&newCfg); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("解析请求体失败: %v", err))
		return
	}
	if dec.More() {
		writeJSONError(w, http.StatusBadRequest, "请求体包含多余JSON内容")
		return
	}
	if err := validateConfig(newCfg); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("配置校验失败: %v", err))
		return
	}

	oldCfg := GlobalConfig.GetConfig()
	GlobalConfig.UpdateConfig(newCfg)
	if err := SaveConfig(); err != nil {
		GlobalConfig.UpdateConfig(oldCfg)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("保存配置失败: %v", err))
		return
	}

	// 如果更新了 bot_qq，同步更新 BotSelfID
	if newCfg.BotQQ != 0 {
		BotSelfID = newCfg.BotQQ
		log.Printf("[WebUI] BotQQ 已更新: %d\n", BotSelfID)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "配置更新成功，部分运行中连接/端口需重启程序后生效",
	}); err != nil {
		log.Printf("[WebUI] 返回配置更新结果失败: %v\n", err)
	}

	log.Println("[WebUI] 配置已更新（部分配置需重启生效）")
}

// handleGetStatus 获取系统状态
func handleGetStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		return
	}

	status := map[string]interface{}{
		"status":  "running",
		"version": "1.0.0",
		"note":    "连接状态基于当前活动连接判断，配置变更后的端口/部分连接需重启程序后生效",
		"ws_connections": map[string]bool{
			"koishi":  false,
			"astrbot": false,
			"yunzai":  false,
			"llbot":   false,
		},
	}

	cfg := GlobalConfig.GetConfig()
	wsNames := []string{"koishi", "astrbot", "yunzai", "llbot"}
	wsURLMap := map[string]string{
		"koishi":  cfg.KoishiWS,
		"astrbot": cfg.AstrBotWS,
		"yunzai":  cfg.YunzaiWS,
		"llbot":   cfg.LLBotWS,
	}
	wsConns := make(map[string]bool)
	for _, name := range wsNames {
		wsURL := wsURLMap[name]
		connected := false
		if wsURL != "" {
			if conn, ok := WSConnections.Load(wsURL); ok && conn != nil {
				connected = true
			}
		}
		if !connected {
			if conn, ok := WSServerConns.Load(name); ok && conn != nil {
				connected = true
			}
		}
		wsConns[name] = connected
	}
	status["ws_connections"] = wsConns

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("[WebUI] 返回状态失败: %v\n", err)
	}
}

// handleGetDatabase 获取数据库内容（白名单列表）
func handleGetDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		return
	}

	if DB == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "数据库未连接")
		return
	}

	now := time.Now().Unix()
	var entries []WhitelistEntry

	// 查询用户白名单
	rows, err := DB.Query("SELECT qq_number, eula, eula_agreed_at, paid_total_cents, expire_at FROM user_list ORDER BY expire_at DESC")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var entry WhitelistEntry
			var expireAt int64
			if err := rows.Scan(&entry.QQNumber, &entry.Eula, &entry.EulaAgreedAt, &entry.PaidTotalCents, &expireAt); err == nil {
				entry.Type = "user"
				entry.ExpireAt = expireAt
				daysRemaining := float64(expireAt-now) / 86400.0
				if daysRemaining < 0 {
					daysRemaining = 0
				}
				entry.DaysRemaining = float64(int(daysRemaining*100)) / 100 // 保留2位小数
				entries = append(entries, entry)
			}
		}
		if err := rows.Err(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("读取用户白名单失败: %v", err))
			return
		}
	}

	// 查询群聊白名单
	rows2, err := DB.Query("SELECT qq_group, paid_total_cents, expire_at FROM group_list ORDER BY expire_at DESC")
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var entry WhitelistEntry
			var expireAt int64
			if err := rows2.Scan(&entry.QQNumber, &entry.PaidTotalCents, &expireAt); err == nil {
				entry.Type = "group"
				entry.ExpireAt = expireAt
				daysRemaining := float64(expireAt-now) / 86400.0
				if daysRemaining < 0 {
					daysRemaining = 0
				}
				entry.DaysRemaining = float64(int(daysRemaining*100)) / 100
				entries = append(entries, entry)
			}
		}
		if err := rows2.Err(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("读取群聊白名单失败: %v", err))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"entries": entries,
		"total":   len(entries),
	}); err != nil {
		log.Printf("[WebUI] 返回数据库内容失败: %v\n", err)
	}
}

// ==================== WebUI 服务启动 ====================

// StartWebUI 构建 WebUI HTTP 服务
func StartWebUI() *http.Server {
	mux := http.NewServeMux()

	// API 路由
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetConfig(w, r)
		case http.MethodPost:
			handleUpdateConfig(w, r)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
	})

	mux.HandleFunc("/api/status", handleGetStatus)
	mux.HandleFunc("/api/database", handleGetDatabase)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "index.html")
	})

	fs := http.FileServer(http.Dir("assets"))
	mux.Handle("/assets/", http.StripPrefix("/assets/", fs))

	addr := fmt.Sprintf(":%d", WebUIPort)
	log.Printf("[WebUI] HTTP 服务启动于 %s\n", addr)

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func validateConfig(cfg Config) error {
	validMode := func(v string) bool {
		return v == "" || v == "client" || v == "server"
	}
	if !validMode(cfg.KoishiWSMode) || !validMode(cfg.AstrBotWSMode) || !validMode(cfg.YunzaiWSMode) || !validMode(cfg.LLBotWSMode) {
		return fmt.Errorf("ws_mode 仅允许 client 或 server")
	}
	ports := []int{cfg.WSServerPortLLBot, cfg.WSServerPortKoishi, cfg.WSServerPortAstrBot, cfg.WSServerPortYunzai}
	for _, port := range ports {
		if port < 0 || port > 65535 {
			return fmt.Errorf("WS服务端口超出范围")
		}
	}
	if cfg.GroupPrice < 0 || cfg.UserPrice < 0 {
		return fmt.Errorf("价格不能为负数")
	}
	if cfg.RedisDB < 0 || cfg.RedisDB > 15 {
		return fmt.Errorf("RedisDB 仅允许 0-15")
	}
	return nil
}
