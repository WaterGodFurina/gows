package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// ==================== 全局变量 ====================

var (
	RedisClient *redis.Client
	DB          *sql.DB
	OneBot      OneBotModule
	KoishiMod   KoishiModule
	AstrBotMod  AstrBotModule
	YunzaiMod   YunzaiModule
	BotSelfID   int64
)

// ==================== WebSocket 升级器 ====================

// WSConn 包装 websocket.Conn，添加写锁防止并发写 panic
type WSConn struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	Framework string
}

func (w *WSConn) WriteMessage(messageType int, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// 设置写超时，防止半开连接导致永久阻塞
	if err := w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	err := w.conn.WriteMessage(messageType, data)
	if err != nil {
		return err
	}
	// 写成功后清除deadline，避免影响后续操作
	return w.conn.SetWriteDeadline(time.Time{})
}

func (w *WSConn) Close() error {
	return w.conn.Close()
}

func (w *WSConn) NextReader() (int, []byte, error) {
	return w.conn.ReadMessage()
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许所有来源
	},
}

// ==================== 初始化 ====================

func initRedis(addr string, password string, db int) error {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := RedisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis连接失败: %w", err)
	}

	log.Println("[Main] Redis连接成功")
	return nil
}

func initPostgreSQL(dsn string) error {
	var err error
	DB, err = sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("PostgreSQL连接失败: %w", err)
	}

	DB.SetMaxOpenConns(25)
	DB.SetMaxIdleConns(5)
	DB.SetConnMaxLifetime(5 * time.Minute)

	if err = DB.Ping(); err != nil {
		return fmt.Errorf("PostgreSQL Ping失败: %w", err)
	}

	log.Println("[Main] PostgreSQL连接成功")

	// 自动建表
	if err = autoMigrate(); err != nil {
		return fmt.Errorf("自动建表失败: %w", err)
	}

	return nil
}

// autoMigrate 启动时自动建表
func autoMigrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS user_list (
                        qq_number BIGINT PRIMARY KEY,
                        eula SMALLINT DEFAULT 0,
                        eula_agreed_at BIGINT DEFAULT 0,
                        paid_total_cents INT DEFAULT 0,
                        expire_at BIGINT DEFAULT 0
                )`,
		`CREATE TABLE IF NOT EXISTS group_list (
                        qq_group BIGINT PRIMARY KEY,
                        paid_total_cents INT DEFAULT 0,
                        expire_at BIGINT DEFAULT 0
                )`,
		`CREATE TABLE IF NOT EXISTS buynumber (
                        id SERIAL PRIMARY KEY,
                        buy_number VARCHAR(128) UNIQUE,
                        user_qq BIGINT,
                        created_at BIGINT
                )`,
	}

	for _, q := range queries {
		if _, err := DB.Exec(q); err != nil {
			return err
		}
	}

	log.Println("[Main] 数据库表检查/创建完成")
	return nil
}

// ==================== OneBot 事件解析 ====================

// parseOneBotEvent 解析OneBot事件为统一Ctx
func parseOneBotEvent(data []byte) (*Ctx, error) {
	// Bug3修复: 使用json.Decoder + UseNumber()，避免float64精度丢失
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw map[string]interface{}
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("存在多余JSON内容")
		}
		return nil, err
	}

	ctx := &Ctx{}

	// 解析基本字段
	if v, ok := raw["post_type"].(string); ok {
		ctx.EventType = v
	}
	if v, ok := raw["sub_type"].(string); ok {
		ctx.SubType = v
	}
	if v, ok := raw["message_type"].(string); ok {
		ctx.MessageType = v
	}
	if v, ok := raw["group_name"].(string); ok {
		ctx.GroupName = v
	}
	if v, ok := raw["message_format"].(string); ok {
		ctx.MessageFormat = v
	}
	// Bug7修复: notice_type 不再覆盖 sub_type，poke 事件明确设置 SubType
	if v, ok := raw["notice_type"].(string); ok {
		ctx.EventType = "notice"
		if v == "poke" || v == "notify" {
			// 兼容不同 OneBot 实现，统一归为 poke
			if subType, _ := raw["sub_type"].(string); subType == "poke" || v == "poke" {
				ctx.SubType = "poke"
			} else {
				ctx.SubType = v
			}
		} else {
			ctx.SubType = v
		}
	}
	// Bug3修复: 使用json.Number转int64，避免float64精度问题
	if v, ok := raw["user_id"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.UserID = n
		}
	}
	if v, ok := raw["group_id"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.GroupID = n
		}
	}
		if v, ok := raw["self_id"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.SelfID = n
			if BotSelfID == 0 {
				// 优先从 WebUI 配置读取机器人QQ号
				cfg := GlobalConfig.GetConfig()
				if cfg.BotQQ != 0 {
					BotSelfID = cfg.BotQQ
				} else {
					BotSelfID = n
				}
			}
		}
	}

	// 解析新增字段
	if v, ok := raw["time"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.Time = n
		}
	}
	if v, ok := raw["message_type"].(string); ok {
		ctx.MessageType = v
	}
	if v, ok := raw["message_id"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.MessageID = n
		}
	}
	if v, ok := raw["message_seq"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.MessageSeq = n
		}
	}
	if v, ok := raw["font"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			ctx.Font = n
		}
	}

	// 解析sender信息
	if sender, ok := raw["sender"].(map[string]interface{}); ok {
		if v, ok := sender["user_id"].(json.Number); ok {
			if n, err := v.Int64(); err == nil {
				ctx.Sender.UserID = n
			}
		}
		if v, ok := sender["nickname"].(string); ok {
			ctx.Sender.Nickname = v
		}
		if v, ok := sender["card"].(string); ok {
			ctx.Sender.Card = v
		}
		if v, ok := sender["role"].(string); ok {
			ctx.Sender.Role = v
		}
	}

	// 解析消息
	if msg, ok := raw["message"]; ok {
		switch m := msg.(type) {
		case string:
			ctx.RawMessage = m
			ctx.PlainText = m
			if ctx.MessageFormat == "" {
				ctx.MessageFormat = "string"
			}
		case []interface{}:
			segments, plainText := parseMessageSegments(m)
			ctx.Segments = segments
			ctx.PlainText = plainText
			// 重建原始消息
			rawBytes, _ := json.Marshal(m)
			ctx.RawMessage = string(rawBytes)
			if ctx.MessageFormat == "" {
				ctx.MessageFormat = "array"
			}
		}
	}
	if ctx.RawMessage == "" {
		if rawMessage, ok := raw["raw_message"].(string); ok {
			ctx.RawMessage = rawMessage
			if ctx.PlainText == "" {
				ctx.PlainText = rawMessage
			}
		}
	}

	// 如果是戳一戳通知
	if ctx.EventType == "notice" && ctx.SubType == "poke" {
		if v, ok := raw["target_id"].(json.Number); ok {
			if n, err := v.Int64(); err == nil {
				ctx.Segments = append(ctx.Segments, Segment{
					Type: "poke",
					Data: map[string]interface{}{"qq": n},
				})
			}
		} else if v, ok := raw["target_id"].(float64); ok {
			ctx.Segments = append(ctx.Segments, Segment{
				Type: "poke",
				Data: map[string]interface{}{"qq": int64(v)},
			})
		}
	}

	return ctx, nil
}

// parseMessageSegments 解析消息段数组
func parseMessageSegments(msgArray []interface{}) ([]Segment, string) {
	var segments []Segment
	var plainText strings.Builder

	for _, item := range msgArray {
		segMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		seg := Segment{
			Type: fmt.Sprintf("%v", segMap["type"]),
			Data: make(map[string]interface{}),
		}

		if data, ok := segMap["data"].(map[string]interface{}); ok {
			seg.Data = data
		}

		segments = append(segments, seg)

		// 提取纯文本
		if seg.Type == "text" {
			if text, ok := seg.Data["text"].(string); ok {
				plainText.WriteString(text)
			}
		}
		if seg.Type == "at" {
			var atID string
			if qq, ok := seg.Data["qq"].(string); ok {
				atID = qq
			} else if qq, ok := seg.Data["qq"].(json.Number); ok {
				atID = qq.String()
			} else if qq, ok := seg.Data["qq"].(float64); ok {
				atID = strconv.FormatInt(int64(qq), 10)
			} else if uid, ok := seg.Data["user_id"].(string); ok {
				atID = uid
			} else if uid, ok := seg.Data["user_id"].(json.Number); ok {
				atID = uid.String()
			} else if uid, ok := seg.Data["user_id"].(float64); ok {
				atID = strconv.FormatInt(int64(uid), 10)
			}
			if atID != "" {
				plainText.WriteString("[At:" + atID + "]")
			}
		}
	}

	return segments, plainText.String()
}

// ==================== 核心路由逻辑 ====================

// handleMessage 核心路由：消息处理绝对优先级
func handleMessage(ctx *Ctx) {
	// 仅处理可路由事件，避免把 meta_event / 响应包误判为普通消息
	if ctx == nil {
		return
	}
	if ctx.EventType != "message" && ctx.EventType != "message_sent" && !(ctx.EventType == "notice" && ctx.SubType == "poke") {
		return
	}

	// ====== 优先级 0: 机器人自身消息(message_sent)转发到框架 ======
	cfg := GlobalConfig.GetConfig()
	botID := BotSelfID
	if cfg.BotQQ != 0 {
		botID = cfg.BotQQ
	}
	if ctx.UserID == botID {
		// 机器人自身发送的消息(例如 message_sent 或 user_id 为机器人QQ的消息): 转发给框架，不回传LLBot
		// LLBot已经知道这条消息（是它上报的），回传会造成循环
		log.Printf("[Route] 机器人自身消息，转发至框架: user=%d group=%d msgid=%d\n", ctx.UserID, ctx.GroupID, ctx.MessageID)
		forwardToFramework(ctx, "koishi")
		forwardToFramework(ctx, "astrbot")
		forwardToFramework(ctx, "yunzai")
		return
	}

	// ====== 优先级 2: 特殊指令拦截（同步调用，防止 ctx 被底层框架复用导致数据错乱） ======
	trimmedText := strings.TrimSpace(ctx.PlainText)
	if strings.HasPrefix(trimmedText, "//撤销同意eula") {
		handleRevokeEula(ctx)
		return
	}
	if strings.HasPrefix(trimmedText, "//同意eula") {
		handleAgreeEula(ctx)
		return
	}
	// 新增: //查看eula → 发送eula.png
	if trimmedText == "//查看eula" {
		sendImage(ctx, "eula.png")
		return
	}
	if strings.HasPrefix(trimmedText, "//支付") {
		if !checkEulaAgreed(ctx.UserID) {
			log.Printf("[Route] 支付指令前置EULA拦截: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
			sendImage(ctx, "eula.png")
			return
		}
		handlePaymentCommand(ctx)
		return
	}
	if strings.HasPrefix(trimmedText, "//白名单") {
		handleWhitelistCommand(ctx)
		return
	}

	// ====== 解析消息属性 ======
	isPoke := ctx.EventType == "notice" && ctx.SubType == "poke"
	isAtBot := hasSegment(ctx, "at", botID)
	isPrivate := ctx.GroupID == 0
	prefix, judgeMod := getPrefixAndJudgeMod(ctx)
	needJudge := isPoke || isAtBot || prefix != ""

	// ====== 优先级 3: Eula 协议前置校验 ======
	if needJudge {
		log.Printf("[Route] 命中受控触发: user=%d group=%d at=%v poke=%v prefix=%q judge=%q\n", ctx.UserID, ctx.GroupID, isAtBot, isPoke, prefix, judgeMod)
		if !checkEulaAgreed(ctx.UserID) {
			log.Printf("[Route] EULA未同意，拦截消息: user=%d group=%d at=%v poke=%v prefix=%q\n", ctx.UserID, ctx.GroupID, isAtBot, isPoke, prefix)
			
			// 决定是否发送 eula.png
			shouldSendEula := false
			if isPrivate {
				// 私聊下：指令前缀、poke/戳一戳、@bot，均可触发 eula 协议图片
				if isAtBot || isPoke || prefix != "" {
					shouldSendEula = true
				}
			} else {
				// 群聊下：只能通过 @bot 触发 eula 协议图片发送
				if isAtBot {
					shouldSendEula = true
				}
			}

			if shouldSendEula {
				sendImage(ctx, "eula.png")
			}
			return // 未同意EULA的所有受控消息均进行拦截
		}
	}

	// ====== 优先级 4: 核心路由判题 ======
	// 路由规则:
	//   - koishi: 接收白名单群聊/私聊的所有消息
	//   - astrbot: 仅接收 @bot 和对应指令前缀的消息
	//   - yunzai: 仅接收 @bot 和对应指令前缀的消息
	if needJudge {
		isSilentPrefix := prefix != "" && judgeMod == "yunzai"
		result := JudgeResult{IsValid: false, Reason: "not_found"}
		if !isAtBot {
			judgeFunc := getJudgeFunc(judgeMod)
			result = judgeFunc(ctx.UserID, ctx.GroupID)
			log.Printf("[Route] 判题结果: user=%d group=%d judge=%q valid=%v reason=%q\n", ctx.UserID, ctx.GroupID, judgeMod, result.IsValid, result.Reason)
		} else {
			log.Printf("[Route] @bot 触发，跳过单框架判题，改走支付态分流: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
		}

		// koishi 判题（@bot 用 koishi 判题，其他用 judgeMod 对应的判题）
		koishiResult := KoishiMod.JudgeAuth(ctx.UserID, ctx.GroupID)

		if isAtBot {
			// @bot: 已支付 → 转发所有三个框架(koishi/astrbot/yunzai); 未支付 → 发送 nobuy.png
			if koishiResult.IsValid {
				log.Printf("[Route] @bot 已支付，转发至 koishi/astrbot/yunzai: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
				forwardToFramework(ctx, "koishi")
				forwardToFramework(ctx, "astrbot")
				forwardToFramework(ctx, "yunzai")
			} else {
				log.Printf("[Route] @bot 未支付，发送 nobuy.png: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
				sendImage(ctx, "nobuy.png")
			}
		} else if result.IsValid {
			// 前缀/触发已支付: koishi 总是接收 + 对应框架
			log.Printf("[Route] 受控触发已支付，转发至 koishi+%s: user=%d group=%d\n", judgeMod, ctx.UserID, ctx.GroupID)
			forwardToFramework(ctx, "koishi")
			forwardToFramework(ctx, judgeMod)
		} else {
			// 未支付
			if isPoke || isSilentPrefix {
				log.Printf("[Route] 静默拦截触发: user=%d group=%d poke=%v prefix=%q judge=%q\n", ctx.UserID, ctx.GroupID, isPoke, prefix, judgeMod)
				return
			} else if isPrivate {
				log.Printf("[Route] 私聊未支付，发送 nobuy.png: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
				sendImage(ctx, "nobuy.png")
			} else {
				log.Printf("[Route] 群聊未支付，发送 nobuy.png: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
				sendImage(ctx, "nobuy.png")
			}
		}
		return
	}

	// ====== 优先级 5: 默认群/私聊消息放行规则 ======
	// koishi/astrbot 接收白名单群聊/私聊的所有消息
	// yunzai 仅接收 @bot + 指令前缀的消息（上面 needJudge 已处理）
	result := KoishiMod.JudgeAuth(ctx.UserID, ctx.GroupID)
	log.Printf("[Route] 默认消息判题(koishi): user=%d group=%d valid=%v reason=%q\n", ctx.UserID, ctx.GroupID, result.IsValid, result.Reason)
	if result.IsValid {
		log.Printf("[Route] 默认消息已支付，同时转发 koishi 和 astrbot: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
		forwardToFramework(ctx, "koishi")
		forwardToFramework(ctx, "astrbot")
		return
	}
	log.Printf("[Route] 默认消息未支付，丢弃: user=%d group=%d\n", ctx.UserID, ctx.GroupID)
	return
}

// ==================== 辅助路由函数 ====================

// hasSegment 检查消息段中是否包含指定类型和目标
func hasSegment(ctx *Ctx, segType string, targetID int64) bool {
	for _, seg := range ctx.Segments {
		if seg.Type == segType {
			// 兼容 json.Number (UseNumber模式) 和 float64 两种情况
			if qq, ok := seg.Data["qq"].(json.Number); ok {
				if n, err := qq.Int64(); err == nil && n == targetID {
					return true
				}
			}
			if qq, ok := seg.Data["qq"].(float64); ok && int64(qq) == targetID {
				return true
			}
			if qq, ok := seg.Data["qq"].(string); ok {
				if fmt.Sprintf("%v", qq) == fmt.Sprintf("%d", targetID) {
					return true
				}
			}
			if uid, ok := seg.Data["user_id"].(json.Number); ok {
				if n, err := uid.Int64(); err == nil && n == targetID {
					return true
				}
			}
			if uid, ok := seg.Data["user_id"].(float64); ok && int64(uid) == targetID {
				return true
			}
		}
	}
	return false
}

// getPrefixAndJudgeMod 获取消息前缀和对应的判题模块
func getPrefixAndJudgeMod(ctx *Ctx) (string, string) {
	trimmedText := strings.TrimSpace(ctx.PlainText)
	prefixConfigs := GetPrefixConfigs()
	for _, pc := range prefixConfigs {
		if strings.HasPrefix(trimmedText, pc.Prefix) {
			return pc.Prefix, pc.JudgeMod
		}
	}

	// @bot 消息
	botID := BotSelfID
	cfg := GlobalConfig.GetConfig()
	if cfg.BotQQ != 0 {
		botID = cfg.BotQQ
	}
	if hasSegment(ctx, "at", botID) {
		return "@", ""
	}

	// 戳一戳
	if ctx.EventType == "notice" && ctx.SubType == "poke" {
		cfg := GlobalConfig.GetConfig()
		handler := strings.TrimSpace(cfg.PokeHandler)
		if handler == "" {
			handler = "koishi"
		}
		return "poke", handler
	}

	return "", ""
}

// getJudgeFunc 根据模块名获取判题函数
func getJudgeFunc(judgeMod string) func(userID, groupID int64) JudgeResult {
	switch judgeMod {
	case "koishi":
		return KoishiMod.JudgeAuth
	case "astrbot":
		return AstrBotMod.JudgeAuth
	case "yunzai":
		return YunzaiMod.JudgeAuth
	default:
		// 默认使用Koishi判题
		return KoishiMod.JudgeAuth
	}
}

// sendImage 发送图片消息
func sendImage(ctx *Ctx, imageName string) {
	imagePath := "assets/" + imageName
	sendImageMessage(ctx.UserID, ctx.GroupID, imagePath)
}

// ==================== 消息转发 ====================

// forwardToFramework 转发消息到指定框架WS
func forwardToFramework(ctx *Ctx, framework string) {
	cfg := GlobalConfig.GetConfig()
	var wsURL string
	var wsAuth string
	var wsMode string

	switch framework {
	case "koishi":
		wsURL = cfg.KoishiWS
		wsAuth = cfg.KoishiWSAuth
		wsMode = cfg.KoishiWSMode
	case "astrbot":
		wsURL = cfg.AstrBotWS
		wsAuth = cfg.AstrBotWSAuth
		wsMode = cfg.AstrBotWSMode
	case "yunzai":
		wsURL = cfg.YunzaiWS
		wsAuth = cfg.YunzaiWSAuth
		wsMode = cfg.YunzaiWSMode
	default:
		log.Printf("[Main] 未知框架: %s\n", framework)
		return
	}

	if wsMode == "server" {
		// server模式: 通过WSServerConns发送
		forwardToWSServer(framework, ctx)
		return
	}

	// client模式: 通过WS客户端连接发送
	if wsURL == "" {
		// WS链接为空则旁路
		log.Printf("[Main] 框架 %s WS链接为空，消息旁路\n", framework)
		return
	}

	forwardToWS(wsURL, wsAuth, ctx)
}

// forwardToLLBot 转发消息(作为OneBot事件)到后端LLBot WS
// 注意: 仅在需要将事件格式消息发送给LLBot时使用(目前暂无人调用)
// 框架发消息给LLBot应通过API请求路径(proxyFrameworkAPIRequest)，而非事件格式
func forwardToLLBot(ctx *Ctx) {
	if ctx == nil {
		return
	}
	// 不再自动调用markSelfEcho，避免污染去重map导致用户消息被误判

	cfg := GlobalConfig.GetConfig()

	if cfg.LLBotWSMode == "server" {
		// server模式: 通过WSServerConns发送
		forwardToWSServer("llbot", ctx)
		return
	}

	// client模式
	if cfg.LLBotWS == "" {
		return
	}
	forwardToWS(cfg.LLBotWS, cfg.LLBotWSAuth, ctx)
}

func buildOneBotEventFromCtx(ctx *Ctx) map[string]interface{} {
	event := map[string]interface{}{
		"post_type":   ctx.EventType,
		"raw_message": ctx.RawMessage,
		"time":        ctx.Time,
		"self_id":     ctx.SelfID,
		"font":        ctx.Font,
	}

	if ctx.EventType == "message" || ctx.EventType == "message_sent" {
		messageType := ctx.MessageType
		if messageType == "" {
			if ctx.GroupID != 0 {
				messageType = "group"
			} else {
				messageType = "private"
			}
		}
		subType := ctx.SubType
		if subType == "" {
			if messageType == "private" {
				subType = "friend"
			} else {
				subType = "normal"
			}
		}
		sender := map[string]interface{}{
			"user_id": ctx.UserID,
		}
		if ctx.Sender.UserID != 0 {
			sender["user_id"] = ctx.Sender.UserID
		}
		if ctx.Sender.Nickname != "" {
			sender["nickname"] = ctx.Sender.Nickname
		}
		if ctx.Sender.Card != "" {
			sender["card"] = ctx.Sender.Card
		}
		if ctx.Sender.Role != "" {
			sender["role"] = ctx.Sender.Role
		}

		event["message_type"] = messageType
		event["sub_type"] = subType
		event["user_id"] = ctx.UserID
		event["message_id"] = ctx.MessageID
		event["sender"] = sender
		if ctx.MessageSeq != 0 {
			event["message_seq"] = ctx.MessageSeq
		}
		if ctx.MessageFormat != "" {
			event["message_format"] = ctx.MessageFormat
		}
		if len(ctx.Segments) > 0 {
			segments := make([]interface{}, 0, len(ctx.Segments))
			for _, seg := range ctx.Segments {
				segments = append(segments, map[string]interface{}{
					"type": seg.Type,
					"data": seg.Data,
				})
			}
			event["message"] = segments
		} else if ctx.MessageFormat == "string" {
			event["message"] = ctx.RawMessage
		} else if ctx.RawMessage != "" {
			event["message"] = []interface{}{
				map[string]interface{}{
					"type": "text",
					"data": map[string]interface{}{"text": ctx.RawMessage},
				},
			}
		} else {
			event["message"] = []interface{}{}
		}
		if ctx.GroupID != 0 {
			event["group_id"] = ctx.GroupID
		}
		if ctx.GroupName != "" {
			event["group_name"] = ctx.GroupName
		}
	} else if ctx.EventType == "notice" && ctx.SubType == "poke" {
		event["notice_type"] = "notify"
		event["sub_type"] = "poke"
		event["user_id"] = ctx.UserID
		event["target_id"] = getPokeTargetID(ctx)
		event["sender_id"] = ctx.UserID
		if ctx.GroupID != 0 {
			event["group_id"] = ctx.GroupID
		}
	}
	if ctx.GroupID != 0 {
		event["group_id"] = ctx.GroupID
	}
	return normalizeOneBotEventPayload(event)
}

// forwardToWSServer 通过WS服务器模式的连接转发消息
func forwardToWSServer(framework string, ctx *Ctx) {
	v, ok := WSServerConns.Load(framework)
	if !ok {
		log.Printf("[Main] 框架 %s WS服务器模式无连接，消息旁路\n", framework)
		return
	}
	wsConn, ok := v.(*WSConn)
	if !ok {
		log.Printf("[Main] 框架 %s WS连接类型异常\n", framework)
		return
	}

	event := buildOneBotEventFromCtx(ctx)

	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[Main] 序列化事件失败: %v\n", err)
		return
	}

	if err := wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[Main] 发送消息到 %s 失败: %v\n", framework, err)
		if current, ok := WSServerConns.Load(framework); ok && current == wsConn {
			WSServerConns.Delete(framework)
		}
		wsConn.conn.Close()
		return
	}
}

// getWSMode 获取指定框架的WS模式
func getWSMode(framework string) string {
	cfg := GlobalConfig.GetConfig()
	switch framework {
	case "koishi":
		if cfg.KoishiWSMode == "server" {
			return "server"
		}
		return "client"
	case "astrbot":
		if cfg.AstrBotWSMode == "server" {
			return "server"
		}
		return "client"
	case "yunzai":
		if cfg.YunzaiWSMode == "server" {
			return "server"
		}
		return "client"
	case "llbot":
		if cfg.LLBotWSMode == "server" {
			return "server"
		}
		return "client"
	default:
		return "client"
	}
}

// handleFrameworkWS 处理WS服务器模式下框架的WS连接
func handleFrameworkWS(w http.ResponseWriter, r *http.Request, framework string) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// 验证鉴权密码
	cfg := GlobalConfig.GetConfig()
	var expectedAuth string
	switch framework {
	case "koishi":
		expectedAuth = cfg.KoishiWSAuth
		if expectedAuth == "" {
			expectedAuth = cfg.AccessToken
		}
	case "astrbot":
		expectedAuth = cfg.AstrBotWSAuth
		if expectedAuth == "" {
			expectedAuth = cfg.AccessToken
		}
	case "yunzai":
		expectedAuth = cfg.YunzaiWSAuth
		if expectedAuth == "" {
			expectedAuth = cfg.AccessToken
		}
	case "llbot":
		expectedAuth = cfg.LLBotWSAuth
		if expectedAuth == "" {
			expectedAuth = cfg.AccessToken
		}
	}

	if expectedAuth != "" {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+expectedAuth {
			log.Printf("[WS] %s WS连接鉴权失败\n", framework)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] %s WS升级失败: %v\n", framework, err)
		return
	}

	log.Printf("[WS] %s 框架已连接 (server模式)\n", framework)
	conn.SetReadLimit(10 * 1024 * 1024)
	conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	})
	wsConn := &WSConn{conn: conn, Framework: framework}

	if oldV, loaded := WSServerConns.LoadOrStore(framework, wsConn); loaded {
		if oldConn, ok := oldV.(*WSConn); ok && oldConn != nil {
			log.Printf("[WS] %s 已存在旧连接，关闭旧连接并切换到新连接\n", framework)
			oldConn.Close()
		}
		WSServerConns.Store(framework, wsConn)
	}

	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(45 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := wsConn.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
					return
				}
			case <-pingDone:
				return
			}
		}
	}()

	defer func() {
		close(pingDone)
		conn.Close()
		if current, ok := WSServerConns.Load(framework); ok && current == wsConn {
			WSServerConns.Delete(framework)
		}
		log.Printf("[WS] %s 框架连接断开 (server模式)\n", framework)
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		if len(msg) > 1024 {
			log.Printf("[WS] %s 读取到大数据包: len=%d bytes, prefix=%s\n", framework, len(msg), truncateForLog(string(msg), 128))
		}
		if framework == "llbot" && tryHandlePendingAPIResponse(msg) {
			continue
		}
		ctx, parseErr := parseOneBotEvent(msg)
		if parseErr != nil {
			if handleFrameworkInboundRawMessage(wsConn, framework, msg) {
				continue
			}
			log.Printf("[WS] %s 消息解析失败: %v, raw=%s\n", framework, parseErr, truncateForLog(string(msg), 256))
			continue
		}
		if ctx == nil {
			if handleFrameworkInboundRawMessage(wsConn, framework, msg) {
				continue
			}
			continue
		}
		if framework == "llbot" {
			// 自回环去重: 仅对机器人自身消息检查，避免把用户消息误判为回环
			cfg := GlobalConfig.GetConfig()
			botID := BotSelfID
			if cfg.BotQQ != 0 {
				botID = cfg.BotQQ
			}
			if ctx.UserID == botID && isDuplicateSelfEcho(ctx) {
				log.Printf("[WS] 忽略 LLBot 自回环消息: user=%d group=%d msgid=%d\n", ctx.UserID, ctx.GroupID, ctx.MessageID)
				continue
			}
			log.Printf("[WS] LLBot 事件进入路由: type=%s user=%d group=%d msgid=%d text=%q\n", ctx.EventType, ctx.UserID, ctx.GroupID, ctx.MessageID, truncateForLog(ctx.PlainText, 80))
			handleMessage(ctx)
		} else {
			handleFrameworkInboundRawMessage(wsConn, framework, msg)
		}
	}
}

// BugG修复: 用 singleflight 防止并发时建立多条重复连接
var dialGroup singleflight.Group

// wsAuthTokens 存储每个WS URL对应的鉴权密码，用于重连时使用
var (
	wsAuthTokens     sync.Map // map[string]string
	wsReconnectFlags sync.Map // map[string]bool，防止同一WS重复并发重连
)

func scheduleWSReconnect(wsURL string) {
	if _, loaded := wsReconnectFlags.LoadOrStore(wsURL, true); loaded {
		return
	}

	go func() {
		defer wsReconnectFlags.Delete(wsURL)
		backoff := 3 * time.Second
		maxBackoff := 60 * time.Second
		retries := 0
		for {
			time.Sleep(backoff)

			if _, ok := WSConnections.Load(wsURL); ok {
				return
			}

			log.Printf("[WS] 尝试重连 %s (退避 %vs, 第 %d 次)\n", wsURL, backoff.Seconds(), retries+1)
			reconnectAuth := ""
			if v, ok := wsAuthTokens.Load(wsURL); ok {
				reconnectAuth = v.(string)
			}
			if _, err := getOrDialWS(wsURL, reconnectAuth); err != nil {
				retries++
				log.Printf("[WS] 重连失败 %s: %v\n", wsURL, err)
				// 指数退避: 3s -> 6s -> 12s -> ... -> 60s
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			log.Printf("[WS] 重连成功 %s\n", wsURL)
			return
		}
	}()
}

// getOrDialWS 从 WSConnections 获取复用连接，断开时重连，并发安全
// 参照ZeroBot的WS逻辑，添加accessToken鉴权和重连机制
// authToken: 该WS连接的鉴权密码，为空则不鉴权
func getOrDialWS(wsURL string, authToken string) (*WSConn, error) {
	// 保存authToken用于重连
	if authToken != "" {
		wsAuthTokens.Store(wsURL, authToken)
	}

	if v, ok := WSConnections.Load(wsURL); ok {
		return v.(*WSConn), nil
	}
	v, err, _ := dialGroup.Do(wsURL, func() (interface{}, error) {
		if v, ok := WSConnections.Load(wsURL); ok {
			return v, nil
		}

		// 构建WS握手请求头
		// OneBot v11 鉴权方式：Authorization: Bearer xxx 或 access_token 查询参数
		header := http.Header{}
		cfg := GlobalConfig.GetConfig()
		isLLBotClient := (wsURL == cfg.LLBotWS && cfg.LLBotWSMode != "server")
		isFrameworkClient := (wsURL == cfg.KoishiWS && cfg.KoishiWSMode != "server") ||
			(wsURL == cfg.AstrBotWS && cfg.AstrBotWSMode != "server") ||
			(wsURL == cfg.YunzaiWS && cfg.YunzaiWSMode != "server")
		if isLLBotClient {
			// LLBot 是标准 OneBot 实现端，使用 Bearer 头 + X-Self-ID + X-Client-Role
			header.Set("Authorization", "Bearer "+authToken)
			if BotSelfID != 0 {
				header.Set("X-Self-ID", fmt.Sprintf("%d", BotSelfID))
			}
			header.Set("X-Client-Role", "Universal")
		} else if isFrameworkClient {
			// koishi/astrbot/yunzai 等框架的 OneBot 实现也要求 Bearer 头 + X-Client-Role + X-Self-ID
			header.Set("Authorization", "Bearer "+authToken)
			header.Set("X-Client-Role", "Universal")
			if BotSelfID != 0 {
				header.Set("X-Self-ID", fmt.Sprintf("%d", BotSelfID))
			}
		}
		// 其余非 OneBot 实现端连接的鉴权 token 通过 access_token 查询参数传递（见下方 URL 拼接）

		// 非OneBot实现端连接且需要鉴权时，将 token 拼入 URL 查询参数
		// OneBot v11 支持 access_token 查询参数作为备用鉴权方式
		dialURL := wsURL
		if !isLLBotClient && !isFrameworkClient && authToken != "" {
			if strings.Contains(wsURL, "?") {
				dialURL = wsURL + "&access_token=" + authToken
			} else {
				dialURL = wsURL + "?access_token=" + authToken
			}
		}

		conn, _, err := websocket.DefaultDialer.Dial(dialURL, header)
		if err != nil {
			return nil, err
		}
		conn.SetReadLimit(10 * 1024 * 1024)
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		})

		wsConn := &WSConn{conn: conn, Framework: frameworkByWSURL(wsURL, cfg)}
		WSConnections.Store(wsURL, wsConn)
		connectedAt := time.Now()
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[WS] 读goroutine panic: %v", r)
					if current, ok := WSConnections.Load(wsURL); ok && current == wsConn {
						WSConnections.Delete(wsURL)
					}
					wsConn.Close()
				}
			}()

			pingTicker := time.NewTicker(45 * time.Second)
			defer pingTicker.Stop()
			go func() {
				for range pingTicker.C {
					if err := wsConn.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
						return
					}
				}
			}()

			for {
				wsConn.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
				msgType, msg, err := wsConn.NextReader()
				if err != nil {
					lived := time.Since(connectedAt)
					log.Printf("[WS] 连接断开 %s: %v (存活 %vs)\n", wsURL, err, int(lived.Seconds()))
					if current, ok := WSConnections.Load(wsURL); ok && current == wsConn {
						WSConnections.Delete(wsURL)
					}
					wsConn.Close()
					// 连接存活极短说明对端主动拒绝 client 模式拨入，提示用户改用 server 模式
					if lived < 5*time.Second {
						log.Printf("[WS] %s 连接存活不足5秒即断开，可能该框架不支持 client 模式被拨入，建议改用 server 模式\n", wsURL)
					}
					scheduleWSReconnect(wsURL)
					return
				}

				if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
					continue
				}

				cfg := GlobalConfig.GetConfig()
				framework := wsConn.Framework
				if framework == "" {
					framework = frameworkByWSURL(wsURL, cfg)
				}

				log.Printf("[WS Debug] Client read message from %s (framework=%q): msgType=%d, len=%d, prefix=%s\n",
					wsURL, framework, msgType, len(msg), truncateForLog(string(msg), 128))

				// ====== 极其重要的客户端路由逻辑 ======
				// 如果当前拨号的连接是针对应用框架(Koishi/AstrBot/Yunzai)的:
				// 框架发送的消息一律作为框架的入站API请求进行代理，不用走 parseOneBotEvent 解析(防止解析错乱而直接 continue)
				if framework != "" {
					if len(msg) > 1024 {
						log.Printf("[WS] client 模式 %s 框架读取到大数据包: len=%d bytes, prefix=%s\n", framework, len(msg), truncateForLog(string(msg), 128))
					}
					handleFrameworkInboundRawMessage(wsConn, framework, msg)
					continue
				}

				// 如果是LLBot连接
				if tryHandlePendingAPIResponse(msg) {
					continue
				}
				ctx, parseErr := parseOneBotEvent(msg)
				if parseErr != nil {
					log.Printf("[WS] client模式消息解析失败 %s: %v, raw=%s\n", wsURL, parseErr, truncateForLog(string(msg), 256))
					continue
				}
				if ctx == nil {
					continue
				}

				if wsURL == cfg.LLBotWS {
					// 自回环去重: 仅对机器人自身消息检查
					botID := BotSelfID
					if cfg.BotQQ != 0 {
						botID = cfg.BotQQ
					}
					if ctx.UserID == botID && isDuplicateSelfEcho(ctx) {
						continue
					}
					handleMessage(ctx)
					continue
				}
			}
		}()
		return wsConn, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*WSConn), nil
}

// forwardToWS 通用WS转发，复用长连接
// 参照ZeroBot: 转发完整的OneBot事件格式
// authToken: 该WS连接的鉴权密码，为空则不鉴权
func forwardToWS(wsURL string, authToken string, ctx *Ctx) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Main] forwardToWS panic: %v", r)
		}
	}()

	msg := buildOneBotEventFromCtx(ctx)

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[Main] 序列化转发消息失败: %v\n", err)
		return
	}
	conn, err := getOrDialWS(wsURL, authToken)
	if err != nil {
		log.Printf("[Main] 连接框架WS失败 %s: %v\n", wsURL, err)
		// 连接失败时触发异步重连，避免后续消息全部失败
		scheduleWSReconnect(wsURL)
		return
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[Main] 转发消息失败，清理连接 %s: %v\n", wsURL, err)
		if current, ok := WSConnections.Load(wsURL); ok && current == conn {
			WSConnections.Delete(wsURL)
		}
		conn.Close()
		// 写失败后触发异步重连
		scheduleWSReconnect(wsURL)
		return
	}
	log.Printf("[Main] 消息已转发至 %s\n", wsURL)
}

func frameworkByWSURL(wsURL string, cfg Config) string {
	// 使用URL解析进行模糊匹配，避免因尾部斜杠、大小写、URL编码差异导致匹配失败
	parseURL := func(urlStr string) string {
		u, err := url.Parse(urlStr)
		if err != nil {
			return urlStr
		}
		// 标准化：小写host + 去掉尾部斜杠的path
		host := strings.ToLower(u.Host)
		path := strings.TrimRight(u.Path, "/")
		return host + path
	}

	target := parseURL(wsURL)
	var matched string
	switch {
	case parseURL(cfg.KoishiWS) == target:
		matched = "koishi"
	case parseURL(cfg.AstrBotWS) == target:
		matched = "astrbot"
	case parseURL(cfg.YunzaiWS) == target:
		matched = "yunzai"
	default:
		matched = ""
	}
	
	// 如果是大包或者连接判定，打印具体匹配链路日志
	if matched == "" || len(wsURL) > 0 {
		log.Printf("[WS Debug] frameworkByWSURL 匹配结果: wsURL=%q target=%q matched=%q (配置中: yunzai=%q koishi=%q astrbot=%q)\n",
			wsURL, target, matched, cfg.YunzaiWS, cfg.KoishiWS, cfg.AstrBotWS)
	}
	return matched
}

func parseJSONMap(data []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw map[string]interface{}
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("存在多余JSON内容")
		}
		return nil, err
	}
	return raw, nil
}

func asInt64FromAny(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case uint64:
		if t > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(t), true
	case uint32:
		return int64(t), true
	case float64:
		return int64(t), true
	case float32:
		return int64(t), true
	case json.Number:
		n, err := t.Int64()
		return n, err == nil
	case string:
		if t == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(t, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func asStringFromAny(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func getActionParams(req map[string]interface{}) map[string]interface{} {
	if req == nil {
		return map[string]interface{}{}
	}
	if params, ok := req["params"].(map[string]interface{}); ok && params != nil {
		return params
	}
	return map[string]interface{}{}
}

func ensureMapField(m map[string]interface{}, key string) map[string]interface{} {
	if m == nil {
		return map[string]interface{}{}
	}
	if data, ok := m[key].(map[string]interface{}); ok && data != nil {
		return data
	}
	data := map[string]interface{}{}
	m[key] = data
	return data
}

func normalizeMessageSegmentItem(item interface{}) interface{} {
	segMap, ok := item.(map[string]interface{})
	if !ok || segMap == nil {
		return item
	}
	segType := asStringFromAny(segMap["type"])
	if segType == "" {
		segType = asStringFromAny(segMap["message_type"])
	}
	data, _ := segMap["data"].(map[string]interface{})
	if data == nil {
		data = map[string]interface{}{}
	}
	seg := map[string]interface{}{
		"type": segType,
		"data": data,
	}
	if segType == "record" || segType == "voice" {
		if file := asStringFromAny(data["file"]); file != "" {
			data["file"] = file
		}
		if url := asStringFromAny(data["url"]); url != "" {
			data["url"] = url
		}
	}
	return seg
}

func normalizeMessageArrayForOneBot(msg interface{}) interface{} {
	switch m := msg.(type) {
	case nil:
		return nil
	case string:
		return m
	case []interface{}:
		out := make([]interface{}, 0, len(m))
		for _, item := range m {
			out = append(out, normalizeMessageSegmentItem(item))
		}
		return out
	default:
		return m
	}
}

func normalizeOneBotAPIResponseByAction(action string, req map[string]interface{}, resp map[string]interface{}) map[string]interface{} {
	resp = normalizeOneBotAPIResponse(resp, nil, false)
	params := getActionParams(req)
	data := ensureMapField(resp, "data")

	switch action {
	case "send_msg", "send_private_msg", "send_group_msg":
		if _, ok := data["message_id"]; !ok {
			if msgID, ok := resp["message_id"]; ok {
				data["message_id"] = msgID
			} else {
				data["message_id"] = int32(time.Now().Unix() & 0x7FFFFFFF)
			}
		}

	case "get_login_info":
		if _, ok := data["user_id"]; !ok {
			if BotSelfID != 0 {
				data["user_id"] = BotSelfID
			}
		}
		if _, ok := data["nickname"]; !ok {
			data["nickname"] = "bot"
		}

	case "get_group_info":
		groupID, _ := asInt64FromAny(params["group_id"])
		if _, ok := data["group_id"]; !ok && groupID != 0 {
			data["group_id"] = groupID
		}
		if _, ok := data["group_name"]; !ok {
			if name := asStringFromAny(resp["group_name"]); name != "" {
				data["group_name"] = name
			} else if name := asStringFromAny(data["group_name"]); name != "" {
				data["group_name"] = name
			} else if groupID != 0 {
				data["group_name"] = strconv.FormatInt(groupID, 10)
			} else {
				data["group_name"] = "unknown"
			}
		}
		if _, ok := data["member_count"]; !ok {
			data["member_count"] = 0
		}
		if _, ok := data["max_member_count"]; !ok {
			data["max_member_count"] = 0
		}

	case "get_group_list":
		if list, ok := resp["data"].([]interface{}); ok {
			for i, item := range list {
				row, ok := item.(map[string]interface{})
				if !ok || row == nil {
					continue
				}
				groupID, _ := asInt64FromAny(row["group_id"])
				if _, ok := row["group_name"]; !ok || asStringFromAny(row["group_name"]) == "" {
					if groupID != 0 {
						row["group_name"] = strconv.FormatInt(groupID, 10)
					} else {
						row["group_name"] = "unknown"
					}
				}
				if _, ok := row["member_count"]; !ok {
					row["member_count"] = 0
				}
				if _, ok := row["max_member_count"]; !ok {
					row["max_member_count"] = 0
				}
				list[i] = row
			}
			resp["data"] = list
		}

	case "get_group_member_info":
		groupID, _ := asInt64FromAny(params["group_id"])
		userID, _ := asInt64FromAny(params["user_id"])
		if _, ok := data["group_id"]; !ok && groupID != 0 {
			data["group_id"] = groupID
		}
		if _, ok := data["user_id"]; !ok && userID != 0 {
			data["user_id"] = userID
		}
		if _, ok := data["nickname"]; !ok {
			data["nickname"] = "unknown"
		}
		if _, ok := data["card"]; !ok {
			data["card"] = ""
		}
		if _, ok := data["role"]; !ok {
			data["role"] = "member"
		}

	case "get_stranger_info":
		userID, _ := asInt64FromAny(params["user_id"])
		if _, ok := data["user_id"]; !ok && userID != 0 {
			data["user_id"] = userID
		}
		if _, ok := data["nickname"]; !ok {
			data["nickname"] = "unknown"
		}

	case "get_msg":
		if _, ok := data["message_id"]; !ok {
			if msgID, ok := asInt64FromAny(params["message_id"]); ok {
				data["message_id"] = msgID
			}
		}
		if _, ok := data["message"]; !ok {
			data["message"] = []interface{}{}
		}
		data["message"] = normalizeMessageArrayForOneBot(data["message"])
		if _, ok := data["raw_message"]; !ok {
			data["raw_message"] = ""
		}
		if _, ok := data["message_type"]; !ok {
			data["message_type"] = "private"
		}
		if _, ok := data["time"]; !ok {
			data["time"] = time.Now().Unix()
		}
		if _, ok := data["sender"]; !ok {
			data["sender"] = map[string]interface{}{}
		}

	case "get_forward_msg":
		if _, ok := data["messages"]; !ok {
			data["messages"] = []interface{}{}
		}

	case "can_send_image", "can_send_record":
		if _, ok := data["yes"]; !ok {
			data["yes"] = true
		}

	case "get_status":
		if _, ok := data["online"]; !ok {
			data["online"] = true
		}
		if _, ok := data["good"]; !ok {
			data["good"] = true
		}

	case "get_version_info":
		if _, ok := data["app_name"]; !ok {
			data["app_name"] = "gows"
		}
		if _, ok := data["app_version"]; !ok {
			data["app_version"] = "1.0.0"
		}
		if _, ok := data["protocol_version"]; !ok {
			data["protocol_version"] = "v11"
		}

	case "get_record", "get_image":
		if _, ok := data["file"]; !ok {
			if file := asStringFromAny(params["file"]); file != "" {
				data["file"] = file
			}
		}

	case "get_group_member_list", "get_friend_list", "get_group_honor_info", "get_essence_msg_list", "get_online_clients", "get_unidirectional_friend_list":
		if _, ok := resp["data"].([]interface{}); !ok {
			resp["data"] = []interface{}{}
		}

	case "send_record", "send_voice":
		if _, ok := data["message_id"]; !ok {
			data["message_id"] = int32(time.Now().Unix() & 0x7FFFFFFF)
		}
	}

	if msg, ok := data["message"]; ok {
		data["message"] = normalizeMessageArrayForOneBot(msg)
	}
	return resp
}

func getPokeTargetID(ctx *Ctx) int64 {
	if ctx == nil {
		return 0
	}
	for _, seg := range ctx.Segments {
		if seg.Type != "poke" {
			continue
		}
		if qq, ok := asInt64FromAny(seg.Data["qq"]); ok {
			return qq
		}
	}
	if ctx.SelfID != 0 {
		return ctx.SelfID
	}
	return 0
}

func normalizeOneBotEventPayload(msg map[string]interface{}) map[string]interface{} {
	if msg == nil {
		return map[string]interface{}{}
	}
	postType := asStringFromAny(msg["post_type"])
	if postType == "" {
		postType = "message"
		msg["post_type"] = postType
	}
	if postType == "message" || postType == "message_sent" {
		if _, ok := msg["message"]; ok {
			if asStringFromAny(msg["message_format"]) == "string" {
				if s, ok := msg["message"].(string); ok {
					msg["raw_message"] = s
				} else {
					msg["message"] = asStringFromAny(msg["message"])
					msg["raw_message"] = asStringFromAny(msg["message"])
				}
			} else {
				msg["message"] = normalizeMessageArrayForOneBot(msg["message"])
			}
		}
		if _, ok := msg["message_type"]; !ok {
			if _, hasGroup := msg["group_id"]; hasGroup {
				msg["message_type"] = "group"
			} else {
				msg["message_type"] = "private"
			}
		}
		if _, ok := msg["sub_type"]; !ok {
			if asStringFromAny(msg["message_type"]) == "private" {
				msg["sub_type"] = "friend"
			} else {
				msg["sub_type"] = "normal"
			}
		}
		sender := ensureMapField(msg, "sender")
		if _, ok := sender["user_id"]; !ok {
			if userID, ok := asInt64FromAny(msg["user_id"]); ok && userID != 0 {
				sender["user_id"] = userID
			}
		}
		if _, ok := msg["message_format"]; !ok {
			if _, ok := msg["message"].(string); ok {
				msg["message_format"] = "string"
			} else {
				msg["message_format"] = "array"
			}
		}
		if _, ok := msg["message_seq"]; !ok {
			if msgID, ok := asInt64FromAny(msg["message_id"]); ok && msgID != 0 {
				msg["message_seq"] = msgID
			}
		}
	}
	if postType == "notice" {
		if _, ok := msg["notice_type"]; !ok {
			if asStringFromAny(msg["sub_type"]) == "poke" {
				msg["notice_type"] = "notify"
			}
		}
		if asStringFromAny(msg["notice_type"]) == "notify" && asStringFromAny(msg["sub_type"]) == "poke" {
			if _, ok := msg["target_id"]; !ok {
				if selfID, ok := asInt64FromAny(msg["self_id"]); ok && selfID != 0 {
					msg["target_id"] = selfID
				}
			}
			if _, ok := msg["raw_info"]; !ok {
				msg["raw_info"] = msg["raw_message"]
			}
		}
	}
	if _, ok := msg["time"]; !ok {
		msg["time"] = time.Now().Unix()
	}
	if _, ok := msg["raw_message"]; !ok {
		if s, ok := msg["message"].(string); ok {
			msg["raw_message"] = s
		} else {
			msg["raw_message"] = ""
		}
	}
	if _, ok := msg["font"]; !ok {
		msg["font"] = 0
	}
	return msg
}

func normalizeOneBotAPIResponse(resp map[string]interface{}, fallbackEcho interface{}, hasFallbackEcho bool) map[string]interface{} {
	if resp == nil {
		resp = map[string]interface{}{}
	}

	status := asStringFromAny(resp["status"])
	if status == "" {
		if retcode, ok := asInt64FromAny(resp["retcode"]); ok && retcode != 0 {
			resp["status"] = "failed"
		} else {
			resp["status"] = "ok"
		}
	}
	if _, ok := resp["retcode"]; !ok {
		if asStringFromAny(resp["status"]) == "failed" {
			resp["retcode"] = 1200
		} else {
			resp["retcode"] = 0
		}
	}
	if _, ok := resp["data"]; !ok {
		resp["data"] = nil
	}
	if _, ok := resp["message"]; !ok {
		resp["message"] = ""
	}
	if _, ok := resp["wording"]; !ok {
		resp["wording"] = asStringFromAny(resp["message"])
	}
	if hasFallbackEcho {
		if _, ok := resp["echo"]; !ok {
			resp["echo"] = fallbackEcho
		}
	}
	return resp
}

func extractEchoKey(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

func buildImmediateFrameworkAPIResponse(action string, raw map[string]interface{}) (map[string]interface{}, bool) {
	params := getActionParams(raw)
	resp := map[string]interface{}{
		"status":  "ok",
		"retcode": 0,
	}
	if echoVal, ok := raw["echo"]; ok {
		resp["echo"] = echoVal
	}

	switch action {
	case "send_msg", "send_private_msg", "send_group_msg", "send_record", "send_voice":
		// OneBot v11 规范中 message_id 最好是 32位正整数，且JS安全数字最大为 2^53-1。
		// 之前使用 UnixNano() 产生了极大的 int64，导致 Node.js/Yunzai 侧可能发生解析溢出或直接抛错忽略该响应
		// 此处改用 安全且符合OneBot规范的 32位正整数
		safeID := int32(time.Now().Unix() & 0x7FFFFFFF)
		resp["data"] = map[string]interface{}{
			"message_id": safeID,
		}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	case "get_group_info":
		groupID, _ := asInt64FromAny(params["group_id"])
		resp["data"] = map[string]interface{}{
			"group_id":         groupID,
			"group_name":       strconv.FormatInt(groupID, 10),
			"member_count":     0,
			"max_member_count": 0,
		}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	case "get_group_member_info":
		groupID, _ := asInt64FromAny(params["group_id"])
		userID, _ := asInt64FromAny(params["user_id"])
		resp["data"] = map[string]interface{}{
			"group_id": groupID,
			"user_id":  userID,
			"nickname": strconv.FormatInt(userID, 10),
			"card":     "",
			"role":     "member",
		}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	case "get_login_info":
		resp["data"] = map[string]interface{}{
			"user_id":  BotSelfID,
			"nickname": "bot",
		}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	case "get_msg":
		messageID, _ := asInt64FromAny(params["message_id"])
		resp["data"] = map[string]interface{}{
			"message_id":   messageID,
			"message":      []interface{}{},
			"raw_message":  "",
			"message_type": "group",
			"time":         time.Now().Unix(),
			"sender":       map[string]interface{}{},
		}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	case "can_send_image", "can_send_record", "get_status", "get_version_info", "get_forward_msg", "get_group_member_list", "get_friend_list", "get_group_list", "get_online_clients", "get_unidirectional_friend_list", "get_essence_msg_list", "get_group_honor_info":
		resp["data"] = map[string]interface{}{}
		return normalizeOneBotAPIResponseByAction(action, raw, resp), true
	default:
		return nil, false
	}
}

func tryHandlePendingAPIResponse(msg []byte) bool {
	raw, err := parseJSONMap(msg)
	if err != nil || raw == nil {
		return false
	}
	echoVal, ok := raw["echo"]
	if !ok {
		return false
	}
	echoKey := extractEchoKey(echoVal)
	if echoKey == "" {
		return false
	}

	pending, ok := PendingAPIRequests.Load(echoKey)
	if !ok || pending == nil {
		PendingAPIRequests.Delete(echoKey)
		return false
	}

	resp := normalizeOneBotAPIResponseByAction(pending.Action, pending.Request, normalizeOneBotAPIResponse(raw, pending.OriginalEcho, pending.HasEcho))
	if pending.ImmediateReply {
		log.Printf("[WS] 收到 %s API异步回包(已即时响应框架): action=%s\n", pending.Framework, pending.Action)
	} else if pending.WSConn != nil {
		respBytes, err := json.Marshal(resp)
		if err == nil {
			if err := pending.WSConn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
				log.Printf("[WS] 低延迟回写 %s API响应失败: action=%s err=%v\n", pending.Framework, pending.Action, err)
			} else {
				log.Printf("[WS] 低延迟回写 %s API响应成功: action=%s\n", pending.Framework, pending.Action)
			}
		}
	}
	select {
	case pending.ResponseCh <- resp:
	default:
	}
	PendingAPIRequests.Delete(echoKey)
	return true
}

func proxyFrameworkAPIRequest(wsConn *WSConn, framework string, raw map[string]interface{}) ([]byte, error) {
	cfg := GlobalConfig.GetConfig()
	action, _ := raw["action"].(string)
	if action == "" {
		return nil, fmt.Errorf("缺少action字段")
	}

	originalEcho, hasOriginalEcho := raw["echo"]
	proxyEcho := fmt.Sprintf("gows-api-%d", time.Now().UnixNano())
	
	// 对 raw 进行深拷贝，避免对并发原始数据的副作用，并自动识别/转换 base64
	proxyPayload := make(map[string]interface{}, len(raw)+1)
	for k, v := range raw {
		if k == "params" {
			if originalParams, ok := v.(map[string]interface{}); ok {
				clonedParams := make(map[string]interface{}, len(originalParams))
				for pk, pv := range originalParams {
					if pk == "message" {
						if originalMsgSlice, ok := pv.([]interface{}); ok {
							clonedMsgSlice := make([]interface{}, len(originalMsgSlice))
							for i, item := range originalMsgSlice {
								if segment, ok := item.(map[string]interface{}); ok {
									clonedSeg := make(map[string]interface{}, len(segment))
									for sk, sv := range segment {
										if sk == "data" {
											if dataMap, ok := sv.(map[string]interface{}); ok {
												clonedData := make(map[string]interface{}, len(dataMap))
												for dk, dv := range dataMap {
													clonedData[dk] = dv
												}
												clonedSeg[sk] = clonedData
											} else {
												clonedSeg[sk] = sv
											}
										} else {
											clonedSeg[sk] = sv
										}
									}
									clonedMsgSlice[i] = clonedSeg
								} else {
									clonedMsgSlice[i] = item
								}
							}
							clonedParams[pk] = clonedMsgSlice
						} else if originalMsgMap, ok := pv.(map[string]interface{}); ok {
							clonedMsgMap := make(map[string]interface{}, len(originalMsgMap))
							for mk, mv := range originalMsgMap {
								if mk == "data" {
									if dataMap, ok := mv.(map[string]interface{}); ok {
										clonedData := make(map[string]interface{}, len(dataMap))
										for dk, dv := range dataMap {
											clonedData[dk] = dv
										}
										clonedMsgMap[mk] = clonedData
									} else {
										clonedMsgMap[mk] = mv
									}
								} else {
									clonedMsgMap[mk] = mv
								}
							}
							clonedParams[pk] = clonedMsgMap
						} else {
							clonedParams[pk] = pv
						}
					} else {
						clonedParams[pk] = pv
					}
				}
				processBase64InParams(clonedParams)
				proxyPayload[k] = clonedParams
			} else {
				proxyPayload[k] = v
			}
		} else {
			proxyPayload[k] = v
		}
	}
	proxyPayload["echo"] = proxyEcho

	pending := &PendingAPIRequest{
		Action:         action,
		Framework:      framework,
		WSConn:         wsConn,
		Request:        proxyPayload,
		OriginalEcho:   originalEcho,
		HasEcho:        hasOriginalEcho,
		ImmediateReply: false,
		ResponseCh:     make(chan map[string]interface{}, 1),
	}

	sendToLLBot := func() error {
		if cfg.LLBotWSMode == "server" {
			if _, ok := WSServerConns.Load("llbot"); ok {
				log.Printf("[WS] 通过LLBot WS代理框架API请求: framework=%s action=%s\n", framework, action)
				return sendViaOneBotWS(proxyPayload)
			}
		} else if cfg.LLBotWS != "" {
			log.Printf("[WS] 通过LLBot WS代理框架API请求: framework=%s action=%s\n", framework, action)
			return sendViaOneBotWS(proxyPayload)
		}
		return fmt.Errorf("LLBot WS未连接，无法代理框架API请求: action=%s", action)
	}

	if immediateResp, ok := buildImmediateFrameworkAPIResponse(action, raw); ok {
		pending.ImmediateReply = true
		PendingAPIRequests.Store(proxyEcho, pending)
		
		// 异步推给LLBot，绝不阻塞框架(Yunzai/Koishi)的API响应！
		// 这样即便包含几万甚至几十万字符的Base64大图片，也不会导致框架发送超时
		go func() {
			if err := sendToLLBot(); err != nil {
				log.Printf("[WS] 异步代理 %s API 到 LLBot 失败: %v\n", action, err)
			}
			// 延迟一分钟清理PendingAPIRequests，给可能的异步回包留足时间
			time.AfterFunc(60*time.Second, func() {
				PendingAPIRequests.Delete(proxyEcho)
			})
		}()

		log.Printf("[WS] 立即回写 %s 框架API响应: action=%s\n", framework, action)
		respBytes, err := json.Marshal(immediateResp)
		if err != nil {
			PendingAPIRequests.Delete(proxyEcho)
			return nil, err
		}
		return respBytes, nil
	}

	PendingAPIRequests.Store(proxyEcho, pending)
	if err := sendToLLBot(); err != nil {
		PendingAPIRequests.Delete(proxyEcho)
		return nil, err
	}

	select {
	case resp := <-pending.ResponseCh:
		return json.Marshal(normalizeOneBotAPIResponseByAction(action, raw, normalizeOneBotAPIResponse(resp, originalEcho, hasOriginalEcho)))
	case <-time.After(12 * time.Second):
		PendingAPIRequests.Delete(proxyEcho)
		return nil, fmt.Errorf("等待LLBot WS API响应超时: action=%s", action)
	}
}

// decodeBase64Robust 鲁棒的 Base64 解码，支持标准/URL、带填充/不带填充等各种组合
func decodeBase64Robust(s string) ([]byte, error) {
	if data, err := base64.StdEncoding.DecodeString(s); err == nil {
		return data, nil
	}
	if data, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return data, nil
	}
	if data, err := base64.URLEncoding.DecodeString(s); err == nil {
		return data, nil
	}
	if data, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return data, nil
	}
	return nil, fmt.Errorf("所有解码尝试均失败")
}

// processBase64InParams 处理并拦截参数中可能包含的超大Base64数据
func processBase64InParams(params map[string]interface{}) {
	// 直接旁路，不进行任何 Base64 到本地 HTTP 转换
	return
}

// tryConvertCQCodeBase64ToLocalHTTP 提取并自动转换 CQ 码中的 Base64
func tryConvertCQCodeBase64ToLocalHTTP(msgStr string) string {
	if !strings.Contains(msgStr, "base64://") && !strings.Contains(msgStr, "data:") {
		return msgStr
	}

	sb := strings.Builder{}
	idx := 0
	for idx < len(msgStr) {
		start := strings.Index(msgStr[idx:], "[CQ:image,")
		if start == -1 {
			sb.WriteString(msgStr[idx:])
			break
		}

		sb.WriteString(msgStr[idx : idx+start])
		idx += start

		end := strings.Index(msgStr[idx:], "]")
		if end == -1 {
			sb.WriteString(msgStr[idx:])
			break
		}

		cqCode := msgStr[idx : idx+end+1]
		idx += end + 1

		newCQ := replaceBase64InCQCode(cqCode)
		sb.WriteString(newCQ)
	}
	return sb.String()
}

// replaceBase64InCQCode 将一个 CQ 码中的 Base64 替换为本地 HTTP URL
func replaceBase64InCQCode(cq string) string {
	if len(cq) <= len("[CQ:image,") || cq[len(cq)-1] != ']' {
		return cq
	}
	content := cq[len("[CQ:image,") : len(cq)-1]
	parts := strings.Split(content, ",")
	modified := false
	for i, part := range parts {
		if strings.HasPrefix(part, "file=") {
			val := part[len("file="):]
			if newURL, ok := tryConvertBase64ToLocalHTTP(val); ok {
				parts[i] = "file=" + newURL
				modified = true
			}
		} else if strings.HasPrefix(part, "url=") {
			val := part[len("url="):]
			if newURL, ok := tryConvertBase64ToLocalHTTP(val); ok {
				parts[i] = "url=" + newURL
				modified = true
			}
		}
	}
	if modified {
		return "[CQ:image," + strings.Join(parts, ",") + "]"
	}
	return cq
}

// tryConvertBase64ToLocalHTTP 将给定的 Base64 数据保存到本地 assets 目录并生成对应的 HTTP URL
func tryConvertBase64ToLocalHTTP(val string) (string, bool) {
	log.Printf("[Base64Proxy] 尝试转换数据，前缀为: %s\n", truncateForLog(val, 64))
	var base64Data string
	var ext string = "png" // 默认后缀名

	if strings.HasPrefix(val, "base64://") {
		base64Data = val[len("base64://"):]
	} else if strings.HasPrefix(val, "data:") {
		semiIdx := strings.Index(val, ";")
		commaIdx := strings.Index(val, ",")
		if semiIdx != -1 && commaIdx != -1 && strings.Contains(val[:commaIdx], "base64") {
			mime := val[5:semiIdx]
			parts := strings.Split(mime, "/")
			if len(parts) == 2 {
				ext = parts[1]
			}
			base64Data = val[commaIdx+1:]
		}
	}

	if base64Data == "" {
		log.Printf("[Base64Proxy] 转换终止：未识别到有效Base64数据段\n")
		return "", false
	}

	// 清洗 Base64 格式
	base64Data = strings.ReplaceAll(base64Data, " ", "")
	base64Data = strings.ReplaceAll(base64Data, "\n", "")
	base64Data = strings.ReplaceAll(base64Data, "\r", "")
	base64Data = strings.TrimSpace(base64Data)

	data, err := decodeBase64Robust(base64Data)
	if err != nil {
		log.Printf("[Base64Proxy] Base64解码彻底失败: %v, length=%d, head=%s\n", err, len(base64Data), truncateForLog(base64Data, 32))
		return "", false
	}

	// 特征字节自动纠正后缀名
	if len(data) > 4 {
		if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 {
			ext = "png"
		} else if data[0] == 0xFF && data[1] == 0xD8 {
			ext = "jpg"
		} else if data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46 {
			ext = "gif"
		} else if data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46 {
			ext = "webp"
		}
	}

	// 生成高可靠随机文件名
	randomID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), int32(time.Now().Unix()&0x7FFFFFFF))
	fileName := fmt.Sprintf("temp_img_%s.%s", randomID, ext)
	imagePath := "assets/" + fileName

	_ = os.MkdirAll("assets", 0755)

	err = os.WriteFile(imagePath, data, 0644)
	if err != nil {
		log.Printf("[Base64Proxy] 保存临时文件失败: %v\n", err)
		return "", false
	}

	// 5分钟后自动异步清理，保护磁盘空间
	time.AfterFunc(5*time.Minute, func() {
		_ = os.Remove(imagePath)
		log.Printf("[Base64Proxy] 已自动清理临时文件: %s\n", imagePath)
	})

	host := detectWebUIHostIP()
	imageURL := fmt.Sprintf("http://%s:%d/assets/%s", host, WebUIPort, fileName)
	log.Printf("[Base64Proxy] 成功转换超大Base64数据为本地HTTP链接: %s (大小: %d bytes)\n", imageURL, len(data))
	return imageURL, true
}

func handleFrameworkInboundRawMessage(wsConn *WSConn, framework string, msg []byte) bool {
	raw, err := parseJSONMap(msg)
	if err != nil {
		log.Printf("[WS] %s 消息解析失败: %v, raw=%s\n", framework, err, truncateForLog(string(msg), 256))
		return true
	}
	if raw == nil {
		return true
	}

	if _, hasAction := raw["action"]; hasAction {
		log.Printf("[WS] 收到 %s 框架API请求\n", framework)
		respBytes, err := proxyFrameworkAPIRequest(wsConn, framework, raw)
		if err != nil {
			log.Printf("[WS] 代理 %s 框架API请求失败: %v\n", framework, err)
			failure := map[string]interface{}{"status": "failed", "retcode": 1400, "msg": err.Error(), "wording": err.Error(), "data": nil}
			if echoVal, ok := raw["echo"]; ok {
				failure["echo"] = echoVal
			}
			respBytes, _ = json.Marshal(failure)
			if wsConn != nil && len(respBytes) > 0 {
				if err := wsConn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
					log.Printf("[WS] 回写 %s 框架API响应失败: %v\n", framework, err)
				} else {
					log.Printf("[WS] 同步回写 %s 框架API失败响应成功\n", framework)
				}
			}
			return true
		}
		if wsConn != nil && len(respBytes) > 0 {
			if err := wsConn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
				log.Printf("[WS] 回写 %s 框架API响应失败: %v\n", framework, err)
			} else {
				log.Printf("[WS] 同步回写 %s 框架API响应成功\n", framework)
			}
		}
		return true
	}

	if _, hasPostType := raw["post_type"]; !hasPostType {
		log.Printf("[WS] 忽略 %s 非事件/非API消息: raw=%s\n", framework, truncateForLog(string(msg), 256))
		return true
	}

	ctx, parseErr := parseOneBotEvent(msg)
	if parseErr != nil {
		log.Printf("[WS] %s 事件解析失败: %v, raw=%s\n", framework, parseErr, truncateForLog(string(msg), 256))
		return true
	}
	if ctx == nil {
		return true
	}

	log.Printf("[WS] 收到 %s 框架回传事件 (type=%s, user=%d, group=%d)\n", framework, ctx.EventType, ctx.UserID, ctx.GroupID)
	// 框架回传的事件不应该转发给LLBot(LLBot是事件生产者，不是消费者)
	// 框架如果需要发送消息，应通过API请求(如send_group_msg)，由proxyFrameworkAPIRequest处理
	// 此处仅记录日志后忽略，避免污染isDuplicateSelfEcho导致后续用户消息被误判
	return true
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func makeSelfEchoKey(ctx *Ctx) string {
	if ctx == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%d:%s", ctx.SelfID, ctx.UserID, ctx.GroupID, ctx.MessageID, ctx.PlainText)
}

func markSelfEcho(ctx *Ctx) {
	key := makeSelfEchoKey(ctx)
	if key == "" {
		return
	}
	expiresAt := time.Now().Add(2 * time.Minute).Unix()
	RecentSelfEchoes.Store(key, expiresAt)
}

func isDuplicateSelfEcho(ctx *Ctx) bool {
	key := makeSelfEchoKey(ctx)
	if key == "" {
		return false
	}
	if v, ok := RecentSelfEchoes.Load(key); ok {
		expiresAt, _ := v.(int64)
		if time.Now().Unix() <= expiresAt {
			RecentSelfEchoes.Delete(key)
			return true
		}
		RecentSelfEchoes.Delete(key)
	}
	return false
}

// ==================== 主函数 ====================

func main() {
	log.Println("========================================")
	log.Println("  Go WS连接中转系统 启动中...")
	log.Println("========================================")

	// 1. 加载配置
	if err := LoadConfig(); err != nil {
		log.Printf("[Main] 加载配置失败: %v，使用默认配置继续\n", err)
	}

	cfg := GlobalConfig.GetConfig()

	// 1.5 从WebUI配置初始化BotSelfID
	if cfg.BotQQ != 0 {
		BotSelfID = cfg.BotQQ
		log.Printf("[Main] 从配置读取机器人QQ号: %d\n", BotSelfID)
	}

	// 1.6 初始化日志文件输出
	if cfg.LogToFile {
		if err := initFileLogger(); err != nil {
			log.Printf("[Main] 日志文件初始化失败: %v\n", err)
		} else {
			log.Println("[Main] 日志文件输出已启用")
		}
	}

	// 2. 初始化 Redis
	if cfg.RedisAddr != "" {
		if err := initRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB); err != nil {
			log.Printf("[Main] Redis初始化失败: %v，系统将在无缓存模式下运行\n", err)
		}
	} else {
		log.Println("[Main] Redis地址未配置，系统将在无缓存模式下运行")
	}

	// 3. 初始化 PostgreSQL
	if cfg.PostgreSQLDSN != "" {
		if err := initPostgreSQL(cfg.PostgreSQLDSN); err != nil {
			log.Printf("[Main] PostgreSQL初始化失败: %v，部分功能不可用\n", err)
		}
	} else {
		log.Println("[Main] PostgreSQL DSN未配置，部分功能不可用")
	}

	// 3.5 启动时自动检查Redis缓存miss，从PostgreSQL拉取数据预热缓存
	if RedisClient != nil && DB != nil {
		go warmUpRedisCache()
	}

	// 4. 启动 WebUI HTTP 服务
	webUIServer := StartWebUI()
	go func() {
		if err := webUIServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[WebUI] HTTP 服务异常退出: %v\n", err)
		}
	}()

	// 等待WebUI启动完成
	time.Sleep(500 * time.Millisecond)

	// 5. 启动 WS 服务器（根据配置为各框架分配端口）
	// 端口分配: 6700=LLBot, 6701=Koishi, 6702=AstrBot, 6703=Yunzai
	frameworkPorts := map[string]int{
		"llbot":   cfg.WSServerPortLLBot,
		"koishi":  cfg.WSServerPortKoishi,
		"astrbot": cfg.WSServerPortAstrBot,
		"yunzai":  cfg.WSServerPortYunzai,
	}
	// 设置默认端口
	defaultPorts := map[string]int{
		"llbot":   6700,
		"koishi":  6701,
		"astrbot": 6702,
		"yunzai":  6703,
	}

	var wsServers []*http.Server
	for framework, port := range frameworkPorts {
		if port == 0 {
			port = defaultPorts[framework]
		}

		// 检查该框架是否配置为server模式
		wsMode := getWSMode(framework)
		if wsMode != "server" {
			continue // client模式不需要启动WS服务器
		}

		fw := framework // 捕获循环变量
		mux := http.NewServeMux()
		mux.HandleFunc("/ws/onebot", func(w http.ResponseWriter, r *http.Request) {
			handleFrameworkWS(w, r, fw)
		})

		// 健康检查
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		wsAddr := fmt.Sprintf(":%d", port)
		server := &http.Server{
			Addr:              wsAddr,
			Handler:           mux,
			ReadTimeout:       10 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			// WriteTimeout不设置，因为WebSocket长连接需要保持打开
			IdleTimeout: 120 * time.Second,
		}
		wsServers = append(wsServers, server)

		go func() {
			log.Printf("[Main] %s WS服务器监听启动于 %s (server模式)\n", fw, wsAddr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[Main] %s WS服务异常: %v\n", fw, err)
			}
		}()
	}

	log.Println("========================================")
	log.Println("  系统启动完成！")
	log.Printf("  WebUI: http://localhost:%d\n", WebUIPort)
	for framework, port := range frameworkPorts {
		if port == 0 {
			port = defaultPorts[framework]
		}
		wsMode := getWSMode(framework)
		if wsMode == "server" {
			log.Printf("  %s WS: ws://localhost:%d/ws/onebot (server模式)\n", framework, port)
		}
	}
	log.Println("========================================")

	// 6. 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[Main] 正在关闭系统...")

	// 关闭HTTP服务
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if webUIServer != nil {
		webUIServer.Shutdown(shutdownCtx)
	}
	for _, srv := range wsServers {
		srv.Shutdown(shutdownCtx)
	}

	// 关闭数据库连接
	if DB != nil {
		DB.Close()
	}

	// 关闭Redis连接
	if RedisClient != nil {
		RedisClient.Close()
	}

	log.Println("[Main] 系统已关闭")
}
