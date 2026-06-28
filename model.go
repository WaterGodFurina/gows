package main

import (
	"sync"
)

// ==================== 统一事件上下文 ====================

// Ctx 统一事件上下文，系统内所有模块统一使用此结构体解析与传递数据
type Ctx struct {
	EventType     string     // message, message_sent, notice
	SubType       string     // group, private, poke
	UserID        int64      // 发送者QQ
	GroupID       int64      // 群号(私聊为0)
	GroupName     string     // 群名(若可获取)
	SelfID        int64      // 机器人QQ
	RawMessage    string     // 原始消息
	Segments      []Segment  // 解析后的消息段数组
	PlainText     string     // 纯文本内容(去除at等)
	Time          int64      // 事件时间(Unix时间戳)
	MessageType   string     // 消息类型: group, private
	MessageID     int64      // 消息ID
	MessageSeq    int64      // 消息序号(LLBot/OB11常见字段)
	MessageFormat string     // array|string
	Font          int64      // 字体
	Sender        SenderInfo // 发送者信息
}

// SenderInfo 发送者信息
type SenderInfo struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname"`
	Card     string `json:"card"`
	Role     string `json:"role"`
}

// ==================== 统一消息段 ====================

// Segment 统一消息段
type Segment struct {
	Type string                 `json:"type"` // text, at, image, poke
	Data map[string]interface{} `json:"data"` // 数据域
}

// ==================== 判题结果 ====================

// JudgeResult 判题结果，由判题模块返回给main.go
type JudgeResult struct {
	IsValid bool   // true=白名单未到期(有效), false=白名单到期或不存在(无效)
	Reason  string // 原因: "valid", "expired", "not_found"
}

// ==================== 核心配置 ====================

// Config 核心配置结构体，使用 sync.RWMutex 保证并发读写安全
type Config struct {
	mu sync.RWMutex `json:"-"`

	// 路由行为配置
	PokeHandler string `json:"poke_handler"` // "koishi", "astrbot", "yunzai"

	// 框架 WS 链接
	KoishiWS         string `json:"koishi_ws"`
	KoishiWSAuth     string `json:"koishi_ws_auth"` // Koishi WS鉴权密码(可为空)
	KoishiWSMode     string `json:"koishi_ws_mode"` // Koishi WS模式: "client"或"server"
	AstrBotWS        string `json:"astrbot_ws"`
	AstrBotWSAuth    string `json:"astrbot_ws_auth"` // AstrBot WS鉴权密码(可为空)
	AstrBotWSMode    string `json:"astrbot_ws_mode"` // AstrBot WS模式: "client"或"server"
	YunzaiWS         string `json:"yunzai_ws"`
	YunzaiWSAuth     string `json:"yunzai_ws_auth"` // Yunzai WS鉴权密码(可为空)
	YunzaiWSMode     string `json:"yunzai_ws_mode"` // Yunzai WS模式: "client"或"server"
	FreeKoishiWS     string `json:"freekoishi_ws"`     // 已废弃，保留字段兼容旧配置
	FreeKoishiWSAuth string `json:"freekoishi_ws_auth"` // 已废弃
	FreeKoishiWSMode string `json:"freekoishi_ws_mode"` // 已废弃
	LLBotWS          string `json:"llbot_ws"`           // 后端上行WS
	LLBotWSAuth      string `json:"llbot_ws_auth"`      // LLBot WS鉴权密码(可为空)
	LLBotWSMode      string `json:"llbot_ws_mode"`      // LLBot WS模式: "client"或"server"

	// WS服务器端口配置（仅在server模式下生效）
	WSServerPortLLBot      int `json:"ws_server_port_llbot"`      // LLBot WS服务器端口(默认6700)
	WSServerPortKoishi     int `json:"ws_server_port_koishi"`     // Koishi WS服务器端口(默认6701)
	WSServerPortAstrBot    int `json:"ws_server_port_astrbot"`    // AstrBot WS服务器端口(默认6702)
	WSServerPortYunzai     int `json:"ws_server_port_yunzai"`     // Yunzai WS服务器端口(默认6703)
	WSServerPortFreeKoishi int `json:"ws_server_port_freekoishi"` // 已废弃

	// 支付与API配置
	GroupPrice       float64 `json:"group_price"` // 群聊价格(元/月)
	UserPrice        float64 `json:"user_price"`  // 用户价格(元/月)
	AfdianToken      string  `json:"afdian_token"`
	AfdianUserID     string  `json:"afdian_user_id"`
	LLBotHTTPAPI     string  `json:"llbot_http_api"`      // LLBot HTTP API地址
	LLBotHTTPAPIAuth string  `json:"llbot_http_api_auth"` // LLBot HTTP API鉴权密码(可为空)

	// 核心配置
	BotQQ          int64  `json:"bot_qq"`           // 机器人QQ号，用于@bot识别和自身消息放行
	AccessToken   string `json:"access_token"` // 通用OneBot鉴权token（各专用token为空时回退使用）
	MasterQQ      int64  `json:"master_qq"`
	PostgreSQLDSN string `json:"postgresql_dsn"`
	RedisAddr     string `json:"redis_addr"`
	RedisDB       int    `json:"redis_db"`       // Redis数据库选择(0-15)
	RedisPassword string `json:"redis_password"` // Redis密码(可为空)
	LogToFile     bool   `json:"log_to_file"`    // 是否输出日志到logs文件夹
}

// GetConfig 并发安全地读取配置
func (c *Config) GetConfig() Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c
}

// UpdateConfig 并发安全地更新配置
// 注意：不能直接 *c = newCfg，因为会覆盖 mutex 字段导致 Unlock of unlocked RWMutex
func (c *Config) UpdateConfig(newCfg Config) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 逐字段赋值，跳过 mu 字段
	c.PokeHandler = newCfg.PokeHandler
	c.KoishiWS = newCfg.KoishiWS
	c.KoishiWSAuth = newCfg.KoishiWSAuth
	c.KoishiWSMode = newCfg.KoishiWSMode
	c.AstrBotWS = newCfg.AstrBotWS
	c.AstrBotWSAuth = newCfg.AstrBotWSAuth
	c.AstrBotWSMode = newCfg.AstrBotWSMode
	c.YunzaiWS = newCfg.YunzaiWS
	c.YunzaiWSAuth = newCfg.YunzaiWSAuth
	c.YunzaiWSMode = newCfg.YunzaiWSMode
	c.FreeKoishiWS = newCfg.FreeKoishiWS
	c.FreeKoishiWSAuth = newCfg.FreeKoishiWSAuth
	c.FreeKoishiWSMode = newCfg.FreeKoishiWSMode
	c.LLBotWS = newCfg.LLBotWS
	c.LLBotWSAuth = newCfg.LLBotWSAuth
	c.LLBotWSMode = newCfg.LLBotWSMode
	c.WSServerPortLLBot = newCfg.WSServerPortLLBot
	c.WSServerPortKoishi = newCfg.WSServerPortKoishi
	c.WSServerPortAstrBot = newCfg.WSServerPortAstrBot
	c.WSServerPortYunzai = newCfg.WSServerPortYunzai
	c.WSServerPortFreeKoishi = newCfg.WSServerPortFreeKoishi
	c.GroupPrice = newCfg.GroupPrice
	c.UserPrice = newCfg.UserPrice
	c.AfdianToken = newCfg.AfdianToken
	c.AfdianUserID = newCfg.AfdianUserID
	c.LLBotHTTPAPI = newCfg.LLBotHTTPAPI
	c.LLBotHTTPAPIAuth = newCfg.LLBotHTTPAPIAuth
	c.BotQQ = newCfg.BotQQ
	c.MasterQQ = newCfg.MasterQQ
	c.AccessToken = newCfg.AccessToken
	c.PostgreSQLDSN = newCfg.PostgreSQLDSN
	c.RedisAddr = newCfg.RedisAddr
	c.RedisDB = newCfg.RedisDB
	c.RedisPassword = newCfg.RedisPassword
	c.LogToFile = newCfg.LogToFile
}

// ==================== 支付确认状态机 ====================

// PaymentConfirm 支付确认状态，用于内存中的二次确认Map
type PaymentConfirm struct {
	PaymentID     string
	Identifier    string
	ConfirmChan   chan bool // 确认通道
	ExpiresAt     int64     // 过期时间(Unix时间戳)
	InitiatorQQ   int64     // 发起人QQ
	TargetType    string    // user/group
	TargetID      int64
	ReplyGroupID  int64 // 发起命令所在群号，私聊为0
	OriginalOrder string
}

type PendingAPIRequest struct {
	Action         string
	Framework      string
	WSConn         *WSConn
	Request        map[string]interface{}
	OriginalEcho   interface{}
	HasEcho        bool
	ImmediateReply bool
	ResponseCh     chan map[string]interface{}
}

type PaymentStore struct {
	mu sync.RWMutex
	m  map[string]*PaymentConfirm
}

func NewPaymentStore() *PaymentStore {
	return &PaymentStore{m: make(map[string]*PaymentConfirm)}
}

func (s *PaymentStore) Load(key string) (*PaymentConfirm, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

func (s *PaymentStore) Store(key string, value *PaymentConfirm) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}

func (s *PaymentStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

func (s *PaymentStore) Range(fn func(string, *PaymentConfirm) bool) {
	s.mu.RLock()
	items := make([]struct {
		k string
		v *PaymentConfirm
	}, 0, len(s.m))
	for k, v := range s.m {
		items = append(items, struct {
			k string
			v *PaymentConfirm
		}{k: k, v: v})
	}
	s.mu.RUnlock()
	for _, item := range items {
		if !fn(item.k, item.v) {
			return
		}
	}
}

type PendingAPIRequestStore struct {
	mu sync.RWMutex
	m  map[string]*PendingAPIRequest
}

func NewPendingAPIRequestStore() *PendingAPIRequestStore {
	return &PendingAPIRequestStore{m: make(map[string]*PendingAPIRequest)}
}

func (s *PendingAPIRequestStore) Load(key string) (*PendingAPIRequest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

func (s *PendingAPIRequestStore) Store(key string, value *PendingAPIRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}

func (s *PendingAPIRequestStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// ==================== 全局变量声明 ====================

var (
	GlobalConfig       Config
	PaymentMap         = NewPaymentStore()
	EulaCache          sync.Map // Key: QQ号(int64), Value: eula状态(int)
	WSConnections      sync.Map // Key: WS URL(string), Value: *WSConn
	RecentSelfEchoes   sync.Map // Key: 指纹(string), Value: 过期Unix时间(int64)
	PendingAPIRequests = NewPendingAPIRequestStore()

	// WSServerConns 记录WS服务器模式下各框架的连接
	// Key: 框架名称(string, 如"koishi","astrbot","yunzai","llbot"), Value: *WSConn
	WSServerConns sync.Map
)

// ==================== 前缀与判题模块映射 ====================

// PrefixConfig 前缀与判题模块的映射关系
type PrefixConfig struct {
	Prefix   string // 消息前缀
	JudgeMod string // 对应的判题模块名称
}

// GetPrefixConfigs 返回前缀配置列表
// 半角符号前缀用于Yunzai: 常见的半角符号如 ! . / ~ # $ 等
func GetPrefixConfigs() []PrefixConfig {
	return []PrefixConfig{
		{Prefix: "//", JudgeMod: "koishi"},
		{Prefix: "%", JudgeMod: "astrbot"},
		{Prefix: "!", JudgeMod: "yunzai"},
		{Prefix: ".", JudgeMod: "yunzai"},
		{Prefix: "~", JudgeMod: "yunzai"},
		{Prefix: "#", JudgeMod: "yunzai"},
		{Prefix: "/", JudgeMod: "yunzai"},
		{Prefix: "*", JudgeMod: "yunzai"},
		{Prefix: "+", JudgeMod: "yunzai"},
		{Prefix: ",", JudgeMod: "yunzai"},
		{Prefix: "_", JudgeMod: "yunzai"},
	}
}
