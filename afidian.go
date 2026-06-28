package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	// redis is used via global RedisClient
)

// ==================== 爱发电 API 相关结构体 ====================

// AfdianOrder 爱发电订单结构
type AfdianOrder struct {
	OrderID     string  `json:"order_id"`
	TotalAmount float64 `json:"total_amount"`
	Status      int     `json:"status"`
	PlanID      string  `json:"plan_id"`
	CreatedAt   string  `json:"created_at"`
}

func (o *AfdianOrder) UnmarshalJSON(data []byte) error {
	type rawOrder struct {
		OrderID     string      `json:"order_id"`
		OutTradeNo  string      `json:"out_trade_no"`
		TotalAmount interface{} `json:"total_amount"`
		Status      int         `json:"status"`
		PlanID      string      `json:"plan_id"`
		CreatedAt   string      `json:"created_at"`
	}

	var raw rawOrder
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	o.OrderID = raw.OrderID
	if o.OrderID == "" {
		o.OrderID = raw.OutTradeNo
	}
	switch v := raw.TotalAmount.(type) {
	case float64:
		o.TotalAmount = v
	case string:
		o.TotalAmount = parsePriceFromString(v)
	case json.Number:
		f, _ := v.Float64()
		o.TotalAmount = f
	case nil:
		o.TotalAmount = 0
	default:
		o.TotalAmount = parsePriceFromString(fmt.Sprint(v))
	}
	o.Status = raw.Status
	o.PlanID = raw.PlanID
	o.CreatedAt = raw.CreatedAt
	return nil
}

// AfdianResponse 爱发电API响应结构
type AfdianResponse struct {
	EC      int           `json:"ec"`
	EM      string        `json:"em"`
	Message string        `json:"message"`
	Data    AfdianDataRes `json:"data"`
}

func (r AfdianResponse) ErrorMessage() string {
	if r.Message != "" {
		return r.Message
	}
	if r.EM != "" {
		return r.EM
	}
	return "未知错误"
}

// AfdianDataRes 爱发电数据响应
type AfdianDataRes struct {
	List      []AfdianOrder `json:"list"`
	TotalPage int           `json:"total_page"`
	Order     *AfdianOrder  `json:"order"`
}

// AfdianRequest 爱发电API请求体
type AfdianRequest struct {
	UserID string `json:"user_id"`
	Params string `json:"params"`
	TS     int64  `json:"ts"`
	Sign   string `json:"sign"`
}

// ==================== 支付状态机 ====================

// PaymentContext 支付上下文，每个支付请求启动一个独立协程
type PaymentContext struct {
	PaymentID    string        // 支付唯一ID
	Identifier   string        // 标识符: "user:QQ号" 或 "group:QQ群号"
	InputOrders  []string      // 用户输入的订单号切片
	UserID       int64         // 发起支付的用户QQ
	GroupID      int64         // 目标群号(私聊为0)
	ReplyGroupID int64         // 回复所在群号(私聊为0)
	TargetID     int64         // 目标用户/群ID
	IsGroup      bool          // 是否群聊环境
	ConfirmChan  chan bool     // 二次确认通道
	ValidOrders  []AfdianOrder // 验证通过的有效订单
	TotalAmount  float64       // 总金额
	GroupMonths  int           // 群聊可增月数
	UserMonths   int           // 用户可增月数
	Remainder    float64       // 零头
}

// ==================== 支付流程入口 ====================

// StartPayment 启动支付流程，每个请求一个独立协程，超时300s
func StartPayment(pCtx *PaymentContext) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Afdian] 支付协程异常退出: %v\n", r)
			}
		}()

		if pCtx.PaymentID == "" {
			pCtx.PaymentID = fmt.Sprintf("pay_%d_%d", pCtx.UserID, time.Now().UnixNano())
		}
		confirm := &PaymentConfirm{
			PaymentID:   pCtx.PaymentID,
			Identifier:  pCtx.Identifier,
			ConfirmChan: pCtx.ConfirmChan,
			ExpiresAt:   time.Now().Add(300 * time.Second).Unix(),
			InitiatorQQ: pCtx.UserID,
			TargetType: func() string {
				if pCtx.IsGroup {
					return "group"
				}
				return "user"
			}(),
			TargetID:      pCtx.TargetID,
			ReplyGroupID:  pCtx.ReplyGroupID,
			OriginalOrder: strings.Join(pCtx.InputOrders, ","),
		}
		PaymentMap.Store(pCtx.PaymentID, confirm)
		defer PaymentMap.Delete(pCtx.PaymentID)

		validOrders, duplicateOrders, err := validateAndDeduplicate(pCtx.InputOrders)
		if err != nil {
			sendMessage(pCtx.UserID, pCtx.ReplyGroupID, fmt.Sprintf("订单验证失败: %v", err))
			return
		}
		if len(duplicateOrders) > 0 {
			sendMessage(pCtx.UserID, pCtx.ReplyGroupID, fmt.Sprintf("以下订单已存在，已忽略：%s", strings.Join(duplicateOrders, ", ")))
		}
		if len(validOrders) == 0 {
			sendMessage(pCtx.UserID, pCtx.ReplyGroupID, "无有效订单，请检查订单号后重试")
			return
		}

		pCtx.ValidOrders = validOrders
		calculatePayment(pCtx)

		confirmMsg := buildConfirmMessage(pCtx)
		sendMessage(pCtx.UserID, pCtx.ReplyGroupID, confirmMsg)

		select {
		case confirmed := <-pCtx.ConfirmChan:
			if !confirmed {
				sendMessage(pCtx.UserID, pCtx.ReplyGroupID, "支付已取消")
				return
			}
		case <-time.After(300 * time.Second):
			sendMessage(pCtx.UserID, pCtx.ReplyGroupID, "支付确认超时(300秒)，请重新发起")
			return
		}

		if err := commitPayment(pCtx); err != nil {
			sendMessage(pCtx.UserID, pCtx.ReplyGroupID, fmt.Sprintf("支付失败: %v", err))
			log.Printf("[Afdian] 支付事务失败: %v\n", err)
			return
		}

		successMsg := fmt.Sprintf("支付成功！\n群聊白名单增加: %d月\n用户白名单增加: %d月",
			pCtx.GroupMonths, pCtx.UserMonths)
		sendMessage(pCtx.UserID, pCtx.ReplyGroupID, successMsg)
		log.Printf("[Afdian] 支付成功 paymentID=%s identifier=%s groupMonths=%d userMonths=%d\n",
			pCtx.PaymentID, pCtx.Identifier, pCtx.GroupMonths, pCtx.UserMonths)
	}()
}

// ==================== 第1步: 订单去重与验证 ====================

// validateAndDeduplicate 订单去重与验证
func validateAndDeduplicate(inputOrders []string) ([]AfdianOrder, []string, error) {
	var validOrders []AfdianOrder
	var duplicateOrders []string

	redisExisting := checkOrdersInRedis(inputOrders)

	var newOrders []string
	for _, order := range inputOrders {
		if redisExisting[order] {
			duplicateOrders = append(duplicateOrders, order)
		} else {
			newOrders = append(newOrders, order)
		}
	}

	if len(newOrders) > 0 {
		pgExisting := checkOrdersInPG(newOrders)
		var trulyNewOrders []string
		for _, order := range newOrders {
			if pgExisting[order] {
				duplicateOrders = append(duplicateOrders, order)
				addOrderToRedisCache(order)
			} else {
				trulyNewOrders = append(trulyNewOrders, order)
			}
		}
		newOrders = trulyNewOrders
	}

	if len(duplicateOrders) > 0 {
		log.Printf("[Afdian] 重复订单已忽略: %v\n", duplicateOrders)
	}

	if len(newOrders) == 0 {
		if len(duplicateOrders) > 0 {
			return nil, duplicateOrders, nil
		}
		return nil, nil, fmt.Errorf("无有效订单")
	}

	apiOrders, err := verifyOrdersFromAfdian(newOrders)
	if err != nil {
		return nil, duplicateOrders, fmt.Errorf("爱发电API验证失败: %v", err)
	}

	for _, order := range apiOrders {
		if order.Status == 2 && order.TotalAmount > 0 {
			validOrders = append(validOrders, order)
		}
	}

	return validOrders, duplicateOrders, nil
}

// checkOrdersInRedis 从 Redis buynumber_set 检查订单号是否存在
func checkOrdersInRedis(orders []string) map[string]bool {
	result := make(map[string]bool)
	if RedisClient == nil {
		return result
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, order := range orders {
		exists, err := RedisClient.SIsMember(ctx, "buynumber_set", order).Result()
		if err == nil && exists {
			result[order] = true
		}
	}
	return result
}

// checkOrdersInPG 从 PostgreSQL buynumber 表检查订单号是否存在
func checkOrdersInPG(orders []string) map[string]bool {
	result := make(map[string]bool)
	if DB == nil {
		return result
	}

	for _, order := range orders {
		var id int
		err := DB.QueryRow("SELECT id FROM buynumber WHERE buy_number = $1", order).Scan(&id)
		if err == nil {
			result[order] = true
		}
	}
	return result
}

// addOrderToRedisCache 将订单号加入 Redis 缓存
func addOrderToRedisCache(order string) {
	if RedisClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pipe := RedisClient.Pipeline()
	pipe.SAdd(ctx, "buynumber_set", order)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[Afdian] Redis缓存回写失败 order=%s err=%v\n", order, err)
	}
}

// ==================== 爱发电 API 对接 ====================

// verifyOrdersFromAfdian 调用爱发电API验证订单
func verifyOrdersFromAfdian(orderIDs []string) ([]AfdianOrder, error) {
	cfg := GlobalConfig.GetConfig()
	if cfg.AfdianToken == "" || cfg.AfdianUserID == "" {
		return nil, fmt.Errorf("爱发电配置缺失")
	}

	// 设置总超时，防止翻页查询无限阻塞
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 获取所有订单(翻页)，BugA修复: 第一页确定totalPage后固定，加硬上限50防无限循环
	var allOrders []AfdianOrder
	firstOrders, totalPage, err := queryAfdianOrdersWithContext(ctx, cfg.AfdianToken, cfg.AfdianUserID, 1)
	if err != nil {
		return nil, err
	}
	allOrders = append(allOrders, firstOrders...)
	for page := 2; page <= totalPage && page <= 50; page++ {
		// 检查总超时
		if ctx.Err() != nil {
			return nil, fmt.Errorf("爱发电API查询超时")
		}
		orders, _, err := queryAfdianOrdersWithContext(ctx, cfg.AfdianToken, cfg.AfdianUserID, page)
		if err != nil {
			return nil, err
		}
		allOrders = append(allOrders, orders...)
	}

	// 筛选目标订单号
	orderMap := make(map[string]bool)
	for _, id := range orderIDs {
		orderMap[id] = true
	}

	var result []AfdianOrder
	for _, order := range allOrders {
		if orderMap[order.OrderID] {
			result = append(result, order)
		}
	}

	return result, nil
}

// queryAfdianOrders 查询爱发电订单(单页)
func queryAfdianOrders(token, userID string, page int) ([]AfdianOrder, int, error) {
	return queryAfdianOrdersWithContext(context.Background(), token, userID, page)
}

// queryAfdianOrdersWithContext 查询爱发电订单(单页，带context)
func queryAfdianOrdersWithContext(ctx context.Context, token, userID string, page int) ([]AfdianOrder, int, error) {
	ts := time.Now().Unix()
	params := fmt.Sprintf(`{"page":%d}`, page)
	sign := computeAfdianSign(token, params, ts, userID)

	reqBody := AfdianRequest{
		UserID: userID,
		Params: params,
		TS:     ts,
		Sign:   sign,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://ifdian.net/api/open/query-order",
		strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil, 0, fmt.Errorf("创建请求失败: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("请求爱发电API失败: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("读取响应失败: %v", err)
	}

	var afdianResp AfdianResponse
	if err := json.Unmarshal(body, &afdianResp); err != nil {
		return nil, 0, fmt.Errorf("解析响应失败: %v", err)
	}

	if afdianResp.EC != 200 {
		return nil, 0, fmt.Errorf("爱发电API错误: %s", afdianResp.ErrorMessage())
	}

	orders := afdianResp.Data.List
	if len(orders) == 0 && afdianResp.Data.Order != nil {
		orders = []AfdianOrder{*afdianResp.Data.Order}
	}
	if afdianResp.Data.TotalPage <= 0 {
		afdianResp.Data.TotalPage = 1
	}

	return orders, afdianResp.Data.TotalPage, nil
}

// computeAfdianSign 计算爱发电API签名
// sign = md5({token}params{params_json}ts{timestamp}user_id{user_id})
func computeAfdianSign(token, params string, ts int64, userID string) string {
	raw := fmt.Sprintf("%sparams%sts%duser_id%s", token, params, ts, userID)
	hash := md5.Sum([]byte(raw))
	return hex.EncodeToString(hash[:])
}

// ==================== 第2步: 金额与时间计算 ====================

// calculatePayment 基于多订单累加计算金额与时间
func calculatePayment(pCtx *PaymentContext) {
	cfg := GlobalConfig.GetConfig()

	// 累加所有有效订单金额
	var totalAmount float64
	for _, order := range pCtx.ValidOrders {
		totalAmount += order.TotalAmount
	}
	pCtx.TotalAmount = totalAmount

	// 1个月 = 30天
	if pCtx.IsGroup {
		// 群聊环境计算
		groupPrice := cfg.GroupPrice
		userPrice := cfg.UserPrice

		if groupPrice <= 0 || userPrice <= 0 {
			pCtx.GroupMonths = 0
			pCtx.UserMonths = 0
			pCtx.Remainder = totalAmount
			return
		}

		x := int(math.Floor(totalAmount / groupPrice))   // 群聊月数
		remainder := totalAmount - float64(x)*groupPrice // 剩余金额
		y := int(math.Floor(remainder / userPrice))      // 用户月数
		change := remainder - float64(y)*userPrice       // 零头

		pCtx.GroupMonths = x
		pCtx.UserMonths = x + y // 用户增加 (x+y) 月
		pCtx.Remainder = change
	} else {
		// 私聊环境计算
		userPrice := cfg.UserPrice

		if userPrice <= 0 {
			pCtx.UserMonths = 0
			pCtx.Remainder = totalAmount
			return
		}

		y := int(math.Floor(totalAmount / userPrice)) // 用户月数
		change := totalAmount - float64(y)*userPrice  // 零头

		pCtx.UserMonths = y
		pCtx.GroupMonths = 0
		pCtx.Remainder = change
	}
}

// buildConfirmMessage 构建二次确认消息
func buildConfirmMessage(pCtx *PaymentContext) string {
	var sb strings.Builder
	sb.WriteString("【支付确认】\n")

	// 列出所有参与计算的订单号及金额
	sb.WriteString("本次有效订单：")
	for i, order := range pCtx.ValidOrders {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s(%.2f元)", order.OrderID, order.TotalAmount))
	}
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("总计金额：%.2f元\n", pCtx.TotalAmount))

	if pCtx.IsGroup {
		sb.WriteString(fmt.Sprintf("群聊可增：%d月，用户可增：%d月\n", pCtx.GroupMonths, pCtx.UserMonths))
	} else {
		sb.WriteString(fmt.Sprintf("用户可增：%d月\n", pCtx.UserMonths))
	}

	sb.WriteString(fmt.Sprintf("零头：%.2f元(舍去)\n", pCtx.Remainder))
	sb.WriteString("以上订单号仅用于核对展示，不能直接用于确认或取消。\n")
	sb.WriteString(fmt.Sprintf("支付ID：%s\n", pCtx.PaymentID))
	sb.WriteString(fmt.Sprintf("请回复 \"//支付 确认 %s\" 完成支付；若不想支付此订单，请回复 \"//支付 取消 %s\" (300秒内有效)", pCtx.PaymentID, pCtx.PaymentID))

	return sb.String()
}

// ==================== 第4步: 数据库写入与事务 ====================

// commitPayment 执行数据库写入事务
func commitPayment(pCtx *PaymentContext) error {
	if DB == nil {
		return fmt.Errorf("数据库连接未初始化")
	}

	// 开启 PG 事务
	tx, err := DB.Begin()
	if err != nil {
		return fmt.Errorf("开启事务失败: %v", err)
	}

	// Bug2修复: Go官方推荐模式，defer无条件Rollback
	// Commit成功后调用Rollback是no-op，安全无害
	defer tx.Rollback()

	// 插入 buynumber 表 (UNIQUE约束防并发)
	now := time.Now().Unix()
	for _, order := range pCtx.ValidOrders {
		_, err = tx.Exec(
			"INSERT INTO buynumber (buy_number, user_qq, created_at) VALUES ($1, $2, $3)",
			order.OrderID, pCtx.UserID, now,
		)
		if err != nil {
			// 唯一键冲突 = 订单已被抢占
			if isDuplicateKeyError(err) {
				return fmt.Errorf("订单 %s 已被抢占使用，支付失败", order.OrderID)
			}
			return fmt.Errorf("插入订单号失败: %v", err)
		}
	}

	// 更新白名单时间
	daysToAdd := int64(pCtx.UserMonths * 30)
	if pCtx.IsGroup {
		groupDaysToAdd := int64(pCtx.GroupMonths * 30)
		if err := updateGroupExpireAt(tx, pCtx.TargetID, groupDaysToAdd, pCtx.TotalAmount); err != nil {
			return err
		}
		// 群聊环境下用户也增加时间
		if daysToAdd > 0 {
			if err := updateUserExpireAt(tx, pCtx.UserID, daysToAdd, 0); err != nil {
				return err
			}
		}
	} else {
		if err := updateUserExpireAt(tx, pCtx.UserID, daysToAdd, pCtx.TotalAmount); err != nil {
			return err
		}
	}

	// 事务提交
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("事务提交失败: %v", err)
	}

	// 事务成功后，更新 Redis 缓存
	for _, order := range pCtx.ValidOrders {
		addOrderToRedisCache(order.OrderID)
	}

	// Bug4修复: 从数据库读取实际提交后的 expire_at 再写缓存，而非用 time.Now() 重新计算
	if pCtx.IsGroup {
		key := fmt.Sprintf("group:%d", pCtx.GroupID)
		if actual, err2 := getExpireAtFromPG("group_list", 0, pCtx.GroupID); err2 == nil {
			writeExpireAtToRedis(key, actual)
		}
	}
	if daysToAdd > 0 {
		key := fmt.Sprintf("user:%d", pCtx.UserID)
		if actual, err2 := getExpireAtFromPG("user_list", pCtx.UserID, 0); err2 == nil {
			writeExpireAtToRedis(key, actual)
		}
	}

	return nil
}

// updateUserExpireAt 更新用户白名单过期时间
func updateUserExpireAt(tx *sql.Tx, qqNumber int64, days int64, amount float64) error {
	now := time.Now().Unix()
	newExpireAt := now + days*86400

	// 检查用户是否存在
	var currentExpireAt int64
	err := tx.QueryRow("SELECT expire_at FROM user_list WHERE qq_number = $1", qqNumber).Scan(&currentExpireAt)
	if err == sql.ErrNoRows {
		// 不存在，插入新记录
		_, err = tx.Exec(
			"INSERT INTO user_list (qq_number, eula, eula_agreed_at, paid_total_cents, expire_at) VALUES ($1, 0, 0, $2, $3)",
			qqNumber, int(amount*100), newExpireAt,
		)
		return err
	} else if err != nil {
		return err
	}

	// 已存在，累加时间
	if currentExpireAt > now {
		newExpireAt = currentExpireAt + days*86400
	}

	var paidTotalCents int
	if err := tx.QueryRow("SELECT paid_total_cents FROM user_list WHERE qq_number = $1", qqNumber).Scan(&paidTotalCents); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("查询用户已付总额失败: %w", err)
	}
	paidTotalCents += int(amount * 100)

	_, err = tx.Exec(
		"UPDATE user_list SET expire_at = $1, paid_total_cents = $2 WHERE qq_number = $3",
		newExpireAt, paidTotalCents, qqNumber,
	)
	return err
}

// updateGroupExpireAt 更新群聊白名单过期时间
func updateGroupExpireAt(tx *sql.Tx, qqGroup int64, days int64, amount float64) error {
	now := time.Now().Unix()
	newExpireAt := now + days*86400

	// 检查群是否存在
	var currentExpireAt int64
	err := tx.QueryRow("SELECT expire_at FROM group_list WHERE qq_group = $1", qqGroup).Scan(&currentExpireAt)
	if err == sql.ErrNoRows {
		// 不存在，插入新记录
		_, err = tx.Exec(
			"INSERT INTO group_list (qq_group, paid_total_cents, expire_at) VALUES ($1, $2, $3)",
			qqGroup, int(amount*100), newExpireAt,
		)
		return err
	} else if err != nil {
		return err
	}

	// 已存在，累加时间
	if currentExpireAt > now {
		newExpireAt = currentExpireAt + days*86400
	}

	var paidTotalCents int
	if err := tx.QueryRow("SELECT paid_total_cents FROM group_list WHERE qq_group = $1", qqGroup).Scan(&paidTotalCents); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("查询群聊已付总额失败: %w", err)
	}
	paidTotalCents += int(amount * 100)

	_, err = tx.Exec(
		"UPDATE group_list SET expire_at = $1, paid_total_cents = $2 WHERE qq_group = $3",
		newExpireAt, paidTotalCents, qqGroup,
	)
	return err
}

// isDuplicateKeyError 判断是否为唯一键冲突错误
func isDuplicateKeyError(err error) bool {
	return strings.Contains(err.Error(), "duplicate key") ||
		strings.Contains(err.Error(), "unique constraint") ||
		strings.Contains(err.Error(), "23505")
}

// ==================== 辅助函数 ====================

// BugB修复: 使用带30s超时的HTTP客户端，防止无响应时协程永久挂死
var httpClient = &http.Client{Timeout: 30 * time.Second}

// sendViaOneBotWS 通过OneBot接口发送API请求
// 发送目标完全由 WebUI / WebuiData.json 中的 llbot 配置决定：
// - llbot_ws_mode=server: 写入 WSServerConns["llbot"]
// - llbot_ws_mode=client: 连接 cfg.LLBotWS
// - 都不可用时回退到 HTTP API
func sendViaOneBotWS(payload map[string]interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("序列化WS消息失败: %w", err)
	}

	cfg := GlobalConfig.GetConfig()
	llbotAuth := cfg.LLBotWSAuth
	if llbotAuth == "" {
		llbotAuth = cfg.AccessToken
	}

	if cfg.LLBotWSMode == "server" {
		if v, ok := WSServerConns.Load("llbot"); ok {
			if wsConn, ok := v.(*WSConn); ok && wsConn != nil {
				if err := wsConn.WriteMessage(websocket.TextMessage, data); err == nil {
					return nil
				}
				log.Printf("[Afdian] 通过LLBot server模式WS发送失败: %v\n", err)
				if current, ok := WSServerConns.Load("llbot"); ok && current == wsConn {
					WSServerConns.Delete("llbot")
				}
				wsConn.conn.Close()
			}
		}
	} else if cfg.LLBotWS != "" {
		conn, err := getOrDialWS(cfg.LLBotWS, llbotAuth)
		if err != nil {
			log.Printf("[Afdian] 连接LLBot client模式WS失败 %s: %v\n", cfg.LLBotWS, err)
			scheduleWSReconnect(cfg.LLBotWS)
		} else if err := conn.WriteMessage(websocket.TextMessage, data); err == nil {
			return nil
		} else {
			log.Printf("[Afdian] 通过LLBot client模式WS发送失败 %s: %v\n", cfg.LLBotWS, err)
			if current, ok := WSConnections.Load(cfg.LLBotWS); ok && current == conn {
				WSConnections.Delete(cfg.LLBotWS)
			}
			conn.conn.Close()
			scheduleWSReconnect(cfg.LLBotWS)
		}
	}

	if cfg.LLBotHTTPAPI == "" {
		return fmt.Errorf("LLBot WS/HTTP均不可用，无法发送消息")
	}
	bodyBytes := data
	apiURL := cfg.LLBotHTTPAPI
	if !strings.HasPrefix(apiURL, "http") {
		apiURL = "http://" + apiURL
	}

	action, _ := payload["action"].(string)
	if action == "" {
		return fmt.Errorf("HTTP fallback缺少action字段")
	}
	apiPath := "/" + strings.TrimPrefix(action, "/")

	req, err := http.NewRequest("POST", apiURL+apiPath, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return fmt.Errorf("创建HTTP请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.LLBotHTTPAPIAuth != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.LLBotHTTPAPIAuth)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP发送消息失败: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP发送消息失败, status=%d, body=%s", resp.StatusCode, truncateForLog(string(respBody), 256))
	}

	var apiResp map[string]interface{}
	if len(respBody) > 0 && json.Unmarshal(respBody, &apiResp) == nil {
		if status, ok := apiResp["status"].(string); ok && status != "ok" {
			return fmt.Errorf("OneBot HTTP API返回失败: %s", truncateForLog(string(respBody), 256))
		}
	}
	return nil
}

// sendMessage 通过OneBot WS接口发送文本消息
func sendMessage(userID, groupID int64, text string) {
	var payload map[string]interface{}
	if groupID != 0 {
		payload = map[string]interface{}{
			"action": "send_group_msg",
			"params": map[string]interface{}{
				"group_id": groupID,
				"message":  text,
			},
		}
	} else {
		payload = map[string]interface{}{
			"action": "send_private_msg",
			"params": map[string]interface{}{
				"user_id": userID,
				"message": text,
			},
		}
	}

	if err := sendViaOneBotWS(payload); err != nil {
		log.Printf("[Afdian] 发送文本消息失败: %v\n", err)
	}
}

// sendImageMessage 发送图片消息（通过OneBot WS接口发送，支持跨Docker容器）
func sendImageMessage(userID, groupID int64, imagePath string) {
	// BugE修复: 过滤路径穿越，防止 file:// 路径注入
	imagePath = filepath.Clean(imagePath)
	if strings.Contains(imagePath, "..") {
		log.Printf("[Afdian] 非法图片路径: %s\n", imagePath)
		return
	}

	// 构建图片URL: 自动推导内网可访问主机，端口固定使用 WebUI 8080
	// 优先 192.168.*.*，若不存在则回退到 172.*.*.*
	fileName := filepath.Base(imagePath)
	host := detectWebUIHostIP()
	imageURL := fmt.Sprintf("http://%s:%d/assets/%s", host, WebUIPort, fileName)

	// 使用OneBot 11消息段数组格式发送图片
	// 参照OneBot 11规范: file参数支持URL
	message := []map[string]interface{}{
		{
			"type": "image",
			"data": map[string]interface{}{
				"file": imageURL,
			},
		},
	}

	var payload map[string]interface{}
	if groupID != 0 {
		payload = map[string]interface{}{
			"action": "send_group_msg",
			"params": map[string]interface{}{
				"group_id": groupID,
				"message":  message,
			},
		}
	} else {
		payload = map[string]interface{}{
			"action": "send_private_msg",
			"params": map[string]interface{}{
				"user_id": userID,
				"message": message,
			},
		}
	}

	// 通过OneBot WS连接发送（不再使用HTTP API）
	if err := sendViaOneBotWS(payload); err != nil {
		log.Printf("[Afdian] 发送图片消息失败: %v\n", err)
	}
}

func detectWebUIHostIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("[Afdian] 获取网卡失败，回退127.0.0.1: %v\n", err)
		return "127.0.0.1"
	}

	var dockerFallback string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			default:
				continue
			}
			ip = ip.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ipStr := ip.String()
			if strings.HasPrefix(ipStr, "192.168.") {
				return ipStr
			}
			if dockerFallback == "" && strings.HasPrefix(ipStr, "172.") {
				dockerFallback = ipStr
			}
		}
	}

	if dockerFallback != "" {
		return dockerFallback
	}

	log.Printf("[Afdian] 未找到192.168.*.*或172.*.*.*地址，回退127.0.0.1\n")
	return "127.0.0.1"
}

// parseOrderIDs 解析订单号字符串，支持逗号分隔
func parseOrderIDs(orderStr string) []string {
	orderStr = strings.ReplaceAll(orderStr, " ", "") // 去除所有空格
	if orderStr == "" {
		return nil
	}
	return strings.Split(orderStr, ",")
}

// parsePriceFromString 安全解析价格字符串
func parsePriceFromString(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}
