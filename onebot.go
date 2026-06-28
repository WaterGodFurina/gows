package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	// redis is used via global RedisClient
)

// ==================== OneBot 指令处理模块 ====================

// OneBotModule OneBot指令处理模块
type OneBotModule struct{}

// HandleCommand 处理特殊指令，每次调用启动一个协程
func (OneBotModule) HandleCommand(ctx *Ctx) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[OneBot] 指令处理异常: %v\n", r)
			}
		}()

		plainText := strings.TrimSpace(ctx.PlainText)

		switch {
		case strings.HasPrefix(plainText, "//支付"):
			handlePaymentCommand(ctx)
		case strings.HasPrefix(plainText, "//白名单"):
			handleWhitelistCommand(ctx)
		case strings.HasPrefix(plainText, "//撤销同意eula"): // Bug1: 必须在 //同意eula 之前
			handleRevokeEula(ctx)
		case strings.HasPrefix(plainText, "//同意eula"):
			handleAgreeEula(ctx)
		default:
			log.Printf("[OneBot] 未知指令: %s\n", plainText)
		}
	}()
}

// ==================== "//支付" 指令处理 ====================

// handlePaymentCommand 处理支付指令
// 支持格式:
//
//	//支付           → 发送 buy.png
//	//支付 <订单号>   → 自动解析环境
//	//支付 为用户支付 <订单号>  → 指定用户
//	//支付 为群聊支付 <QQ群号> <订单号>  → 指定群聊
//	//支付 确认 <支付ID>      → 确认支付
//	//支付 取消 <支付ID>      → 取消支付
func handlePaymentCommand(ctx *Ctx) {
	plainText := strings.TrimSpace(ctx.PlainText)
	content := strings.TrimPrefix(plainText, "//支付")
	content = strings.TrimSpace(content)

	// 无参数 → 发送 buyhelp.png
	if content == "" {
		sendImageMessage(ctx.UserID, ctx.GroupID, "assets/buyhelp.png")
		return
	}

	// 获取二维码 → 发送 buy.png
	if content == "获取二维码" {
		sendImageMessage(ctx.UserID, ctx.GroupID, "assets/buy.png")
		return
	}

	// 确认支付
	if strings.HasPrefix(content, "确认") {
		handlePaymentConfirm(ctx, strings.TrimSpace(strings.TrimPrefix(content, "确认")))
		return
	}
	if strings.HasPrefix(content, "取消") {
		handlePaymentCancel(ctx, strings.TrimSpace(strings.TrimPrefix(content, "取消")))
		return
	}

	// 解析支付目标与订单号
	var identifier string
	var orderStr string
	var targetGroupID int64
	var targetUserID int64
	isGroup := ctx.GroupID != 0

	if strings.HasPrefix(content, "为群聊支付") {
		parts := strings.Fields(strings.TrimPrefix(content, "为群聊支付"))
		if len(parts) < 2 {
			sendMessage(ctx.UserID, ctx.GroupID, "格式错误，正确格式: //支付 为群聊支付 <QQ群号> <订单号>")
			return
		}
		var err error
		targetGroupID, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "群号格式错误")
			return
		}
		orderStr = strings.Join(parts[1:], "")
		identifier = fmt.Sprintf("group:%d", targetGroupID)
		isGroup = true
	} else if strings.HasPrefix(content, "为用户支付") {
		orderStr = strings.TrimPrefix(content, "为用户支付")
		orderStr = strings.TrimSpace(orderStr)
		targetUserID = ctx.UserID
		identifier = fmt.Sprintf("user:%d", targetUserID)
		isGroup = false
	} else {
		orderStr = content
		if ctx.GroupID != 0 {
			targetGroupID = ctx.GroupID
			identifier = fmt.Sprintf("group:%d", targetGroupID)
			isGroup = true
		} else {
			targetUserID = ctx.UserID
			identifier = fmt.Sprintf("user:%d", targetUserID)
			isGroup = false
		}
	}

	inputOrders := parseOrderIDs(orderStr)
	if len(inputOrders) == 0 {
		sendMessage(ctx.UserID, ctx.GroupID, "订单号不能为空")
		return
	}

	if existingID, existing := findPendingPaymentByIdentifier(identifier); existing != nil {
		tip := fmt.Sprintf("当前%s已有待确认支付，支付ID：%s。订单号仅用于核对，不能直接用于确认。若要继续原订单，请发送 //支付 确认 %s；若不想支付此订单，请发送 //支付 取消 %s",
			identifier, existingID, existingID, existingID)
		sendMessage(ctx.UserID, ctx.GroupID, tip)
		return
	}

	pCtx := &PaymentContext{
		Identifier:   identifier,
		InputOrders:  inputOrders,
		UserID:       ctx.UserID,
		GroupID:      targetGroupID,
		IsGroup:      isGroup,
		ConfirmChan:  make(chan bool, 1),
		ReplyGroupID: ctx.GroupID,
		TargetID: func() int64 {
			if isGroup {
				return targetGroupID
			}
			return targetUserID
		}(),
	}

	StartPayment(pCtx)
}

// handlePaymentConfirm 处理支付确认
func handlePaymentConfirm(ctx *Ctx, paymentID string) {
	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		sendMessage(ctx.UserID, ctx.GroupID, "请指定支付ID，格式：//支付 确认 <支付ID>")
		return
	}

	confirm, ok := PaymentMap.Load(paymentID)
	if !ok || confirm == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "未找到对应的待确认支付记录")
		return
	}
	if time.Now().Unix() > confirm.ExpiresAt {
		PaymentMap.Delete(paymentID)
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录已过期")
		return
	}
	if confirm.InitiatorQQ != ctx.UserID {
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录不是由您发起，无法确认")
		return
	}

	select {
	case confirm.ConfirmChan <- true:
		log.Printf("[OneBot] 支付确认已发送 paymentID=%s identifier=%s\n", paymentID, confirm.Identifier)
	default:
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录已处理或无法重复确认")
	}
}

func handlePaymentCancel(ctx *Ctx, paymentID string) {
	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		sendMessage(ctx.UserID, ctx.GroupID, "请指定支付ID，格式：//支付 取消 <支付ID>")
		return
	}

	confirm, ok := PaymentMap.Load(paymentID)
	if !ok || confirm == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "未找到对应的待取消支付记录")
		return
	}
	if time.Now().Unix() > confirm.ExpiresAt {
		PaymentMap.Delete(paymentID)
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录已过期")
		return
	}
	if confirm.InitiatorQQ != ctx.UserID {
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录不是由您发起，无法取消")
		return
	}

	select {
	case confirm.ConfirmChan <- false:
		log.Printf("[OneBot] 支付取消已发送 paymentID=%s identifier=%s\n", paymentID, confirm.Identifier)
	default:
		sendMessage(ctx.UserID, ctx.GroupID, "该支付记录已处理或无法重复取消")
	}
}

func findPendingPaymentByIdentifier(identifier string) (string, *PaymentConfirm) {
	var foundID string
	var found *PaymentConfirm
	now := time.Now().Unix()
	PaymentMap.Range(func(paymentID string, confirm *PaymentConfirm) bool {
		if confirm == nil {
			PaymentMap.Delete(paymentID)
			return true
		}
		if now > confirm.ExpiresAt {
			PaymentMap.Delete(paymentID)
			return true
		}
		if confirm.Identifier == identifier {
			foundID = paymentID
			found = confirm
			return false
		}
		return true
	})
	return foundID, found
}

// ==================== "//白名单" 指令处理 ====================

// handleWhitelistCommand 处理白名单指令 (仅主人QQ)
func handleWhitelistCommand(ctx *Ctx) {
	cfg := GlobalConfig.GetConfig()

	// 权限校验：仅主人QQ
	if ctx.UserID != cfg.MasterQQ {
		sendMessage(ctx.UserID, ctx.GroupID, "权限不足，仅主人可操作白名单")
		return
	}

	plainText := strings.TrimSpace(ctx.PlainText)
	content := strings.TrimPrefix(plainText, "//白名单")
	content = strings.TrimSpace(content)

	// 无参数 → 发送 white.png
	if content == "" {
		sendImageMessage(ctx.UserID, ctx.GroupID, "assets/white.png")
		return
	}

	parts := strings.Fields(content)
	if len(parts) < 2 {
		sendMessage(ctx.UserID, ctx.GroupID, "格式错误，正确格式: //白名单 <增加|减少|查询> <用户|群聊> <QQ号|群号> [天数]")
		return
	}

	action := parts[0]     // 增加/减少/查询
	targetType := parts[1] // 用户/群聊

	// 基本参数校验
	if action != "增加" && action != "减少" && action != "查询" {
		sendMessage(ctx.UserID, ctx.GroupID, "未知操作，支持: 增加/减少/查询")
		return
	}
	if targetType != "用户" && targetType != "群聊" {
		sendMessage(ctx.UserID, ctx.GroupID, "目标类型错误，支持: 用户/群聊")
		return
	}

	switch action {
	case "增加":
		if len(parts) < 4 {
			sendMessage(ctx.UserID, ctx.GroupID, "格式错误，正确格式: //白名单 增加 <用户|群聊> <QQ号|群号> <天数>")
			return
		}
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "ID格式错误")
			return
		}
		days, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil || days <= 0 {
			sendMessage(ctx.UserID, ctx.GroupID, "天数格式错误")
			return
		}
		handleWhitelistAdd(ctx, targetType, id, days)

	case "减少":
		if len(parts) < 4 {
			sendMessage(ctx.UserID, ctx.GroupID, "格式错误，正确格式: //白名单 减少 <用户|群聊> <QQ号|群号> <天数>")
			return
		}
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "ID格式错误")
			return
		}
		days, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil || days <= 0 {
			sendMessage(ctx.UserID, ctx.GroupID, "天数格式错误")
			return
		}
		handleWhitelistReduce(ctx, targetType, id, days)

	case "查询":
		if len(parts) < 3 {
			sendMessage(ctx.UserID, ctx.GroupID, "格式错误，正确格式: //白名单 查询 <用户|群聊> <QQ号|群号>")
			return
		}
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "ID格式错误")
			return
		}
		handleWhitelistQuery(ctx, targetType, id)

	default:
		sendMessage(ctx.UserID, ctx.GroupID, "未知操作，支持: 增加/减少/查询")
	}
}

// handleWhitelistAdd 增加白名单天数
// 无记录则创建 expire_at = now + days*86400；有记录则 expire_at += days*86400
func handleWhitelistAdd(ctx *Ctx, targetType string, id int64, days int64) {
	if DB == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "数据库未连接")
		return
	}

	now := time.Now().Unix()
	addSeconds := days * 86400

	if targetType == "群聊" {
		// BugH修复: 用 upsert 替代 SELECT+INSERT，防止并发重复插入
		_, err := DB.Exec(`
                        INSERT INTO group_list (qq_group, paid_total_cents, expire_at)
                        VALUES ($1, 0, $2)
                        ON CONFLICT (qq_group) DO UPDATE
                        SET expire_at = CASE
                                WHEN group_list.expire_at > EXTRACT(EPOCH FROM NOW())::BIGINT
                                THEN group_list.expire_at + $3
                                ELSE EXTRACT(EPOCH FROM NOW())::BIGINT + $3
                        END`,
			id, now+addSeconds, addSeconds,
		)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
			return
		}

		// 从数据库读取实际写入的 expire_at 再写缓存
		key := fmt.Sprintf("group:%d", id)
		if actual, err2 := getExpireAtFromPG("group_list", 0, id); err2 == nil {
			writeExpireAtToRedis(key, actual)
		}

	} else {
		// 用户
		_, err := DB.Exec(`
                        INSERT INTO user_list (qq_number, eula, eula_agreed_at, paid_total_cents, expire_at)
                        VALUES ($1, 0, 0, 0, $2)
                        ON CONFLICT (qq_number) DO UPDATE
                        SET expire_at = CASE
                                WHEN user_list.expire_at > EXTRACT(EPOCH FROM NOW())::BIGINT
                                THEN user_list.expire_at + $3
                                ELSE EXTRACT(EPOCH FROM NOW())::BIGINT + $3
                        END`,
			id, now+addSeconds, addSeconds,
		)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
			return
		}

		// 从数据库读取实际写入的 expire_at 再写缓存
		key := fmt.Sprintf("user:%d", id)
		if actual, err2 := getExpireAtFromPG("user_list", id, 0); err2 == nil {
			writeExpireAtToRedis(key, actual)
		}
	}

	sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("白名单增加成功: %s %d, +%d天", targetType, id, days))
	log.Printf("[OneBot] 白名单增加: type=%s id=%d days=%d\n", targetType, id, days)
}

// handleWhitelistReduce 减少白名单天数
// 无记录报错；有记录则 expire_at -= days*86400 (若小于当前时间则设为当前时间)
func handleWhitelistReduce(ctx *Ctx, targetType string, id int64, days int64) {
	if DB == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "数据库未连接")
		return
	}

	now := time.Now().Unix()
	reduceSeconds := days * 86400

	if targetType == "群聊" {
		var currentExpireAt int64
		err := DB.QueryRow("SELECT expire_at FROM group_list WHERE qq_group = $1", id).Scan(&currentExpireAt)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "该群聊无白名单记录")
			return
		}

		newExpireAt := currentExpireAt - reduceSeconds
		if newExpireAt < now {
			newExpireAt = now
		}
		_, err = DB.Exec("UPDATE group_list SET expire_at = $1 WHERE qq_group = $2", newExpireAt, id)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
			return
		}

		// 更新Redis缓存
		key := fmt.Sprintf("group:%d", id)
		writeExpireAtToRedis(key, newExpireAt)

	} else {
		var currentExpireAt int64
		err := DB.QueryRow("SELECT expire_at FROM user_list WHERE qq_number = $1", id).Scan(&currentExpireAt)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, "该用户无白名单记录")
			return
		}

		newExpireAt := currentExpireAt - reduceSeconds
		if newExpireAt < now {
			newExpireAt = now
		}
		_, err = DB.Exec("UPDATE user_list SET expire_at = $1 WHERE qq_number = $2", newExpireAt, id)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
			return
		}

		// 更新Redis缓存
		key := fmt.Sprintf("user:%d", id)
		writeExpireAtToRedis(key, newExpireAt)
	}

	sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("白名单减少成功: %s %d, -%d天", targetType, id, days))
	log.Printf("[OneBot] 白名单减少: type=%s id=%d days=%d\n", targetType, id, days)
}

// handleWhitelistQuery 查询白名单剩余天数
func handleWhitelistQuery(ctx *Ctx, targetType string, id int64) {
	now := time.Now().Unix()
	var expireAt int64

	if targetType == "群聊" {
		key := fmt.Sprintf("group:%d", id)
		// 先查Redis
		val, err := getExpireAtFromRedis(key)
		if err == nil {
			expireAt = val
		} else {
			// 查PG
			expireAt, err = getExpireAtFromPG("group_list", 0, id)
			if err != nil {
				sendMessage(ctx.UserID, ctx.GroupID, "该群聊无白名单记录")
				return
			}
			writeExpireAtToRedis(key, expireAt)
		}
	} else {
		key := fmt.Sprintf("user:%d", id)
		val, err := getExpireAtFromRedis(key)
		if err == nil {
			expireAt = val
		} else {
			expireAt, err = getExpireAtFromPG("user_list", id, 0)
			if err != nil {
				sendMessage(ctx.UserID, ctx.GroupID, "该用户无白名单记录")
				return
			}
			writeExpireAtToRedis(key, expireAt)
		}
	}

	if expireAt <= now {
		sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("%s %d 白名单已过期", targetType, id))
	} else {
		remainingDays := (expireAt - now) / 86400
		if (expireAt-now)%86400 > 0 {
			remainingDays++
		}
		sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("%s %d 白名单剩余: %d天", targetType, id, remainingDays))
	}
}

// ==================== Eula 指令处理 ====================

// checkEulaAgreed 检查用户是否同意了Eula协议
func checkEulaAgreed(userID int64) bool {
	// 先查内存缓存
	if val, ok := EulaCache.Load(userID); ok {
		return val.(int) == 1
	}

	// 查Redis
	if RedisClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		key := fmt.Sprintf("user:%d", userID)
		val, err := RedisClient.HGet(ctx, key, "eula").Result()
		if err == nil {
			eula, err := strconv.Atoi(val)
			if err != nil {
				log.Printf("[OneBot] 解析eula值失败: %v\n", err)
				eula = 0
			}
			EulaCache.Store(userID, eula)
			return eula == 1
		}
	}

	// 查PG
	if DB != nil {
		var eula int
		err := DB.QueryRow("SELECT eula FROM user_list WHERE qq_number = $1", userID).Scan(&eula)
		if err == nil {
			EulaCache.Store(userID, eula)
			return eula == 1
		}
	}

	return false
}

// handleAgreeEula 处理同意Eula协议
// 无记录则插入 (eula=1, eula_agreed_at=now)；有记录且 eula=1 则提醒已同意
func handleAgreeEula(ctx *Ctx) {
	if DB == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "数据库未连接")
		return
	}

	now := time.Now().Unix()

	// 检查是否已同意
	var eula int
	err := DB.QueryRow("SELECT eula FROM user_list WHERE qq_number = $1", ctx.UserID).Scan(&eula)

	if err != nil {
		// 无记录，插入
		_, err = DB.Exec(
			"INSERT INTO user_list (qq_number, eula, eula_agreed_at, paid_total_cents, expire_at) VALUES ($1, 1, $2, 0, 0)",
			ctx.UserID, now,
		)
		if err != nil {
			sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
			return
		}
		EulaCache.Store(ctx.UserID, 1)
		// 同步更新Redis缓存
		if RedisClient != nil {
			key := fmt.Sprintf("user:%d", ctx.UserID)
			cacheCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			RedisClient.HSet(cacheCtx, key, "eula", 1)
			RedisClient.Expire(cacheCtx, key, 24*time.Hour)
		}
		sendMessage(ctx.UserID, ctx.GroupID, "已同意Eula协议")
		return
	}

	if eula == 1 {
		sendMessage(ctx.UserID, ctx.GroupID, "您已同意过Eula协议")
		return
	}

	// eula=0，更新为1
	_, err = DB.Exec(
		"UPDATE user_list SET eula = 1, eula_agreed_at = $1 WHERE qq_number = $2",
		now, ctx.UserID,
	)
	if err != nil {
		sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
		return
	}

	EulaCache.Store(ctx.UserID, 1)
	// 同步更新Redis缓存
	if RedisClient != nil {
		key := fmt.Sprintf("user:%d", ctx.UserID)
		cacheCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		RedisClient.HSet(cacheCtx, key, "eula", 1)
		RedisClient.Expire(cacheCtx, key, 24*time.Hour)
	}
	sendMessage(ctx.UserID, ctx.GroupID, "已同意Eula协议")
	log.Printf("[OneBot] 用户 %d 同意Eula协议\n", ctx.UserID)
}

// handleRevokeEula 处理撤销同意Eula协议
// eula=1 改为 0；eula=0 提示已撤销；无记录提示未同意过
func handleRevokeEula(ctx *Ctx) {
	if DB == nil {
		sendMessage(ctx.UserID, ctx.GroupID, "数据库未连接")
		return
	}

	var eula int
	err := DB.QueryRow("SELECT eula FROM user_list WHERE qq_number = $1", ctx.UserID).Scan(&eula)

	if err != nil {
		sendMessage(ctx.UserID, ctx.GroupID, "您未同意过Eula协议")
		return
	}

	if eula == 0 {
		sendMessage(ctx.UserID, ctx.GroupID, "您已撤销过Eula协议")
		return
	}

	// eula=1，更新为0
	_, err = DB.Exec("UPDATE user_list SET eula = 0 WHERE qq_number = $1", ctx.UserID)
	if err != nil {
		sendMessage(ctx.UserID, ctx.GroupID, fmt.Sprintf("操作失败: %v", err))
		return
	}

	EulaCache.Store(ctx.UserID, 0)
	// 同步更新Redis缓存
	if RedisClient != nil {
		key := fmt.Sprintf("user:%d", ctx.UserID)
		cacheCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		RedisClient.HSet(cacheCtx, key, "eula", 0)
		RedisClient.Expire(cacheCtx, key, 24*time.Hour)
	}
	sendMessage(ctx.UserID, ctx.GroupID, "已撤销同意Eula协议")
	log.Printf("[OneBot] 用户 %d 撤销同意Eula协议\n", ctx.UserID)
}
