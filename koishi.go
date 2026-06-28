package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ==================== Koishi 判题模块 ====================

// JudgeAuth Koishi判题模块，判断用户/群聊白名单是否有效
// 统一函数签名：func JudgeAuth(userID, groupID int64) JudgeResult
func (KoishiModule) JudgeAuth(userID, groupID int64) JudgeResult {
	return judgeAuthInternal(userID, groupID)
}

// KoishiModule 空结构体，用于模块命名空间
type KoishiModule struct{}

// ==================== 内部判题逻辑 ====================

// judgeAuthInternal 统一判题内部实现
// 群聊场景：优先查群白名单；若群无效/不存在，再回退查用户白名单
// 私聊场景：只查用户白名单
func judgeAuthInternal(userID, groupID int64) JudgeResult {
	if groupID != 0 {
		groupResult := judgeSingleTarget("group_list", 0, groupID)
		if groupResult.IsValid {
			return groupResult
		}
		return judgeSingleTarget("user_list", userID, 0)
	}

	return judgeSingleTarget("user_list", userID, 0)
}

func judgeSingleTarget(tableName string, userID, groupID int64) JudgeResult {
	var key string
	if tableName == "group_list" {
		key = fmt.Sprintf("group:%d", groupID)
	} else {
		key = fmt.Sprintf("user:%d", userID)
	}

	// 1. 查 Redis 缓存
	expireAt, err := getExpireAtFromRedis(key)
	if err == nil {
		return checkExpireAt(expireAt)
	}

	// Redis Miss 或错误，查 PG
	if err != redis.Nil {
		log.Printf("[JudgeAuth] Redis查询异常 key=%s, err=%v, 降级查PG\n", key, err)
	}

	// 2. 查 PostgreSQL
	expireAt, err = getExpireAtFromPG(tableName, userID, groupID)
	if err != nil {
		log.Printf("[JudgeAuth] PG查询失败 table=%s, err=%v\n", tableName, err)
		return JudgeResult{IsValid: false, Reason: "not_found"}
	}

	if expireAt == 0 {
		// 无记录
		return JudgeResult{IsValid: false, Reason: "not_found"}
	}

	// 3. 回写 Redis 缓存 (TTL 24h)
	writeExpireAtToRedis(key, expireAt)

	return checkExpireAt(expireAt)
}

// checkExpireAt 根据 expire_at 时间戳判断白名单状态
func checkExpireAt(expireAt int64) JudgeResult {
	now := time.Now().Unix()
	if expireAt > now {
		return JudgeResult{IsValid: true, Reason: "valid"}
	}
	return JudgeResult{IsValid: false, Reason: "expired"}
}

// ==================== Redis 操作 ====================

// getExpireAtFromRedis 从 Redis Hash 中获取 expire_at 字段
func getExpireAtFromRedis(key string) (int64, error) {
	if RedisClient == nil {
		return 0, fmt.Errorf("Redis客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	val, err := RedisClient.HGet(ctx, key, "expire_at").Result()
	if err != nil {
		return 0, err
	}

	var expireAt int64
	fmt.Sscanf(val, "%d", &expireAt)
	return expireAt, nil
}

// writeExpireAtToRedis 将 expire_at 写入 Redis Hash，TTL 24h
func writeExpireAtToRedis(key string, expireAt int64) {
	if RedisClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pipe := RedisClient.Pipeline()
	pipe.HSet(ctx, key, "expire_at", expireAt)
	pipe.Expire(ctx, key, 24*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[JudgeAuth] Redis回写失败 key=%s, err=%v\n", key, err)
	}
}

// ==================== PostgreSQL 操作 ====================

// getExpireAtFromPG 从 PostgreSQL 查询 expire_at
// ✅ 修复后
func getExpireAtFromPG(tableName string, userID, groupID int64) (int64, error) {
	if DB == nil {
		return 0, fmt.Errorf("数据库连接未初始化")
	}

	var expireAt int64
	var query string
	var args []interface{}

	if tableName == "group_list" {
		query = "SELECT expire_at FROM group_list WHERE qq_group = $1"
		args = []interface{}{groupID}
	} else {
		query = "SELECT expire_at FROM user_list WHERE qq_number = $1"
		args = []interface{}{userID}
	}

	err := DB.QueryRow(query, args...).Scan(&expireAt)
	if err != nil {
		if err == sql.ErrNoRows {
			// 查不到记录 = 未购买，返回 0 表示无记录，但不视为错误
			return 0, nil
		}
		// 其他才是真正的数据库错误
		return 0, err
	}

	return expireAt, nil
}

// ==================== 缓存预热 ====================

// warmUpRedisCache 启动时自动检查Redis缓存miss，从PostgreSQL拉取数据预热缓存
// 遍历PG中的所有白名单记录，如果Redis中没有对应缓存则写入，TTL为24h
func warmUpRedisCache() {
	log.Println("[CacheWarmUp] 开始预热Redis缓存...")
	const pageSize = 500

	for offset := 0; ; offset += pageSize {
		rows, err := DB.Query("SELECT qq_number, expire_at FROM user_list WHERE expire_at > 0 ORDER BY qq_number LIMIT $1 OFFSET $2", pageSize, offset)
		if err != nil {
			log.Printf("[CacheWarmUp] 查询用户白名单失败: %v\n", err)
			break
		}
		count := 0
		hasRows := false
		for rows.Next() {
			hasRows = true
			var qqNumber int64
			var expireAt int64
			if err := rows.Scan(&qqNumber, &expireAt); err != nil {
				continue
			}
			key := fmt.Sprintf("user:%d", qqNumber)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			exists, err := RedisClient.Exists(ctx, key).Result()
			cancel()
			if err == nil && exists == 0 {
				writeExpireAtToRedis(key, expireAt)
				count++
			}
		}
		rows.Close()
		log.Printf("[CacheWarmUp] 用户白名单分批预热: offset=%d, 新写入=%d\n", offset, count)
		if !hasRows {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for offset := 0; ; offset += pageSize {
		rows2, err := DB.Query("SELECT qq_group, expire_at FROM group_list WHERE expire_at > 0 ORDER BY qq_group LIMIT $1 OFFSET $2", pageSize, offset)
		if err != nil {
			log.Printf("[CacheWarmUp] 查询群聊白名单失败: %v\n", err)
			break
		}
		count := 0
		hasRows := false
		for rows2.Next() {
			hasRows = true
			var qqGroup int64
			var expireAt int64
			if err := rows2.Scan(&qqGroup, &expireAt); err != nil {
				continue
			}
			key := fmt.Sprintf("group:%d", qqGroup)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			exists, err := RedisClient.Exists(ctx, key).Result()
			cancel()
			if err == nil && exists == 0 {
				writeExpireAtToRedis(key, expireAt)
				count++
			}
		}
		rows2.Close()
		log.Printf("[CacheWarmUp] 群聊白名单分批预热: offset=%d, 新写入=%d\n", offset, count)
		if !hasRows {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	log.Println("[CacheWarmUp] Redis缓存预热完成")
}
