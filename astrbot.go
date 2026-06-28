package main

// ==================== AstrBot 判题模块 ====================

// AstrBotModule 空结构体，用于模块命名空间
type AstrBotModule struct{}

// JudgeAuth AstrBot判题模块，判断用户/群聊白名单是否有效
// 统一函数签名：func JudgeAuth(userID, groupID int64) JudgeResult
// 内部逻辑与 koishi.go 完全一致，共享 judgeAuthInternal 实现
func (AstrBotModule) JudgeAuth(userID, groupID int64) JudgeResult {
	return judgeAuthInternal(userID, groupID)
}
