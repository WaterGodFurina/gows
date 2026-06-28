package main

// ==================== Yunzai 判题模块 ====================

// YunzaiModule 空结构体，用于模块命名空间
type YunzaiModule struct{}

// JudgeAuth Yunzai判题模块，判断用户/群聊白名单是否有效
// 统一函数签名：func JudgeAuth(userID, groupID int64) JudgeResult
// 内部逻辑与 koishi.go 完全一致，共享 judgeAuthInternal 实现
func (YunzaiModule) JudgeAuth(userID, groupID int64) JudgeResult {
	return judgeAuthInternal(userID, groupID)
}
