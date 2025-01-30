package utils

import (
	"fmt"
)

var (
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") // 更规范的字符集
)

// 获取测试使用的 key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

// 获取测试使用的 value
func GetTestValue(n int) []byte {

	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		// 按顺序循环使用 letters 中的字符（非随机）
		buf[i] = letters[i%len(letters)]
	}
	return []byte("bitcask-go-value-" + string(buf))
}
