package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// 抽象的索引的接口  后续如果接入其余数据结构科直接使用这个接口
type Indexer interface {

	// Put 向索引中存储 key 对应的索引信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 通过 key 取出对应位置的索引信息
	Get(key []byte) *data.LogRecordPos

	// Delete 通过 key 删除对应位置的索引信息
	Delete(key []byte) bool
}

// 实现 BTree 中的 Item
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// BTree key 的比较逻辑
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
