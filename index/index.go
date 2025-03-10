package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// 抽象的索引的接口  后续如果接入其余数据结构科直接使用这个接口
type Indexer interface {

	// Put 向索引中存储 key 对应的索引信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 通过 key 取出对应位置的索引信息
	Get(key []byte) *data.LogRecordPos

	// Delete 通过 key 删除对应位置的索引信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

// 抽象索引
type IndexType = int8

const (
	Btree  IndexType = iota + 1 /* Btree 索引 */
	ART                         /* ART 自适应基树 */
	BPTree                      /* B+ 树索引 */
)

// NewIndex 根据类型初始化索引
func NewIndex(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
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

// 通用索引迭代器接口
type Iterator interface {

	// Rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入 key 查找第一个大于（或小于）等于的目标 Key，根据这个 Key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 Key
	Next()

	// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器并且释放相关资源
	Close()
}
