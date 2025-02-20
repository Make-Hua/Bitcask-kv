package bitcaskkv

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator  /* 索引迭代器 */
	db        *DB             /* 对应 db */
	Options   IteratorOptions /* 对应配置项 */
}

// 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {

	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		Options:   opts,
	}
}

// Rewind 重新回到迭代器的起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入 key 查找第一个大于（或小于）等于的目标 Key，根据这个 Key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 Key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {

	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()

	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器并且释放相关资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {

		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.Options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
