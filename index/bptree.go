package index

import (
	"bitcask-go/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// B+ 树索引
// 封装 go.etcd.io/bbolt 库
type BPlusTree struct {
	tree *bbolt.DB
}

// 初始化 b+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {

	// 处理配置项
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites

	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("falied to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

// Put 向索引中存储 key 对应的索引信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {

	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("falied to put value in bptree")
	}
	if len(oldVal) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldVal)
}

// Get 通过 key 取出对应位置的索引信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

// Delete 通过 key 删除对应位置的索引信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal := bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("falied to put value in bptree")
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldVal), true
}

// Size 索引中的数据量
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

// Iterator 索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+ 树迭代器
type bptreeIterator struct {
	tx      *bbolt.Tx     /* 库所需 */
	cursor  *bbolt.Cursor /* 库所需 */
	reverse bool          /* 正反遍历控制项 */

	// 方便实现迭代器
	currKey   []byte
	currValue []byte
}

// 创建 B+ 树迭代器
func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {

	// 手动开启事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin  a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

// Rewind 重新回到迭代器的起点
func (bpi *bptreeIterator) Rewind() {

	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

// Seek 根据传入 key 查找第一个大于（或小于）等于的目标 Key，根据这个 Key 开始遍历
func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

// Next 跳转到下一个 Key
func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

// Key 当前遍历位置的 Key 数据
func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

// Value 当前遍历位置的 Value 数据
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

// Close 关闭迭代器并且释放相关资源
func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
