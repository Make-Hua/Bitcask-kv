package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

// BTree 索引,主要封装了 google 的 btree kv
// https:://github.com/google/btree
type BTree struct {
	tree *btree.BTree  /* BTree 实例 */
	lock *sync.RWMutex /* google BTree 多线程 write 不安全，Read 安全，所以需要锁自行加锁 */
}

// 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的索引信息
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {

	// 封装 Item
	it := &Item{key: key, pos: pos}

	// 对 BTree 的写操作需要加锁
	bt.lock.Lock()

	// 调用 BTree 内部提供的 insert 接口存储信息
	bt.tree.ReplaceOrInsert(it)

	bt.lock.Unlock()

	return true
}

// Get 通过 key 取出对应位置的索引信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {

	// 封装 Item
	it := &Item{key: key}

	// 获取 key 对应的信息
	btreeItem := bt.tree.Get(it)

	// 如果未找到返回 nil
	if btreeItem == nil {
		return nil
	}

	return btreeItem.(*Item).pos
}

// Delete 通过 key 删除对应位置的索引信息
func (bt *BTree) Delete(key []byte) bool {

	// 封装 Item
	it := &Item{key: key}

	// 写操作加锁
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()

	if oldItem == nil {
		return false
	}
	return true
}
