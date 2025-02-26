package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
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

// Size 返回数据量的多少
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Iterator 初始化 BTree 迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

// Close 关闭索引
func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     /* 当前遍历的下标 */
	reverse   bool    /* 是否是反向遍历 */
	values    []*Item /* key+位置索引信息 */
}

// 新建 btreeIterator 结构
func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {

	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入 key 查找第一个大于（或小于）等于的目标 Key，根据这个 Key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {

	var idx int
	if bti.reverse {
		idx = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		idx = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
	bti.currIndex = idx
}

// Next 跳转到下一个 Key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 Key 数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器并且释放相关资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
