package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// 自适应基数树索引
// 封装 https://github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// 初始化 BTree 索引结构
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的索引信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

// Get 通过 key 取出对应位置的索引信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	val, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return val.(*data.LogRecordPos)
}

// Delete 通过 key 删除对应位置的索引信息
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

// Size 索引中的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// Close 关闭索引
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Art 索引迭代器
type artIterator struct {
	currIndex int     /* 当前遍历的下标 */
	reverse   bool    /* 是否是反向遍历 */
	values    []*Item /* key+位置索引信息 */
}

// 新建 btreeIterator 结构
func newARTIterator(tree goart.Tree, reverse bool) *artIterator {

	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

// Seek 根据传入 key 查找第一个大于（或小于）等于的目标 Key，根据这个 Key 开始遍历
func (ai *artIterator) Seek(key []byte) {

	var idx int
	if ai.reverse {
		idx = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		idx = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
	ai.currIndex = idx
}

// Next 跳转到下一个 Key
func (ai *artIterator) Next() {
	ai.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的 Key 数据
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器并且释放相关资源
func (ai *artIterator) Close() {
	ai.values = nil
}
