package index

import (
	"bitcask-go/data"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {

	// 创建临时目录
	dir, err := os.MkdirTemp("", "bptree-put")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// 初始化 B+ 树索引
	bpt := NewBPlusTree(dir, false)
	defer bpt.tree.Close()

	// 准备测试数据
	key1 := []byte("acc")
	pos1 := &data.LogRecordPos{Fid: 1, Offset: 100}
	key2 := []byte("bbc")
	pos2 := &data.LogRecordPos{Fid: 2, Offset: 200}
	key3 := []byte("avv")
	pos3 := &data.LogRecordPos{Fid: 3, Offset: 300}

	// 存储数据
	res1 := bpt.Put(key1, pos1)
	assert.Nil(t, res1)
	res2 := bpt.Put(key2, pos2)
	assert.Nil(t, res2)
	res3 := bpt.Put(key3, pos3)
	assert.Nil(t, res3)

}

func TestBPlusTree_Get(t *testing.T) {

	// 创建临时目录
	dir, err := os.MkdirTemp("", "bptree-get")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// 初始化 B+ 树索引
	bpt := NewBPlusTree(dir, false)
	defer bpt.tree.Close()

	// 准备测试数据
	key := []byte("test-key")
	pos := &data.LogRecordPos{Fid: 1, Offset: 100}

	// 存储数据
	res2 := bpt.Put(key, pos)
	assert.Nil(t, res2)

	// 获取数据
	result := bpt.Get(key)
	assert.NotNil(t, result)
	assert.Equal(t, pos.Fid, result.Fid)
	assert.Equal(t, pos.Offset, result.Offset)
}

func TestBPlusTree_Delete(t *testing.T) {

	// 创建临时目录
	dir, err := os.MkdirTemp("", "bptree-delete")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// 初始化 B+ 树索引
	bpt := NewBPlusTree(dir, false)
	defer bpt.tree.Close()

	// 删除 nil data
	res1, ok1 := bpt.Delete([]byte("not byte!"))
	assert.False(t, ok1)
	assert.Nil(t, res1)

	// 准备测试数据
	key := []byte("test-key")
	pos := &data.LogRecordPos{Fid: 1, Offset: 100}

	// 存储数据
	res2 := bpt.Put(key, pos)
	assert.Nil(t, res2)

	// 删除数据
	res3, deleted := bpt.Delete(key)
	assert.False(t, deleted)
	assert.Nil(t, res3)

	// 再次获取数据，应该返回 nil
	result := bpt.Get(key)
	assert.Nil(t, result)
}

func TestBPlusTree_Size(t *testing.T) {

	// 创建临时目录
	dir, err := os.MkdirTemp("", "bptree-size")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// 初始化 B+ 树索引
	bpt := NewBPlusTree(dir, false)
	defer bpt.tree.Close()

	// 准备测试数据
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}
	pos := &data.LogRecordPos{Fid: 1, Offset: 100}

	// 存储数据
	for _, key := range keys {
		res1 := bpt.Put(key, pos)
		assert.Nil(t, res1)
	}

	// 获取索引大小
	size := bpt.Size()
	assert.Equal(t, len(keys), size)

}

func TestBPlusTree_Iterator(t *testing.T) {

	// 创建临时目录
	dir, err := os.MkdirTemp("", "bptree-iter")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	// 初始化 B+ 树索引
	bpt := NewBPlusTree(dir, false)

	// 存储数据
	res1 := bpt.Put([]byte("aabc"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Nil(t, res1)
	res2 := bpt.Put([]byte("bcba"), &data.LogRecordPos{Fid: 1, Offset: 20})
	assert.Nil(t, res2)
	rea3 := bpt.Put([]byte("bcca"), &data.LogRecordPos{Fid: 1, Offset: 30})
	assert.Nil(t, rea3)
	rea4 := bpt.Put([]byte("dada"), &data.LogRecordPos{Fid: 2, Offset: 10})
	assert.Nil(t, rea4)
	rea5 := bpt.Put([]byte("acev"), &data.LogRecordPos{Fid: 2, Offset: 20})
	assert.Nil(t, rea5)

	iter := bpt.Iterator(false)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		// t.Log(string(iter.Key()))
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
	iter.Close() // 确保迭代器使用完毕后关闭

}
