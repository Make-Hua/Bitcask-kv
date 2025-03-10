package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {

	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3, &data.LogRecordPos{Fid: 1, Offset: 2})
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 获取结果
	pos1 := bt.Get(nil)
	// 判断是否相同
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	/* 观察两次 PUT 后是否能正确覆盖 */
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3, &data.LogRecordPos{Fid: 1, Offset: 2})

	pos2 := bt.Get([]byte("a"))
	// t.Log(pos2)
	// 判断是否相同
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	val1 := &data.LogRecordPos{Fid: 1, Offset: 100}

	res1 := bt.Put(nil, val1)
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2, &data.LogRecordPos{Fid: 1, Offset: 100})

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4, &data.LogRecordPos{Fid: 1, Offset: 2})

}

func TestBTree_Iterator(t *testing.T) {

	bt1 := NewBTree()

	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2.BTree 有数据的情况
	bt1.Put([]byte("code"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())

	iter2.Next()

	assert.Equal(t, false, iter2.Valid())

	// 3.BTree 有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	bt1.Put([]byte("eeee"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	bt1.Put([]byte("acac"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		// t.Log("key = ,", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		// t.Log("key = ,", string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// 4. seek 测试
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("ee")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5. 反向遍历 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("0")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}

}
