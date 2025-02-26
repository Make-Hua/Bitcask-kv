package bitcaskkv

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {

	// 初始化数据库
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Iterator1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())

	iterator.Close()

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_Iterator_One_Value(t *testing.T) {

	// 初始化数据库
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Iterator2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	//t.Log(utils.GetTestValue(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)

	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()

	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestValue(10), val)
	//t.Log(utils.GetTestValue(10))
	iterator.Close()

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_Iterator_Multi_Value(t *testing.T) {

	// 初始化数据库
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Iterato3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbb"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccc"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ddd"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("eee"), utils.GetTestValue(10))
	assert.Nil(t, err)

	/* 正向迭代 */
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		// t.Log("key = ", string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}

	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	/* 反向迭代 */
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		// t.Log("key = ", string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	/* 指定 perfix  */
	iterOps2 := DefaultIteratorOptions
	iterOps2.Prefix = []byte("a")
	iter3 := db.NewIterator(iterOps2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}
