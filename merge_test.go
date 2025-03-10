package bitcaskkv

import (
	"bitcask-go/utils"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 无数据运行
func TestDB_Merge1(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer func() {
		/* 销毁创建的临时 DB 以及临时文件 */
		if err := destroyDB(db); err != nil {
			assert.Nil(t, err)
		}
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)

}

// 全部都是有效数据运行
func TestDB_Merge2(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db2); err != nil {
		assert.Nil(t, err)
	}
}

// 有失效数据和被重复 Put 数据
func TestDB_Merge3(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, val, []byte("new value in merge"))
	}

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db2); err != nil {
		assert.Nil(t, err)
	}
}

// 全都是失效数据
func TestDB_Merge4(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db2); err != nil {
		assert.Nil(t, err)
	}
}

// merge 过程中有新的数据写入或者删除（merge 与 maset db 的并发情况）
func TestDB_Merge5(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(t, err)
	}

	// 删除 50000 条数据并新增一条(测试并发情况)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
			assert.Nil(t, err)
		}
	}()

	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db2); err != nil {
		assert.Nil(t, err)
	}
}
