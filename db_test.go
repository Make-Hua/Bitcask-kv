package bitcaskkv

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 销毁 DB 实例，同时销毁 DB 存储以及相关文件
func destroyDB(db *DB) error {
	if db != nil {
		if db.activeFile != nil {
			if err := db.activeFile.Close(); err != nil {
				return err
			}
		}
		// println(db.options.DirPath)
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// 销毁 DB 实例
func destroyDB1(db *DB) error {
	if db != nil {
		if db.activeFile != nil {
			if err := db.activeFile.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir

	db, err := Open(opts)
	defer assert.Nil(t, err)
	assert.NotNil(t, db)

	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_Put(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/* 1.正常 Put 一条数据 ---> OVER! */
	err = db.Put([]byte("1"), []byte("24"))
	assert.Nil(t, err)
	val1, err := db.Get([]byte("1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("24"), val1)

	/* 2.重复 Put 相同 key 数据 ---> OVER! */
	err = db.Put([]byte("1"), []byte("25"))
	assert.Nil(t, err)
	val2, err := db.Get([]byte("1"))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	assert.Equal(t, []byte("25"), val2)

	/* 3.Put key 为空 */
	err = db.Put(nil, []byte("25"))
	assert.NotNil(t, err)
	assert.Equal(t, ErrKeyIsEmpty, err)

	/* 4.Put key 为空 */
	err = db.Put([]byte("2"), nil)
	assert.Nil(t, err)
	val3, err := db.Get([]byte("2"))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	/* 5.写到数据文件进行新老转换 */
	// 模拟写入大量数据，触发数据文件新老转换
	// 这里假设每次写入的数据大小为 1KB，写入足够多的数据以达到文件大小限制
	dataSize := 1024
	for i := 0; i < int(opts.DataFileSize/int64(dataSize))+1; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, dataSize)
		err := db.Put(key, value)
		assert.Nil(t, err)
	}
	// 验证最后一次写入的数据是否能正常获取
	lastKey := []byte(fmt.Sprintf("key-%d", uint64(opts.DataFileSize/int64(dataSize))))
	lastValue, err := db.Get(lastKey)
	assert.Nil(t, err)
	assert.NotNil(t, lastValue)
	assert.Equal(t, dataSize, len(lastValue))

	/* 6.重启后前面数据都能拿到 */
	// 先销毁当前数据库实例
	err = destroyDB1(db)
	assert.Nil(t, err)

	// 重新打开数据库
	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 验证之前写入的数据是否能正常获取
	// 验证第一条数据
	val1AfterRestart, err := db.Get([]byte("1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1AfterRestart)
	assert.Equal(t, []byte("25"), val1AfterRestart)

	// 验证最后一条数据
	lastValueAfterRestart, err := db.Get(lastKey)
	assert.Nil(t, err)
	assert.NotNil(t, lastValueAfterRestart)
	assert.Equal(t, dataSize, len(lastValueAfterRestart))

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_Get(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/* 1.正常读取数据 */
	err = db.Put([]byte("01"), []byte("001"))
	assert.Nil(t, err)
	val1, err := db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("001"), val1)

	/* 2.读取一条不存在的数据 */
	_, err = db.Get([]byte("xxx"))
	assert.NotNil(t, err)
	assert.Equal(t, ErrKeyNotFound, err)

	/* 3.key/value 被重复 Put 后读取 */
	err = db.Put([]byte("01"), []byte("002"))
	assert.Nil(t, err)
	val2, err := db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("002"), val2)

	err = db.Put([]byte("01"), []byte("001"))
	assert.Nil(t, err)
	val3, err := db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("001"), val3)

	/* 4.key/value 被 Delete 后读取 */
	err = db.Put([]byte("02"), []byte("002"))
	assert.Nil(t, err)
	val4, err := db.Get([]byte("02"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("002"), val4)

	err = db.Delete([]byte("02"))
	assert.Nil(t, err)
	_, err = db.Get([]byte("02"))
	assert.NotNil(t, err)
	assert.Equal(t, ErrKeyNotFound, err)

	/* 5.转换为旧的数据文件后在 Get */
	dataSize := 1024
	for i := 0; i < int(opts.DataFileSize/int64(dataSize))+1; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, dataSize)
		err := db.Put(key, value)
		assert.Nil(t, err)
	}
	val5, err := db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("001"), val5)

	/* 6.重启后，之前数据均能拿到 */
	err = destroyDB1(db)
	assert.Nil(t, err)

	// 重新打开数据库
	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 验证之前写入的数据是否能正常获取
	// 验证第一条数据
	val1AfterRestart, err := db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1AfterRestart)
	assert.Equal(t, []byte("001"), val1AfterRestart)

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_Delete(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/* 1.删除一条存在的 key */
	err = db.Put([]byte("01"), []byte("001"))
	assert.Nil(t, err)
	err = db.Put([]byte("02"), []byte("002"))
	assert.Nil(t, err)
	err = db.Delete([]byte("02"))
	assert.Nil(t, err)

	/* 2.删除一条不存在的 key */
	err = db.Delete([]byte("03"))
	assert.Nil(t, err)

	/* 3.删除一条空的 key */
	err = db.Delete(nil)
	assert.NotNil(t, err)
	assert.Equal(t, ErrKeyIsEmpty, err)

	/* 4.key/value 被 Delete 后重新 Put */
	err = db.Put([]byte("02"), []byte("002"))
	assert.Nil(t, err)
	err = db.Put([]byte("03"), []byte("003"))
	assert.Nil(t, err)
	err = db.Delete([]byte("02"))
	assert.Nil(t, err)
	err = db.Delete([]byte("03"))
	assert.Nil(t, err)
	err = db.Put([]byte("02"), []byte("002"))
	assert.Nil(t, err)
	err = db.Put([]byte("03"), []byte("005"))
	assert.Nil(t, err)

	val1, err := db.Get([]byte("02"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("002"), val1)

	val1, err = db.Get([]byte("03"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("005"), val1)

	/* 5.重启后进行校验 */
	err = destroyDB1(db)
	assert.Nil(t, err)

	// 重新打开数据库
	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	val1, err = db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("001"), val1)

	err = db.Delete([]byte("001"))
	assert.Nil(t, err)
	err = db.Put([]byte("01"), []byte("001"))
	assert.Nil(t, err)

	val1, err = db.Get([]byte("01"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, []byte("001"), val1)

	/* 销毁创建的临时 DB 以及临时文件 */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}
