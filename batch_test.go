package bitcaskkv

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewWriteBatch(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-NewWriteBatch")
	opts.DirPath = dir
	// defer func() {
	// 	if err := os.RemoveAll(dir); err != nil {
	// 		t.Errorf("Failed to remove temporary directory: %v", err)
	// 	}
	// }()
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/* 1.原子写 写入数据后不提交场景 */
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, ErrKeyNotFound, err)

	/* 2.原子写正常提交数据 */
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	/* 3.删除有效数据 */
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val2)
	assert.NotNil(t, err)

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_NewWriteBatch_down(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-NewWriteBatch")
	opts.DirPath = dir
	// defer func() {
	// 	if err := os.RemoveAll(dir); err != nil {
	// 		t.Errorf("Failed to remove temporary directory: %v", err)
	// 	}
	// }()
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	// t.Log(opts.DirPath)
	db, err = Open(opts)
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db.seqNo)

	/* 销毁创建的临时 DB */
	if err := destroyDB(db); err != nil {
		assert.Nil(t, err)
	}
}

func TestDB_NewWriteBatch3(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-NewWriteBatch3v")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// keys := db.ListKeys()
	// assert.Equal(t, 0, len(keys))

	// wbOpts := DefaultWriteBatchOptions
	// wbOpts.MaxBatchNum = 1000000

	// wb := db.NewWriteBatch(wbOpts)
	// for i := 0; i < 500000; i++ {
	// 	err = wb.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
	// 	assert.Nil(t, err)
	// }
	// err = wb.Commit()
	// assert.Nil(t, err)

	/* 销毁创建的临时 DB */
	// if err := destroyDB(db); err != nil {
	// 	assert.Nil(t, err)
	// }
}
