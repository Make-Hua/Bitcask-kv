package redis

import (
	bitcaskkv "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func destroyDB(db *bitcaskkv.DB) error {
	if db != nil {
		if err := db.Close(); err != nil {
			panic(err)
		}
		// println(db.options.DirPath)
		err := os.RemoveAll(db.GetDirPath())
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func TestRedisDataStructure_Get(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestValue(128))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.GetTestValue(128))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(333))
	assert.Equal(t, bitcaskkv.ErrKeyNotFound, err)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(33))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestValue(128))
	assert.Nil(t, err)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcaskkv.ErrKeyNotFound, err)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_HGet(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-Hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	v1 := utils.GetTestValue(128)
	v2 := utils.GetTestValue(128)
	v3 := utils.GetTestValue(128)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v2)
	assert.False(t, ok2)
	assert.Nil(t, err)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field3"), v3)
	assert.True(t, ok3)
	assert.Nil(t, err)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Equal(t, val1, v2)
	assert.Nil(t, err)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Equal(t, val2, v3)
	assert.Nil(t, err)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Nil(t, val3)
	assert.Equal(t, err, bitcaskkv.ErrKeyNotFound)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-Hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	v1 := utils.GetTestValue(128)
	v2 := utils.GetTestValue(128)
	v3 := utils.GetTestValue(128)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.True(t, ok2)
	assert.Nil(t, err)
	ok3, err := rds.HSet(utils.GetTestKey(2), []byte("field3"), v3)
	assert.True(t, ok3)
	assert.Nil(t, err)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
	del3, err := rds.HDel(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.True(t, del3)
	del4, err := rds.HDel(utils.GetTestKey(2), []byte("field3"))
	assert.Nil(t, err)
	assert.True(t, del4)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SIsMember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-bot-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_SRem(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SRem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_List(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-List")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val2", string(val))
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val1", string(val))
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val1", string(val))

	err = destroyDB(rds.db)
	assert.Nil(t, err)
}

func TestRedisDataStructure_ZScore(t *testing.T) {

	opts := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-List")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 99, []byte("val2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(99), score)

}
