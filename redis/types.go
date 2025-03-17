package redis

import (
	bitcaskkv "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
	db *bitcaskkv.DB
}

// NewRedisDataStructure 初始化 Redis 数据结构服务
func NewRedisDataStructure(options bitcaskkv.Options) (*RedisDataStructure, error) {

	db, err := bitcaskkv.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

// ============================== string =====================================
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {

	if value == nil {
		return nil
	}

	// 编码 value ： type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String

	// buf[0] 存储 Type。从 1 开始存过期时间
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {

	// 调用存储引擎接口获取数据
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}
