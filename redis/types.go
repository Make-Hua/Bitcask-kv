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

// ============================== Hash =====================================
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {

	// 查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, nil
	}

	// 构造 Hash 数据部分的 Key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	// 先查找对应数据是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcaskkv.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcaskkv.DefaultWriteBatchOptions)
	// 不存在则进行更新
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {

	// 先找到元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造 Hash 数据部分的 Key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {

	// 先找到元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造 Hash 数据部分的 Key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	// 查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcaskkv.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcaskkv.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// ============================== Set =====================================
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {

	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == bitcaskkv.ErrKeyNotFound {

		// 不存在则更新
		wb := rds.db.NewWriteBatch(bitcaskkv.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {

	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcaskkv.ErrKeyNotFound {
		return false, err
	}
	if err == bitcaskkv.ErrKeyNotFound {
		return false, nil
	}

	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {

	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcaskkv.ErrKeyNotFound {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(bitcaskkv.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {

	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcaskkv.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcaskkv.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)

		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initailListMark
			meta.tail = initailListMark
		}
	}

	return meta, nil
}
