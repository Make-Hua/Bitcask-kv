package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initailListMark = math.MaxUint64 / 2
)

// 元数据
type metadata struct {
	dataType byte   /* 数据类型 */
	expire   int64  /* 过期时间 */
	version  int64  /* 版本号 */
	size     uint32 /* 数据大小 */

	head uint64 /* List 专有 */
	tail uint64 /* List 专有 */
}

// redis 类型 --》 存储引擎存储类型的编码
func (md *metadata) encode() []byte {

	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}

	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

// 存储引擎存储类型 -> redis 类型的解码
func decodeMetadata(buf []byte) *metadata {

	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte /*  */
	version int64
	filed   []byte
}

func (hk *hashInternalKey) encode() []byte {

	buf := make([]byte, len(hk.key)+len(hk.filed)+8)

	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// filed
	copy(buf[index:], hk.filed)

	return buf
}

type setInternalKey struct {
	key     []byte /*  */
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {

	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key     []byte /*  */
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {

	buf := make([]byte, len(lk.key)+8+8)

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}
