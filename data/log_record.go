package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

/* crc + type + keySize + valSize */
/*  4  +  1   +    5    +    5    = 15*/
const maxLogRecordHeaderSize = (4 + 1) + binary.MaxVarintLen32*2

// LogRecord 写入到数据文件的记录（数据文件中数据的写入是追加的）
type LogRecord struct {
	Key   []byte        /* 数据库存储的键 */
	Value []byte        /* 数据库存储的值 */
	Type  LogRecordType /* 该条信息对应的类型 */
}

// LogRecordPos 数据内存索引， 主要是描述磁盘上的数据
type LogRecordPos struct {
	Fid    uint32 /* Fid 文件标识，表示将数据存储到哪个文件中 */
	Offset int64  /* Offset 偏移量，表示将数据存储到对应文件的哪个位置（第几行） */
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        /* crc 校验值 */
	recordType LogRecordType /* 该条 LogRecord 对应的类型 */
	keySize    uint32        /* key 对应的长度 */
	valueSize  uint32        /* value 对应的长度 */
}

// TransactionRecord 暂存事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 结构进行编码，返回字节数组和长度
/*
| crc 校验值 | type 类型 |   key size   |  value size  |   key   |  value |
|    4字节   |  1 字节   | 变长（最大5） | 变长（最大5） |  变长   |  变长  |
*/
/* logRecordHeader --> []byte */
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {

	// 初始化 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// type 的存储
	header[4] = logRecord.Type

	var index = 5

	// 第 5 个字节后，第 6 个字节开始，存储的是 key 和 value 的长度
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// 计算出该条 LogRecord 的字节大小
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// 编码的字节数组
	encBytes := make([]byte, size)

	// 将 header 拷贝过来
	copy(encBytes[:index], header[:index])

	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 计算 crc32 的值
	crc := crc32.ChecksumIEEE(encBytes[4:])

	// LittleEndian 小端序存储
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// fmt.Printf("header length : %d, crc : %d\n", index, crc)

	return encBytes, int64(size)
}

// 对一条 LogRecord 信息的 Header 部分解码
/* []byte --> logRecordHeader */
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {

	// 如果连 crc 对应的 4 个字节都没有，则说明该条数据有问题
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	// 从字节数组取出对应的 key 和 value
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 注意 header []byte 不包括前 4 个字节（crc）
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {

	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
