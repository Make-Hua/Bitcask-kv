package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录（数据文件中数据的写入是追加的）
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引， 主要是描述磁盘上的数据
type LogRecordPos struct {
	Fid    uint32 /* Fid 文件标识，表示将数据存储到哪个文件中 */
	Offset int64  /* Offset 偏移量，表示将数据存储到对应文件的哪个位置（第几行） */
}

// EncodeLogRecord 对 LogRecord 结构进行编码，返回字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
