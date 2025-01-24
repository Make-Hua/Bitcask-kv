package data

// LogRecordPos 数据内存索引， 主要是描述磁盘上的数据
type LogRecordPos struct {
	Fid    uint32 /* Fid 文件标识，表示将数据存储到哪个文件中 */
	Offset int64  /* Offset 偏移量，表示将数据存储到对应文件的哪个位置（第几行） */
}
