package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// 数据文件结构体
type DataFile struct {
	FileId    uint32        /* 文件对应 id */
	WriteOff  int64         /* 文件写入对应偏移量 offset */
	IoManager fio.IOManager /* io 读写管理 */
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 根据便宜量读取文件对应数据
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// Writer 数据文件写入方法
func (df *DataFile) Write(buf []byte) error {

	return nil
}

// Sync 数据文件持久化到磁盘
func (df *DataFile) Sync() error {
	return nil
}
