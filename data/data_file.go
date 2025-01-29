package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const DataFileNameSuffix = ".data"

// 数据文件结构体
type DataFile struct {
	FileId    uint32        /* 文件对应 id */
	WriteOff  int64         /* 文件写入对应偏移量 offset */
	IoManager fio.IOManager /* io 读写管理 */
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

	// 完整的数据文件名称
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据 offset 偏移量读取文件中的 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取最大 header 长度已经超过了文件的长度。则只需要读取到文件末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 先读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 获取头部信息
	header, headerSize := decodeLogRecordHeader(headerBuf)

	// 当读取到文件末尾，则直接返回
	if (header == nil) || (header.crc == 0 && header.keySize == 0 && header.valueSize == 0) {
		return nil, 0, io.EOF
	}

	// 取出对应 keySize 和 valSize
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	// 读取该条 LogRecord 实际存储的 key/val
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 解除 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据 crc 是否正确
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// Writer 数据文件写入方法
func (df *DataFile) Write(buf []byte) error {

	// 将数据写入文件
	nBytes, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}

	// 更新偏移量
	df.WriteOff += int64(nBytes)

	return nil
}

// Sync 数据文件持久化到磁盘
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close 关闭数据文件
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// 从指定 offset 位置读取 n 个字节的数据
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {

	// 调用 Read 读取数据
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
