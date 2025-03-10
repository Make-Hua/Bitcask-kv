package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File /* 系统文件描述符 */
}

// NewFileIOManager 初始化标准文件 IO
func NewFileIOManager(fileName string) (*FileIO, error) {

	// fileName：打开文件名称或者路径名
	// - os.O_CREATE：如果文件不存在，则创建该文件
	// - os.O_RDWR：以读写模式打开文件，允许对文件进行读取和写入操作
	// - os.O_APPEND：以追加模式打开文件，写入的数据会被追加到文件末尾
	// DataFilePerm：表示该文件的权限
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)

	// 错误处理
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// Read 从文件给定位置读取相应信息
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size 获取文件大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
