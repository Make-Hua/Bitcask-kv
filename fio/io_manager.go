package fio

const DateFilePerm = 0644

// IOManager 抽象 IO 管理器接口，可以接入不同 IO 类型
// 标准文件 IO
type IOManager interface {

	// Read 从文件给定位置读取相应信息
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// 初始化 IDManager （目前仅支持标准 FileIO）
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
