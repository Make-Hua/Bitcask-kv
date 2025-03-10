package fio

const DataFilePerm = 0644

// IOManager 的枚举类型
type FileIOType = byte

const (
	StandardFIO FileIOType = iota /* 标准文件 IO */
	MemoryMap                     /* MMap 内存文件映射 */
)

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

// 初始化 标准IO IDManager
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {

	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
