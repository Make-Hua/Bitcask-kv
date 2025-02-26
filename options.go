package bitcaskkv

import "os"

// 数据库配置项结构体
type Options struct {
	DirPath      string      /* 数据库的数据目录 */
	DataFileSize int64       /* activeFile 对应阈值大小 */
	SyncWrites   bool        /* 每次写数据是否持久化 */
	IndexType    IndexerType /* 内存索引类型 */
}

// 迭代器配置项结构体
type IteratorOptions struct {
	Prefix  []byte /* 遍历前缀为指定值的 Key, 默认 空 */
	Reverse bool   /* 是否反向遍历，false 是正向 */
}

// 原子写配置项结构体
type WriteBatchOptions struct {
	MaxBatchNum uint /* 一次批处理的最大处理数据条数 */
	SyncWrites  bool /* 提交时是否需要 Sync 持久化 */
}

type IndexerType = int8

const (
	BTree  IndexerType = iota + 1 /* BTree 索引 */
	ART                           /* ART 自适应基数树索引 */
	BPTree                        /* BPTree B+树索引 */
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    BPTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
