package bitcaskkv

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options                   /* 配置项相关 */
	mu         *sync.RWMutex             /* 读写锁 */
	activeFile *data.DataFile            /* 当前活跃文件（读写） */
	olderFiles map[uint32]*data.DataFile /* 当前就文件（只读） */
	index      index.Indexer             /* 内存索引 */
	seqNo      uint64                    /* 事务序列号 */
	isMerging  bool                      /* 表示当前 db 是否在进行 merge */

	fileIds []int /* 文件 id （方便复用，禁止其余地方使用） */
}

// 启动存储引擎实例的方法
func Open(options Options) (*DB, error) {

	// 对用户传入的配置项进行校\验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，需要创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例数据
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndex(options.IndexType),
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭数据库活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 持久化数据库文件
func (db *DB) Sync() error {

	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 数据存储引擎对外提供的操作方法，以追加的方式将数据写入活跃文件（key 不能为空）
func (db *DB) Put(key []byte, value []byte) error {

	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到活跃的数据文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 追加成功则将信息更新到内存索引中
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {

	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord,标识该 key 已经被删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// 写入该条删除标识数据
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 从索引中删除对应 key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 数据存储引擎对外提供的操作方法，根据对应 key 读取数据（key 不能为空）
func (db *DB) Get(key []byte) ([]byte, error) {

	// 加锁
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断 key 是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	/* 内存索引 -> 数据文件 -> 根据 offset 获得数据 */

	// 先从内存中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)

	// 如果 key 无对应索引信息，则 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库中所有的 Key
func (db *DB) ListKeys() [][]byte {

	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())

	// 通过迭代器遍历 BTree 索引树，然后添加到 []byte 数组
	var idx int = 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并且执行用户指定操作(func 返回 false 中止遍历)
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {

		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到相应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 如果数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNoFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 如果
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecord 向活跃文件追加数据
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 向活跃文件追加数据
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	/* 文件写入前的操作 */

	// 判断当前活跃数据文件是否存在（数据库在从未写入文件时是空的）
	// 如果空则对文件进行初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果当前新的数据文件加上现在写入数据已经大于阈值，
	// 则将新文件变老，同时创建新的数据文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {

		// 先持久化数据文件，保证已有数据持久化到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件加入老的数据文件组中
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	/* 开始实际写入 */

	// 将数据写入文件
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置信息确定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构建内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}

	return pos, nil
}

// 设置当前活跃文件
// 需要加锁
func (db *DB) setActiveDataFile() error {

	var initialFileId uint32 = 0

	// 文件 id 自增
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	// 设置新的数据文件
	db.activeFile = dataFile

	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {

	// 调用 os.ReadDir 函数来读取指定目录下的所有文件和子目录信息
	// 函数会返回一个包含目录项信息的切片
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {

		// 检查当前目录项的名称是否以 .data 结尾
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {

			// Split 函数按 "." 对文件名进行分割，得到一个字符串切片
			// 例如 “001.data” 经过调用得到 a []string, a[0] = "001", a[1] = "data"
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])

			// 数据目录有可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件 id 打开对应的数据文件
	for i, fid := range fileIds {

		// 打开文件 id 对应文件
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// 最后一个文件是 活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles 从数据文件中加载内存索引
func (db *DB) loadIndexFromDataFiles() error {

	// 如果数据库为空，则直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看该文件是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updataIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {

		// 如果类型为删除，则从内存索引中删除
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有文件 id，处理文件中的记录
	for i, fid := range db.fileIds {

		// 获取对应数据文件
		var fileId = uint32(fid)
		var dataFile *data.DataFile

		// 如果比最近未参加 merge 的文件 id 小， 则说明已经从 hint 文件加载
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 循环处理，将数据文件内容加入内存索引
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 解析 Key
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {

				// 非事务操作，直接更新内存索引
				updataIndex(realKey, logRecord.Type, logRecordPos)
			} else {

				// 事务完成，对应
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updataIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset
			offset += size
		}

		// 如果当前文件是活跃文件，则需要更新文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo

	return nil
}

// checkOptions 检查配置项是否合理
func checkOptions(options Options) error {

	// 如果文件为空
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	// 如果大小不对
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}
