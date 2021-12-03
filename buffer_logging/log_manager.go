package buffer_logging

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/blastbao/fakedb/util"
	"github.com/kr/pretty"
	"io"
	"os"
	"sync"
	"time"
)

var logManagerLog = util.GetLog("logManager")

type LogManager struct {
	WALLock                sync.Mutex
	CheckPointWriter       *os.File
	WAL                    *os.File // File is needs to be on append | f_sync
	Lsn                    int64
	Lock                   sync.Mutex
	LogBuffer              chan *LogRecord
	Ctx                    context.Context
	FlushedLsn             int64
	LastCheckPointLsn      int64
	ActiveTransactionTable map[uint64]*TransactionTableEntry // 活跃事务表
	FlushDuration          time.Duration
}

// ReadLastCheckPoint
//
// Read checkpoint. The checkPoint point to the lastest lsn of begin checkpoint.
// The checkPoint file is written in WAL mode. and each checkPoint occupy 8 bytes.
// To make sure data is complete, we will truncate those checkpoint whose length is less than 8 bytes.
//
// 读取检查点，即最后一次 Begin CheckPoint 对应的 LSN 。
// 注意，检查点文件大小一定是 8B 的整数倍，如果不是，那么可能是最后一次写入发生错误，
// 那么应该忽略掉这部分残缺的数据。
func ReadLastCheckPoint(reader *os.File) (int64, error) {

	// 获取文件大小
	stat, err := reader.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()

	// 如果文件小于 8 字节，当作空文件
	if size < 8 {
		reader.Seek(0, io.SeekStart)
		return 0, nil
	}

	// 如果 size 不是 8 的整数倍，就忽略掉尾部的错误字节。
	checkPointAddr := size - size%8 - 8
	_, err = reader.Seek(checkPointAddr, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// 读取 8B
	bytes := make([]byte, 8)
	for i := 0; i < 8; {
		size, err := reader.Read(bytes[i:])
		if err != nil {
			return 0, err
		}
		i += size
	}

	// 解析 LSN
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

func NewLogManager(ctx context.Context, bufferSize int, checkPointFileName string, WAL string, flushDuration time.Duration) (*LogManager, error) {

	// 打开 CheckPoint 文件
	checkPointWriter, err := os.OpenFile(checkPointFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// 读取上个 CheckPoint 对应的 LSN
	lastCheckPointLsn, err := ReadLastCheckPoint(checkPointWriter)
	if err != nil {
		return nil, err
	}

	// 打开 wal 文件
	wal, err := os.OpenFile(WAL, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &LogManager{
		CheckPointWriter:       checkPointWriter,
		WAL:                    wal,
		Lsn:                    InvalidLsn,
		Lock:                   sync.Mutex{},
		LogBuffer:              make(chan *LogRecord, bufferSize),
		Ctx:                    ctx,
		LastCheckPointLsn:      lastCheckPointLsn,
		ActiveTransactionTable: map[uint64]*TransactionTableEntry{},
		FlushDuration:          flushDuration,
	}, nil
}

const InvalidLsn int64 = -1

// LogRecord 代表 wal 中的一条日志记录。
//
// Todo: maybe we need a crc here.
type LogRecord struct {
	LSN           int64         // 当前日志序号。
	PrevLsn       int64         // 指向同一事务的上一条日志序号。一个事务可能产生多条日志，这些日志通过 PrevLsn 串联起来，形成逻辑链表。
	UndoNextLsn   int64         // 指向当前日志对应的回滚前日志，沿着这个链逐个回滚到事务初始状态。
	TransactionId uint64        // 事务 ID
	PageId        int32         // 页 ID
	TP            LogRecordType // 日志类型
	ActionTP      ActionType    // 操作类型
	BeforeValue   []byte        // 旧值
	AfterValue    []byte        // 新值
	Done          chan bool     // 完成落盘的回调通知管道
	Force         bool          // 是否强制落盘
}

type ActionType byte

const (
	DelAction ActionType = iota // 删除
	SetAction                   // 设置
	AddAction                   // 添加
)

var ActionTypeNameMap = map[ActionType]string{
	DelAction: "del",
	SetAction: "set",
	AddAction: "add",
}

type LogRecordType byte

const (
	SetLogType             LogRecordType = iota
	CompensationLogType                  // 补偿日志
	TransBeginLogType                    // 事务开始
	TransCommitLogType                   // 事务提交
	TransEndLogType                      // 事务结束
	TransAbortLogType                    // 事务中止
	CheckPointBeginLogType               // CheckPoint 开始
	CheckPointEndLogType                 // CheckPoint 结束
	DummyLogType                         // 缓存日志刷盘
)

var LogRecordTypeNameMap = map[LogRecordType]string{
	SetLogType:             "set",
	CompensationLogType:    "compensation",
	TransBeginLogType:      "begin",
	TransCommitLogType:     "commit",
	TransEndLogType:        "end",
	TransAbortLogType:      "abort",
	CheckPointBeginLogType: "begin_checkpoint",
	CheckPointEndLogType:   "end_checkpoint",
}

type TransactionTableEntry struct {
	TransactionId uint64           // 事务 ID
	State         TransactionState // 事务状态
	Lsn           int64            // LSN
	UndoNextLsn   int64            // Undo LSN
}

type TransactionState byte

const (
	TransactionC TransactionState = iota // 提交
	TransactionU                         // 未决的
	TransactionP                         // 准备
	TransactionE                         // 结束
)

var TransactionStateNameMap = map[TransactionState]string{
	TransactionC: "commit",
	TransactionU: "undecided",
	TransactionP: "prepare",
	TransactionE: "end",
}

func (entry *TransactionTableEntry) Serialize() []byte {
	ret := make([]byte, 25)
	binary.BigEndian.PutUint64(ret, entry.TransactionId)            // 8B
	ret[8] = byte(entry.State)                                      // 1B
	binary.BigEndian.PutUint64(ret[9:], uint64(entry.Lsn))          // 8B
	binary.BigEndian.PutUint64(ret[17:], uint64(entry.UndoNextLsn)) // 8B
	return ret
}

func (entry *TransactionTableEntry) Deserialize(data []byte) error {
	if len(data) < 25 {
		return damagedPacket
	}
	entry.TransactionId = binary.BigEndian.Uint64(data)
	entry.State = TransactionState(data[8])
	entry.Lsn = int64(binary.BigEndian.Uint64(data[9:]))
	entry.UndoNextLsn = int64(binary.BigEndian.Uint64(data[17:]))
	return nil
}

func (entry *TransactionTableEntry) Len() int {
	return 25
}

func (entry *TransactionTableEntry) String() string {
	return fmt.Sprintf("[transId: %d, state: %s, lsn: %d, undoNextLsn: %d]",
		entry.TransactionId,
		TransactionStateNameMap[entry.State],
		entry.Lsn,
		entry.UndoNextLsn,
	)
}

// FixedLength of logRecord: 4 + 8 * 4 + 4 + 1 + 4 + before value bytes + 4 + after value bytes.
func (log *LogRecord) Serialize() []byte {
	data := make([]byte, log.Len())
	binary.BigEndian.PutUint32(data, uint32(log.Len()))
	binary.BigEndian.PutUint64(data[4:], uint64(log.LSN))
	binary.BigEndian.PutUint64(data[12:], uint64(log.PrevLsn))
	binary.BigEndian.PutUint64(data[20:], uint64(log.UndoNextLsn))
	binary.BigEndian.PutUint64(data[28:], log.TransactionId)
	binary.BigEndian.PutUint32(data[36:], uint32(log.PageId))
	data[40] = byte(log.TP)
	data[41] = byte(log.ActionTP)
	binary.BigEndian.PutUint32(data[42:], uint32(len(log.BeforeValue)))
	copy(data[46:], log.BeforeValue)
	binary.BigEndian.PutUint32(data[46+len(log.BeforeValue):], uint32(len(log.AfterValue)))
	copy(data[50+len(log.BeforeValue):], log.AfterValue)
	return data
}

func (log *LogRecord) Deserialize(data []byte) error {
	if len(data) < 50 {
		return damagedPacket
	}
	log.LSN = int64(binary.BigEndian.Uint64(data[4:]))
	log.PrevLsn = int64(binary.BigEndian.Uint64(data[12:]))
	log.UndoNextLsn = int64(binary.BigEndian.Uint64(data[20:]))
	log.TransactionId = binary.BigEndian.Uint64(data[28:])
	log.PageId = int32(binary.BigEndian.Uint32(data[36:]))
	log.TP = LogRecordType(data[40])
	log.ActionTP = ActionType(data[41])
	beforeValueLen := binary.BigEndian.Uint32(data[42:])
	if uint32(len(data)) < 50+beforeValueLen {
		return damagedPacket
	}
	log.BeforeValue = data[46 : beforeValueLen+46]
	afterValueLen := binary.BigEndian.Uint32(data[46+beforeValueLen:])
	if uint32(len(data)) < 50+beforeValueLen+afterValueLen {
		return damagedPacket
	}
	log.AfterValue = data[50+beforeValueLen : 50+beforeValueLen+afterValueLen]
	// TODO: support CRC
	return nil
}

func (log *LogRecord) Len() int {
	return 4 + 8*4 + 4 + 2 + 4 + len(log.BeforeValue) + 4 + len(log.AfterValue)
}

func (log *LogRecord) String() string {
	switch log.TP {
	case TransBeginLogType, TransCommitLogType, TransAbortLogType, TransEndLogType:
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, tranceId: %d, force: %t, log: %s, len: %d.",
			log.LSN, log.PrevLsn, log.UndoNextLsn, log.TransactionId, log.Force, LogRecordTypeNameMap[log.TP], log.Len())
	case CheckPointBeginLogType:
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, log: %s, len: %d.", log.LSN, log.PrevLsn,
			log.UndoNextLsn, LogRecordTypeNameMap[log.TP], log.Len())
	case CheckPointEndLogType:
		trans := pretty.Sprintf("%v", DeserializeTransactionTableEntry(log.BeforeValue))
		dirtyPages := pretty.Sprintf("%v", DeserializeDirtyPageRecord(log.AfterValue))
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, log: %s, trans: %s, dirtyPages: %s, len: %d.",
			log.LSN, log.PrevLsn, log.UndoNextLsn, LogRecordTypeNameMap[log.TP], trans, dirtyPages, log.Len())
	case SetLogType, CompensationLogType:
		pair := &Pair{}
		err := pair.Deserialize(log.BeforeValue)
		if err != nil {
			panic(err)
		}
		pair2 := &Pair{}
		err = pair2.Deserialize(log.AfterValue)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("log: lsn: %d, prevLsn: %d, undoNextLsn: %d, pageId: %d, tranceId: %d, "+
			"force: %t, action: %s, log: %s. before: [key: %s, value: %s], after: [key: %s, value: %s], len: %d.", log.LSN,
			log.PrevLsn, log.UndoNextLsn, log.PageId, log.TransactionId, log.Force,
			ActionTypeNameMap[log.ActionTP], LogRecordTypeNameMap[log.TP], string(pair.Key), string(pair.Value),
			string(pair2.Key), string(pair2.Value), log.Len())
	default:
		panic("wrong type")
	}
}

// Wait until the log l is flushed to WAL.
func (log *LogManager) WaitFlush(l *LogRecord) {
	<-l.Done
}

var LogFlushCapacity = 1024 * 16

func (log *LogManager) FlushPeriodly() {
	logManagerLog.InfoF("start flush log goroutine, lsn: %d", log.Lsn)
	defer logManagerLog.InfoF("end flush log goroutine")

	// 记录缓存，完成 flush 之后会通过 close(record.done) 通知上游。
	var pendingLog []*LogRecord
	// 数据缓存，用于缓存一批数据，批量写磁盘，减少 IO 。
	limit := LogFlushCapacity
	buf := make([]byte, LogFlushCapacity)
	i := 0

	var record *LogRecord
	flush := false
	for {
		record = nil

		select {
		// 退出信号
		case <-log.Ctx.Done():
			return
		// 读管道
		case record = <-log.LogBuffer:
		// 定时
		case <-time.After(log.FlushDuration):
			flush = true
		}

		// 收到空数据，退出
		if record == nil && !flush {
			return
		}

		// 定时 flush
		if flush {
			// flush
			log.Write(buf[:i], i)
			// 通知写入完成
			log.NoticeDone(pendingLog)
			// 重置变量
			pendingLog = nil
			i = 0
			flush = false
			continue
		}

		// 如果日志指定强制落盘，且其类型为 DummyLog，则将缓存日志刷盘。
		if record.Force && record.TP == DummyLogType {
			// logManagerLog.InfoF("data len: %d", i)
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			pendingLog = nil
			i = 0
			continue
		}

		data := record.Serialize()

		// logManagerLog.InfoF("data len: %d", len(data))
		if len(data) > (limit - i) {
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			pendingLog = nil
			i = 0
		}

		// 大日志，直接落盘
		if len(data) > limit {
			// Flush directly.
			log.Write(data, len(data))
			log.NoticeDone([]*LogRecord{record})
			continue
		}

		// 缓存记录
		pendingLog = append(pendingLog, record)
		// 缓存数据
		copy(buf[i:], data)
		i += len(data)

		// 如果日志指定强制落盘，就直接 flush
		if record.Force {
			// Force writing to logs.
			log.Write(buf[:i], i)
			log.NoticeDone(pendingLog)
			i = 0
			pendingLog = nil
		}

	}
}

// NoticeDone 通知完成日志的刷盘
func (log *LogManager) NoticeDone(logs []*LogRecord) {
	for _, l := range logs {
		logManagerLog.InfoF("write a log: %s", l)
		close(l.Done)
	}
}

// 将 data 写入到 wal 文件中，更新 lsn
func (log *LogManager) Write(data []byte, len int) {
	if len == 0 {
		return
	}
	log.WALLock.Lock()
	// 循环写入 WAL 文件
	for i := 0; i < len; {
		size, err := log.WAL.Write(data[i:])
		if err != nil {
			panic(err)
		}
		i += size
	}
	// 执行 WAL 文件落盘
	if err := log.WAL.Sync(); err != nil {
		panic(err)
	}
	// 解锁
	log.WALLock.Unlock()

	// 更新已刷盘的日志序号
	log.Lock.Lock()
	log.FlushedLsn += int64(len)
	log.Lock.Unlock()
}

// 是否是事务日志，也即非检查点、非 dummy 的 log
func IsTransLog(l *LogRecord) bool {
	return l.TP != CheckPointBeginLogType && l.TP != CheckPointEndLogType && l.TP != DummyLogType
}

func (log *LogManager) Append(l *LogRecord) *LogRecord {

	// 加锁，确保写 wal 是串行的
	log.Lock.Lock()

	// 设置 LSN
	l.LSN = log.Lsn

	// 更新 LSN
	log.Lsn += int64(l.Len())

	// 如果是事务日志，就更新事务状态
	if IsTransLog(l) {
		log.updateActiveTransactionTable(l)
	}

	log.Lock.Unlock()

	// 写入 buffer ，异步落盘
	l.Done = make(chan bool)
	log.LogBuffer <- l

	return l
}

// Todo: need to check further.
//
// 根据日志类型，判断事务状态
func transActionStateFromLogRecord(l *LogRecord) TransactionState {
	switch l.TP {
	case TransBeginLogType:
		return TransactionP
	case TransCommitLogType:
		return TransactionC
	case TransEndLogType:
		return TransactionE
	default:
		return TransactionU
	}
}

// CheckPoint 定时创建 checkpoint
func (log *LogManager) CheckPoint(bufManager *BufferManager) {
	logManagerLog.InfoF("start checkpoint goroutine")
	defer logManagerLog.InfoF("end checkpoint goroutine")
	for {

		select {
		case <-time.After(log.FlushDuration):
		case <-log.Ctx.Done():
			return
		}

		// 创建检查点
		err := log.DoCheckPoint(bufManager)
		if err != nil {
			panic(err)
		}
	}
}


// DoCheckPoint
//
// 创建检查点
// 	1. 写入 "begin_checkpoint" 类型日志，得到该日志的 LSN
//	2. 获取当前活跃事务列表，包含事务的 <TxID, 状态, LSN, UndoNextLsn> 信息
//  3. 获取当前脏页列表，包含页面的 <PageId, RevLSN>
//  4. 写入 "end_checkpoint" 类型日志，其包含活跃事务和脏页的信息
//  5. 调用 flush 将日志落盘
//  6. 将 "begin_checkpoint" 类型日志的 LSN 写入到独立的 CheckPoint 文件记录下来
//
func (log *LogManager) DoCheckPoint(bufManager *BufferManager) error {
	// 创建 "begin_checkpoint" 日志
	beginCheckPointRecord := &LogRecord{
		TP: CheckPointBeginLogType,
	}

	// 写入 wal 文件
	log.Append(beginCheckPointRecord)

	// 复制活跃事务表
	transTable := log.CloneTransactionTable()

	// 复制脏页列表
	dirtyTable := bufManager.DirtyPageRecordTable()

	// 脏页数 4B + N 个脏页 * 12B
	dirtyTableBytes := make([]byte, len(dirtyTable)*12+4)
	binary.BigEndian.PutUint32(dirtyTableBytes, uint32(len(dirtyTable))) // 脏页数 4B
	for i, entry := range dirtyTable {                                   // N 个脏页，每个 12B
		copy(dirtyTableBytes[4+i*12:], entry.Serialize())
	}

	// 事务数 4B + N 个事务 25B
	transTableBytes := make([]byte, len(transTable)*25+4)
	binary.BigEndian.PutUint32(transTableBytes, uint32(len(transTable))) // 活跃事务数目，4B
	for i, entry := range transTable {
		copy(transTableBytes[4+i*25:], entry.Serialize()) // 每个事务的
	}

	// check point finish log
	//
	// 创建 "end_checkpoint" 日志
	finishCheckPointLogRecord := &LogRecord{
		TP:          CheckPointEndLogType,	// 日志类型
		BeforeValue: transTableBytes,		//
		AfterValue:  dirtyTableBytes,		//
		Force:       true,					// 强制刷盘
	}

	// 写入 wal 文件
	log.Append(finishCheckPointLogRecord)

	// 等待完成落盘
	log.WaitFlush(finishCheckPointLogRecord)

	logManagerLog.InfoF("do a checkpoint: ")

	// 完成 CheckPoint 之后，把 8 Bytes 的 checkPointLsn 写入到 CheckPoint 日志文件
	return log.WriteBeginCheckPoint(beginCheckPointRecord.LSN)
}


//
type CheckPointAddr struct {
	LSN int64 // Todo: add crc
}

// WriteBeginCheckPoint
//
// 完成 CheckPoint 之后，把 8 Bytes 的 checkPointLsn 写入到 CheckPoint 日志文件
func (log *LogManager) WriteBeginCheckPoint(checkPointLsn int64) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(checkPointLsn))
	// 循环写入 8 个 Bytes 的 LSN
	for i := 0; i < 8; {
		size, err := log.CheckPointWriter.Write(data[i:])
		if err != nil {
			return err
		}
		i += size
	}
	return nil
}


// 复制活跃事务信息
func (log *LogManager) CloneTransactionTable() (ret []TransactionTableEntry) {
	log.Lock.Lock()
	defer log.Lock.Unlock()

	// 遍历活跃事务
	for txnId, v := range log.ActiveTransactionTable {
		ret = append(ret, TransactionTableEntry{
			TransactionId: txnId,         // 事务 ID
			State:         v.State,       // 事务状态
			Lsn:           v.Lsn,         // 事务 LSN
			UndoNextLsn:   v.UndoNextLsn, // 事务 UndoLsn
		})
	}
	return ret
}

func (log *LogManager) FlushLogs() {
	l := &LogRecord{
		TP:    DummyLogType,	// 空类型，无意义
		Force: true,			// 强制落盘
	}
	log.LogBuffer <- l			// 触发落盘
}

// Thread-safe
//
//
func (log *LogManager) GetBeginCheckPointLSN() int64 {
	return log.LastCheckPointLsn
}

func (log *LogManager) GetFlushedLsn() int64 {
	log.Lock.Lock()
	defer log.Lock.Unlock()
	return log.FlushedLsn
}

// Only used during recovery. Should be single goroutine. no lock protection needed.
//
//
func (log *LogManager) AppendRecoveryLog(l *LogRecord) {
	// 取当前 log.LSN
	offset := log.Lsn
	// 把当前 log.LSN 赋值给 l
	l.LSN = log.Lsn
	// 增加 log.LSN
	log.Lsn += int64(l.Len())

	// 如果是事务日志，需要更新事务状态
	if IsTransLog(l) {
		log.updateActiveTransactionTable(l)
	}

	l.Done = make(chan bool)

	// 定位到 WAL 文件头
	_, err := log.WAL.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}

	// 将 l 写入 WAL 文件
	data := l.Serialize()
	for i := 0; i < l.Len(); {
		size, err := log.WAL.Write(data[i:])
		if err != nil {
			panic(err)
		}
		i += size
	}

}

func (log *LogManager) updateActiveTransactionTable(l *LogRecord) {
	// 查询活跃事务
	old, ok := log.ActiveTransactionTable[l.TransactionId]

	// 如果不存在，则创建
	if !ok {
		old = &TransactionTableEntry{
			TransactionId: l.TransactionId,						// 事务 ID
			State:         transActionStateFromLogRecord(l),	// 事务状态
			Lsn:           l.LSN,								// 事务关联的最近一条 WAL 日志序号
			UndoNextLsn:   l.LSN,								//
		}
		log.ActiveTransactionTable[l.TransactionId] = old
	// 如果存在，就更新事务状态
	} else {
		old.State = transActionStateFromLogRecord(l)
		old.Lsn, old.UndoNextLsn = l.LSN, l.LSN
	}

	logManagerLog.InfoF("update trans state: id: %d, now: %s", old.TransactionId, TransactionStateNameMap[old.State])

	// 如果事务状态为 Committed 或者 End ，就从活跃事务表中移除
	if old.State == TransactionC || old.State == TransactionE {
		delete(log.ActiveTransactionTable, old.TransactionId)
	}
}

func (log *LogManager) LogIterator(lsn int64) *LogIterator {
	return &LogIterator{
		LSN:     lsn,
		WAL:     log.WAL,
		NextLog: nil,
	}
}

// ReadLog 从 wal 文件的 offset 偏移处读取一个 LogRecord 。
func (log *LogManager) ReadLog(offset int64) (*LogRecord, error) {
	l, err := ReadLog(log.WAL, offset)
	if err != nil {
		return nil, err
	}
	return l, nil
}


// LogIterator WAL 日志迭代器
type LogIterator struct {
	LSN     int64		// 日志偏移量
	WAL     *os.File	// 日志文件
	NextLog *LogRecord	// 当前日志记录
}

// ReadLog 从 wal 文件的 offset 处读取一个 LogRecord 。
func ReadLog(wal *os.File, offset int64) (*LogRecord, error) {

	// 定位到 Start 偏移处
	_, err := wal.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// 读取 4B 的长度
	bytes := make([]byte, 4)
	for i := 0; i < 4; {
		size, err := wal.Read(bytes[i:])
		if err != nil {
			return nil, err
		}
		i += size
	}

	// 解析出 data 的长度
	len := binary.BigEndian.Uint32(bytes) - 4

	// 读取 data
	data := make([]byte, len)
	for i := 0; i < int(len); {
		size, err := wal.Read(data[i:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			logManagerLog.InfoF("read a wrong length log")
			break
		}
		i += size
	}

	// 反序列化得到 LogRecord 并返回
	data = append(bytes, data...)
	ret := &LogRecord{}
	err = ret.Deserialize(data)
	return ret, err
}

func (ite *LogIterator) HasNext() bool {
	var err error
	if ite.NextLog == nil {
		// 读取
		ite.NextLog, err = ReadLog(ite.WAL, ite.LSN)
		if err != nil {
			return false
		}

		if ite.NextLog != nil {
			ite.LSN += int64(ite.NextLog.Len())
		}
	}
	return ite.NextLog != nil
}

func (ite *LogIterator) Next() *LogRecord {
	ret := ite.NextLog
	ite.NextLog = nil
	return ret
}

type Pair struct {
	Key   []byte
	Value []byte
}

// Serialize
// pair = len(key) + key + len(value) + value
func (pair *Pair) Serialize() []byte {
	data := make([]byte, pair.Len())
	// len(key)
	binary.BigEndian.PutUint32(data, uint32(len(pair.Key)))
	// key
	copy(data[4:], pair.Key)
	// len(value)
	binary.BigEndian.PutUint32(data[4+len(pair.Key):], uint32(len(pair.Value)))
	// value
	copy(data[8+len(pair.Key):], pair.Value)
	return data
}

// Deserialize
func (pair *Pair) Deserialize(data []byte) error {
	// check
	if len(data) < 8 {
		return damagedPacket
	}
	// len(key)
	keyLen := binary.BigEndian.Uint32(data)
	if uint32(len(data)) < keyLen+8 {
		return damagedPacket
	}
	// key
	pair.Key = data[4 : 4+keyLen]
	// len(value)
	valueLen := binary.BigEndian.Uint32(data[4+keyLen:])
	if uint32(len(data)) < keyLen+valueLen+8 {
		return damagedPacket
	}
	// value
	pair.Value = data[8+keyLen : 8+keyLen+valueLen]
	return nil
}

// Len
//
// pair = len(key) + key + len(value) + value
func (pair *Pair) Len() int {
	return 8 + len(pair.Key) + len(pair.Value)
}
