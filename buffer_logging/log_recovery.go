package buffer_logging

import (
	"encoding/binary"
	"github.com/blastbao/fakedb/util"
)

var recoveryLog = util.GetLog("recovery")

func Recovery(logManager *LogManager, bufManager *BufferManager) (nextUsefulTransactionId uint64) {
	activeTransActionTable, dirtyPageRecordsTable := analysisForRecovery(logManager)

	// 定时将脏页刷盘
	go bufManager.FlushDirtyPagesRegularly(logManager)

	// We use the maximum transactionId from the current trans table as the next transId.
	// 获取下一个可用的事务 ID
	nextUsefulTransactionId = NextUsefulTransId(activeTransActionTable)

	//
	err := redoForRecovery(logManager, bufManager, dirtyPageRecordsTable)
	if err != nil {
		panic(err)
	}

	err = undoForRecovery(logManager, bufManager, activeTransActionTable)
	if err != nil {
		panic(err)
	}

	cleanFinishTrans(logManager, activeTransActionTable)

	err = logManager.WAL.Sync()	// Wal 刷盘
	if err != nil {
		panic(err)
	}
	go logManager.FlushPeriodly()			// 异步日志落盘
	go logManager.CheckPoint(bufManager) 	// 定时创建 checkpoint
	return
}

// 获取下一个可用的事务 ID
func NextUsefulTransId(activeTransActionTable map[uint64]*TransactionTableEntry) uint64 {
	nextUsefulTransactionId := uint64(0)
	for txnId, _ := range activeTransActionTable {
		if txnId > nextUsefulTransactionId {
			nextUsefulTransactionId = txnId + 1
		}
	}
	return nextUsefulTransactionId
}

func cleanFinishTrans(logManager *LogManager, activeTransActionTable map[uint64]*TransactionTableEntry) {

	for txnId, txn := range activeTransActionTable {

		if txn.State == TransactionC || txn.State == TransactionP {
			log := &LogRecord{
				PrevLsn:       txn.Lsn,
				TransactionId: txn.TransactionId,
				TP:            TransEndLogType,
			}
			logManager.AppendRecoveryLog(log)
			delete(activeTransActionTable, txnId)
		}

		if txn.State == TransactionE {
			delete(activeTransActionTable, txnId)
		}

		if txn.State == TransactionU && txn.UndoNextLsn == InvalidLsn {
			log := &LogRecord{
				PrevLsn:       txn.Lsn,
				TransactionId: txn.TransactionId,
				TP:            TransEndLogType,
			}
			logManager.AppendRecoveryLog(log)
			delete(activeTransActionTable, txnId)
		}

	}
}

func DeserializeTransactionTableEntry(data []byte) []*TransactionTableEntry {
	transTableLen := binary.BigEndian.Uint32(data)
	transTable := make([]*TransactionTableEntry, transTableLen)
	for i := uint32(0); i < transTableLen; i++ {
		transTable[i] = &TransactionTableEntry{}
		err := transTable[i].Deserialize(data[4+25*i:])
		if err != nil {
			panic(err)
		}
	}
	return transTable
}

func DeserializeDirtyPageRecord(data []byte) []*DirtyPageRecord {
	dirtyTableLen := binary.BigEndian.Uint32(data)
	dirtyTable := make([]*DirtyPageRecord, dirtyTableLen)
	for i := uint32(0); i < dirtyTableLen; i++ {
		dirtyTable[i] = &DirtyPageRecord{}
		err := dirtyTable[i].Deserialize(data[4+12*i:])
		if err != nil {
			panic(err)
		}
	}
	return dirtyTable
}

func handleCheckPointLogDuringRecovery(checkPointEndLog *LogRecord, activeTransActionTable map[uint64]*TransactionTableEntry,
	dirtyPageTables map[int32]*DirtyPageRecord) {
	transTable := DeserializeTransactionTableEntry(checkPointEndLog.BeforeValue)
	dirtyTable := DeserializeDirtyPageRecord(checkPointEndLog.AfterValue)

	for _, entry := range transTable {
		_, ok := activeTransActionTable[entry.TransactionId]
		if ok {
			continue
		}
		activeTransActionTable[entry.TransactionId] = &TransactionTableEntry{
			TransactionId: entry.TransactionId,
			State:         entry.State,
			Lsn:           entry.Lsn,
			UndoNextLsn:   entry.UndoNextLsn,
		}
	}

	for _, entry := range dirtyTable {
		_, ok := dirtyPageTables[entry.PageId]
		if !ok {
			dirtyPageTables[entry.PageId] = &DirtyPageRecord{PageId: entry.PageId, RevLSN: entry.RevLSN}
			continue
		}
		dirtyPageTables[entry.PageId].RevLSN = entry.RevLSN // entry.RevLSN must be less than the lsn in dirty page table.
	}
}

func analysisForRecovery(logManager *LogManager) (
	activeTransActionTable map[uint64]*TransactionTableEntry,
	dirtyPageTables map[int32]*DirtyPageRecord,
) {

	activeTransActionTable = map[uint64]*TransactionTableEntry{}
	dirtyPageTables = map[int32]*DirtyPageRecord{}

	// 读取最后一个检查点
	lsn := logManager.GetBeginCheckPointLSN()

	// 读取该检查点之后的 LogRecord
	logIte := logManager.LogIterator(lsn)

	// 循环读取 LogRecord
	for logIte.HasNext() {

		// 取当前 LogRecord
		log := logIte.Next()
		recoveryLog.InfoF("analysis log: %s", log)

		// ???
		_, ok := activeTransActionTable[log.TransactionId]
		if IsTransLog(log) && !ok {
			activeTransActionTable[log.TransactionId] = &TransactionTableEntry{
				TransactionId: log.TransactionId,
				State:         TransactionU,
				Lsn:           log.LSN,
				UndoNextLsn:   log.PrevLsn,
			}
		}

		// 检查类型
		tp := log.TP
		switch tp {
		case SetLogType:
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
			activeTransActionTable[log.TransactionId].UndoNextLsn = log.LSN
			_, ok := dirtyPageTables[log.PageId]
			if !ok {
				dirtyPageTables[log.PageId] = &DirtyPageRecord{PageId: log.PageId, RevLSN: log.LSN}
			}
			activeTransActionTable[log.TransactionId].State = TransactionU
		case CompensationLogType:
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
			activeTransActionTable[log.TransactionId].UndoNextLsn = log.UndoNextLsn
			_, ok := dirtyPageTables[log.PageId]
			if !ok {
				dirtyPageTables[log.PageId] = &DirtyPageRecord{PageId: log.PageId, RevLSN: log.LSN}
			}
		case TransBeginLogType, TransAbortLogType:
			if tp == TransBeginLogType {
				activeTransActionTable[log.TransactionId].State = TransactionP
			} else {
				activeTransActionTable[log.TransactionId].State = TransactionU
			}
			activeTransActionTable[log.TransactionId].Lsn = log.LSN
		case TransCommitLogType:
			activeTransActionTable[log.TransactionId].State = TransactionC
		case TransEndLogType:
			delete(activeTransActionTable, log.TransactionId)
		case CheckPointBeginLogType:
		case CheckPointEndLogType:
			handleCheckPointLogDuringRecovery(log, activeTransActionTable, dirtyPageTables)
		}
		lsn += int64(log.Len())
	}

	// Note: in case packet is broken, we redirect the lsn and flushLsn here.
	logManager.Lsn = lsn
	logManager.FlushedLsn = lsn
	for _, entry := range activeTransActionTable {
		if entry.State == TransactionU && entry.UndoNextLsn == InvalidLsn {
			entry.State = TransactionC
		}
	}

	return
}

func redoForRecovery(logManager *LogManager, bufManager *BufferManager, dirtyPageTable map[int32]*DirtyPageRecord) error {


	recLsn := int64(-1)
	for _, record := range dirtyPageTable {
		if recLsn == -1 {
			recLsn = record.RevLSN
			continue
		}
		if recLsn > record.RevLSN {
			recLsn = record.RevLSN
		}
	}


	logIte := logManager.LogIterator(recLsn)
	for logIte.HasNext() {
		log := logIte.Next()

		if log.TP != SetLogType && log.TP != CompensationLogType {
			continue
		}

		_, ok := dirtyPageTable[log.PageId]
		if !ok || log.LSN < dirtyPageTable[log.PageId].RevLSN {
			continue
		}

		page, err := bufManager.GetPage(log.PageId)
		if err != nil {
			return err
		}
		if page == nil {
			err = redoLog(bufManager, log)
			if err != nil {
				return err
			}
			page, err = bufManager.GetPage(log.PageId)
			if err != nil {
				return err
			}
		}

		if page.LSN < log.LSN {
			redoLog(bufManager, log)
			page.LSN = log.LSN
			continue
		}

		dirtyPageTable[log.PageId].RevLSN = page.LSN
	}
	return nil
}

// log must be a compensation log or update log.
func redoLog(bufManager *BufferManager, log *LogRecord) error {
	recoveryLog.InfoF("redoLog: %s", log)
	switch log.ActionTP {
	case AddAction, SetAction:
		pair := &Pair{}
		pair.Deserialize(log.AfterValue)
		return bufManager.Set(log.PageId, pair.Key, pair.Value, log.LSN)
	case DelAction:
		bufManager.Del(log.PageId, log.LSN)
		return nil
	default:
		panic("unknown action type")
	}
}

func undoForRecovery(logManager *LogManager, bufManager *BufferManager, activeTransactionTable map[uint64]*TransactionTableEntry) error {
	for {
		// 获取当前事务关联的最大 LSN
		maxUndoLsn := maximumUndoLsn(activeTransactionTable)

		// 所有事务均已回滚完毕，退出
		if maxUndoLsn == InvalidLsn {
			return nil
		}

		// 从 wal 文件的 offset 偏移处读取一个 LogRecord 。
		log, err := logManager.ReadLog(maxUndoLsn)
		if err != nil {
			return err
		}
		// 不同 log 类型，做不同处理。
		switch log.TP {
		case TransAbortLogType, TransBeginLogType:
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.PrevLsn
		case CompensationLogType:
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.UndoNextLsn
		case SetLogType:
			l := undoLog(log, bufManager, logManager, activeTransactionTable)
			activeTransactionTable[log.TransactionId].Lsn = l.LSN
			activeTransactionTable[log.TransactionId].UndoNextLsn = log.PrevLsn
			//
			if log.PrevLsn == InvalidLsn {
				endLog := &LogRecord{
					PrevLsn:       log.LSN,
					TransactionId: log.TransactionId,
					TP:            TransEndLogType,
				}
				logManager.AppendRecoveryLog(endLog)
				activeTransactionTable[log.TransactionId].State = TransactionC
			}
		default:
		}
	}
	return nil
}


//
func maximumUndoLsn(activeTransactionTable map[uint64]*TransactionTableEntry) int64 {
	maxLsn := int64(InvalidLsn)
	for _, entry := range activeTransactionTable {
		if entry.State == TransactionP || entry.State == TransactionC || entry.State == TransactionE {
			continue
		}
		if entry.UndoNextLsn > maxLsn {
			maxLsn = entry.UndoNextLsn
		}
	}
	return maxLsn
}

func undoLog(
	undoLog *LogRecord,
	bufManager *BufferManager,
	logManager *LogManager,
	activeTransactionTable map[uint64]*TransactionTableEntry,
) *LogRecord {

	// 创建 LogRecord 的补偿 LogRecord
	l := &LogRecord{
		TP:            CompensationLogType,		// 补偿
		TransactionId: undoLog.TransactionId,	// 事务 ID
		PageId:        undoLog.PageId,			// 页 ID
		PrevLsn:       activeTransactionTable[undoLog.TransactionId].Lsn,	// 事务关联的最新的 WAL 日志序号
		UndoNextLsn:   undoLog.PrevLsn,			//
	}

	recoveryLog.InfoF("undoLog: %s", undoLog)
	var err error

	switch undoLog.ActionTP {
	case AddAction:
		l.ActionTP = DelAction
		logManager.AppendRecoveryLog(l)
		err = bufManager.Del(undoLog.PageId, l.LSN)
	case SetAction:
		l.ActionTP = SetAction
		l.BeforeValue = undoLog.AfterValue
		l.AfterValue = undoLog.BeforeValue
		logManager.AppendRecoveryLog(l)
		pair := &Pair{}
		err = pair.Deserialize(l.AfterValue)
		if err != nil {
			panic(err)
		}
		err = bufManager.Set(undoLog.PageId, pair.Key, pair.Value, l.LSN)
	case DelAction:
		l.ActionTP = AddAction
		l.BeforeValue = nil
		l.AfterValue = undoLog.BeforeValue
		logManager.AppendRecoveryLog(l)
		pair := &Pair{}
		err = pair.Deserialize(l.AfterValue)
		if err != nil {
			panic(err)
		}
		err = bufManager.Set(undoLog.PageId, pair.Key, pair.Value, l.LSN)
	}
	if err != nil {
		panic(err)
	}
	return l
}
