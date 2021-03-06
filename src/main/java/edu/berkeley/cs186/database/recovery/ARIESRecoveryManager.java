package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * <p>
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     *
     * @param diskSpaceManager disk space manager
     * @param bufferManager    buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     * <p>
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     * <p>
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        long prevLsn = transactionTableEntry.lastLSN;
        LogRecord record = new CommitTransactionLogRecord(transNum, prevLsn);
        long lsn = logManager.appendToLog(record);
        transactionTableEntry.lastLSN = lsn;


        pageFlushHook(lsn);

        // TODO(proj5): implement
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     * <p>
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        transactionTableEntry.transaction.setStatus(Transaction.Status.ABORTING);
        long prevLsn = transactionTableEntry.lastLSN;
        LogRecord record = new AbortTransactionLogRecord(transNum, prevLsn);
        long lsn = logManager.appendToLog(record);
        transactionTableEntry.lastLSN = lsn;
        // TODO(proj5): implement
        return lsn;
    }

    private Optional<Long> undoRecordIfPossible(LogRecord lastRecord, long lastLsn) {
        if (lastRecord.isUndoable()) {
            Pair<LogRecord, Boolean> undoPair = lastRecord.undo(lastLsn);
            logManager.appendToLog(undoPair.getFirst());

            if (undoPair.getSecond()) {
                // true if needs to flushed false otherwise
                logManager.flushToLSN(undoPair.getFirst().getLSN());
            }

            // https://piazza.com/class/k5ecyhh3xdw1dd?cid=945 TODO FIX THIS. I USED THE SAME LOGIC FOR RESTART UNDO. SO IF A FIX IS FOUND HERE FIX THERE
//            if (lastRecord.getPageNum().isPresent()) {
//                if (undoPair.getFirst().getType() == LogType.ALLOC_PAGE
//                        || undoPair.getFirst().getType() == LogType.FREE_PAGE
//                        || undoPair.getFirst().getType() == LogType.UNDO_ALLOC_PAGE
//                        || undoPair.getFirst().getType() == LogType.UNDO_FREE_PAGE) {
//                    dirtyPageTable.remove(lastRecord.getPageNum().get());
//                } else if (lastRecord.getType() == LogType.UPDATE_PAGE) {
//                    dirtyPageTable.put(lastRecord.getPageNum().get(), undoPair.getFirst().getLSN());
//                } else if (lastRecord.getType() == LogType.UNDO_UPDATE_PAGE) {
//                    dirtyPageTable.putIfAbsent(lastRecord.getPageNum().get(), undoPair.getFirst().getLSN());
//                }
//            }


            undoPair.getFirst().redo(diskSpaceManager, bufferManager);

            return Optional.of(undoPair.getFirst().getLSN());
        }
        return Optional.empty();
    }

    private long rollbackOperations(LogRecord lastRecord, long stopLsn) {
        if (lastRecord.getLSN() <= stopLsn) {
            return stopLsn;
        }
        long lastLSN = lastRecord.getLSN();

        Optional<Long> res = undoRecordIfPossible(lastRecord, lastRecord.getLSN());
        if (res.isPresent()) {
            lastLSN = res.get();
        }
        Optional<Long> prevLsn;
        prevLsn = lastRecord.getPrevLSN();

        // The following case is sketch https://piazza.com/class/k5ecyhh3xdw1dd?cid=901_f26 following this
        if (lastRecord.getUndoNextLSN().isPresent() && lastRecord.getUndoNextLSN().get() <= stopLsn) {
            return lastLSN;
        }

        while (prevLsn.isPresent() && prevLsn.get() > stopLsn) {
            lastRecord = logManager.fetchLogRecord(prevLsn.get());

            // The following case is sketch https://piazza.com/class/k5ecyhh3xdw1dd?cid=901_f26 following this
            if (lastRecord.getUndoNextLSN().isPresent() && lastRecord.getUndoNextLSN().get() <= stopLsn) {
                break;
            }

            res = undoRecordIfPossible(lastRecord, lastLSN);

            if (res.isPresent()) {
                lastLSN = res.get();
            }

            prevLsn = lastRecord.getPrevLSN();

        }
        return lastLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     * <p>
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        Optional<Long> prevLSN = Optional.empty();
        if (transactionTableEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            LogRecord lastRecord = logManager.fetchLogRecord(transactionTableEntry.lastLSN);
            prevLSN = Optional.of(rollbackOperations(lastRecord, Long.MIN_VALUE));
        }

        transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        if (!prevLSN.isPresent()) {
            prevLSN = Optional.of(transactionTableEntry.lastLSN);
        }
        LogRecord record = new EndTransactionLogRecord(transNum, prevLSN.get());
        long lsn = logManager.appendToLog(record);
        transactionTableEntry.lastLSN = lsn;

        for (Long page : transactionTableEntry.touchedPages) {
            dirtyPageTable.putIfAbsent(page, prevLSN.get());
        }

        transactionTable.remove(transNum);
        // TODO(proj5): implement
        return lsn;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     * <p>
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     * <p>
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     * <p>
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     * <p>
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     * <p>
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum   transaction performing the write
     * @param pageNum    page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before     bytes starting at pageOffset before the write
     * @param after      bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long prevLsn = transactionTableEntry.lastLSN;

        long lsn;
        long earliestLogDirtied;
        if (before.length < BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLsn, pageOffset, before, after);
            lsn = logManager.appendToLog(record);
            earliestLogDirtied = lsn;
        } else {
            LogRecord undo = new UpdatePageLogRecord(transNum, pageNum, prevLsn, pageOffset, before, null);
            LogRecord redo = new UpdatePageLogRecord(transNum, pageNum, prevLsn, pageOffset, null, after);
            logManager.appendToLog(undo);
            lsn = logManager.appendToLog(redo);
            earliestLogDirtied = undo.LSN; // https://piazza.com/class/k5ecyhh3xdw1dd?cid=901_f27

        }
        transactionTableEntry.lastLSN = lsn;
        transactionTableEntry.touchedPages.add(pageNum);

        if (!dirtyPageTable.containsKey(pageNum)) {
            // Since it's the earliest log record dirtied, if the page already exists no need to update
            dirtyPageTable.put(pageNum, earliestLogDirtied);
        }
        // TODO(proj5): implement
        return lsn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     * <p>
     * This method should return -1 if the partition is the log partition.
     * <p>
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum  partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     * <p>
     * This method should return -1 if the partition is the log partition.
     * <p>
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum  partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     * <p>
     * This method should return -1 if the page is in the log partition.
     * <p>
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum  page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     * <p>
     * This method should return -1 if the page is in the log partition.
     * <p>
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum  page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     * <p>
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name     name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     *
     * @param transNum transaction to delete savepoint for
     * @param name     name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     * <p>
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name     name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        rollbackOperations(lastRecord, LSN);
        // TODO(proj5): implement
        return;
    }

    /**
     * Create a checkpoint.
     * <p>
     * First, a begin checkpoint record should be written.
     * <p>
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     * <p>
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        // Copied from format below these 2 for loops
        for (Map.Entry<Long, Long> item : dirtyPageTable.entrySet()) {
            boolean fitsInOneRecord = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size() + 1,
                    txnTable.size(),
                    touchedPages.size(),
                    numTouchedPages);

            if (!fitsInOneRecord) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            dpt.put(item.getKey(), item.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> item : transactionTable.entrySet()) {
            boolean fitsInOneRecord = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(),
                    txnTable.size() + 1,
                    touchedPages.size(),
                    numTouchedPages);

            if (!fitsInOneRecord) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            // Requires format
            Pair<Transaction.Status, Long> pair = new Pair<>(
                    item.getValue().transaction.getStatus(),
                    item.getValue().lastLSN);

            txnTable.put(item.getKey(), pair);
        }
        // END TODO

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     * <p>
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     * <p>
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        restartAnalysis();
        restartRedo();

        bufferManager.iterPageNums((pageNum, dirty) -> { // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f2
            if (!dirty) {
                dirtyPageTable.remove(pageNum);
            }
        });
        return () -> {
            restartUndo();
            checkpoint();
        };
    }

    public TransactionTableEntry createNewXact(TransactionTableEntry entry, long transNum) {
        return createNewXactType(entry, transNum, null);
    }

    public TransactionTableEntry createNewXactType(TransactionTableEntry entry, long transNum, Transaction.Status status) {
        if (entry != null) return entry;
        Transaction xact = newTransaction.apply(transNum);
        if (status != null) {
            xact.setStatus(status);
        }
        startTransaction(xact);
        return this.transactionTable.get(xact.getTransNum());
    }

    /**
     * This method performs the analysis pass of restart recovery.
     * <p>
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     * <p>
     * We then begin scanning log records, starting at the begin checkpoint record.
     * <p>
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     * - add to touchedPages
     * - acquire X lock
     * - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     * <p>
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * <p>
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     * <p>
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     * add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     * transaction table if the transaction has not finished yet, and acquire X locks.
     * <p>
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord zeroRecord = logManager.fetchLogRecord(0L);
        assert (zeroRecord != null);
        // Type casting
        assert (zeroRecord.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) zeroRecord;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            LogType recordType = record.getType();
            //https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f115 Weird issue with 6th transaction being included. The fix was not iterate over master record
            if (isPageType(record) || isPartitionType(record)) {
                if (!record.getTransNum().isPresent()) {
                    continue;
                }

                long transNum = record.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(transNum);
                tableEntry = createNewXact(tableEntry, transNum);

                Transaction xact = tableEntry.transaction;
                tableEntry.lastLSN = record.getLSN();

                if (isPartitionType(record)) {
                    continue; // Ignore if update part type
                }

                if (!record.getPageNum().isPresent()) {
                    continue;
                }

                long pageNum = record.getPageNum().get();
                if (recordType == LogType.UPDATE_PAGE || recordType == LogType.UNDO_UPDATE_PAGE) {
                    dirtyPageTable.putIfAbsent(pageNum, record.getLSN());
                } else {
                    // No need to explicitly flush
                    dirtyPageTable.remove(pageNum); // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f35
                }

                tableEntry.touchedPages.add(pageNum);
                LockContext context = getPageLockContext(pageNum);
                acquireTransactionLock(xact, context, LockType.X);
            }


            if (recordType == LogType.ABORT_TRANSACTION
                    || recordType == LogType.COMMIT_TRANSACTION
                    || recordType == LogType.END_TRANSACTION) {
                if (!record.getTransNum().isPresent()) {
                    continue;
                }
                if (!record.getTransNum().isPresent()) {
                    continue;
                }
                long recordTransNum = record.getTransNum().get();
                TransactionTableEntry recordTableEntry = transactionTable.get(recordTransNum);
                recordTableEntry = createNewXact(recordTableEntry, recordTransNum);

                Transaction xact = recordTableEntry.transaction;
                recordTableEntry.lastLSN = Math.max(record.getLSN(), recordTableEntry.lastLSN);

                if (recordType == LogType.END_TRANSACTION) {
                    xact.cleanup();
                    xact.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(xact.getTransNum());
                } else if (recordType == LogType.ABORT_TRANSACTION) {
                    xact.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else if (recordType == LogType.COMMIT_TRANSACTION) {
                    xact.setStatus(Transaction.Status.COMMITTING);
                }


            }


            if (record.type == LogType.BEGIN_CHECKPOINT) {
                record.getMaxTransactionNum().ifPresent(aLong -> updateTransactionCounter.accept(aLong));
            }

            if (record.type == LogType.END_CHECKPOINT) {
                for (Map.Entry<Long, Long> item : record.getDirtyPageTable().entrySet()) {
                    this.dirtyPageTable.put(item.getKey(), item.getValue());
                }


                for (Map.Entry<Long, Pair<Transaction.Status, Long>> item : record.getTransactionTable().entrySet()) {
                    long transNum = item.getKey();
                    Transaction.Status status = item.getValue().getFirst();
                    long recordLastLSN = item.getValue().getSecond();

                    TransactionTableEntry tableEntry = transactionTable.get(transNum);
                    tableEntry = createNewXact(tableEntry, transNum);
                    Transaction transaction = tableEntry.transaction;
                    if (transaction.getStatus() == Transaction.Status.COMPLETE) {
                        this.transactionTable.remove(transaction.getTransNum());
                        continue;
                    }
                    if (status == Transaction.Status.ABORTING) {
                        status = Transaction.Status.RECOVERY_ABORTING;
                    }
                    // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f5
                    if (transaction.getStatus() != Transaction.Status.COMMITTING && transaction.getStatus() != Transaction.Status.RECOVERY_ABORTING) {
                        transaction.setStatus(status); // Record will always overwrite
                    }
                    tableEntry.lastLSN = Math.max(tableEntry.lastLSN, recordLastLSN); // Record will always overwrite if max

                }

                for (Map.Entry<Long, List<Long>> transNumPagesPair : record.getTransactionTouchedPages().entrySet()) {
                    long transNum = transNumPagesPair.getKey();
                    List<Long> pages = transNumPagesPair.getValue();
                    TransactionTableEntry tableEntry = transactionTable.get(transNum);
                    tableEntry = createNewXact(tableEntry, transNum);

                    Transaction transaction = tableEntry.transaction;
                    // We only care about non complete transactions
                    if (transaction.getStatus() == Transaction.Status.COMPLETE) {
                        transactionTable.remove(transaction.getTransNum());
                        continue;
                    }
                    for (Long pageNum : pages) {
                        tableEntry.touchedPages.add(pageNum);
                        LockContext context = getPageLockContext(pageNum);
                        acquireTransactionLock(transaction, context, LockType.X);
                    }

                }
            }
        }

        // TODO(proj5): implement

        for (Map.Entry<Long, TransactionTableEntry> item : this.transactionTable.entrySet()) {
            long transNum = item.getKey();

            TransactionTableEntry tableEntry = transactionTable.get(transNum);
            if (tableEntry == null) {
                continue;
            }

            Transaction transaction = tableEntry.transaction;

            // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f21 Weird edge case for commiting then aborting or something like that

            // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f17 ?? maybe and https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f140
            if (transaction.getStatus() == Transaction.Status.RUNNING) {
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                long newAbortLSN = logManager.appendToLog(new AbortTransactionLogRecord(transaction.getTransNum(), tableEntry.lastLSN));
                tableEntry.lastLSN = newAbortLSN;
            }
            // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f11
            if (transaction.getStatus() == Transaction.Status.COMMITTING) {
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(transaction.getTransNum(), tableEntry.lastLSN));
                if (!transactionTable.containsKey(transaction.getTransNum())) {
                    // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f11
                    TransactionTableEntry newTransac = createNewXact(null, transaction.getTransNum());
                    if (newTransac.transaction.getStatus() == Transaction.Status.COMPLETE) {
                        transactionTable.remove(newTransac.transaction.getTransNum());
                    }

                } else {

                    transactionTable.remove(transaction.getTransNum());
                }
            }

        }


        return;
    }

    public boolean isPartitionType(LogRecord record) {
        return record.getType() == LogType.ALLOC_PART || record.getType() == LogType.FREE_PART ||
                record.getType() == LogType.UNDO_ALLOC_PART || record.getType() == LogType.UNDO_FREE_PART;
    }

    public boolean isPageType(LogRecord record) {
        return record.getType() == LogType.UNDO_FREE_PAGE || record.getType() == LogType.UNDO_UPDATE_PAGE ||
                record.getType() == LogType.UNDO_ALLOC_PAGE || record.getType() == LogType.UPDATE_PAGE ||
                record.getType() == LogType.ALLOC_PAGE || record.getType() == LogType.FREE_PAGE;
    }

    /**
     * This method performs the redo pass of restart recovery.
     * <p>
     * First, determine the starting point for REDO from the DPT.
     * <p>
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     * the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5): implement
        long minLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> records = logManager.scanFrom(minLSN);
        while (records.hasNext()) {
            LogRecord record = records.next();
            if (!record.isRedoable()) {
                continue;
            }
            boolean partitionType = isPartitionType(record);
            boolean pageType = isPageType(record);
            Optional<Long> pageNum = record.getPageNum();
            boolean inDPT = pageNum.isPresent() && dirtyPageTable.containsKey(pageNum.get());
            //the LSN is not less than the recLSN of the page, and
            //the pageLSN on the page itself is strictly less than the LSN of the record.
            boolean shouldRedo = partitionType;
            if (!shouldRedo && inDPT && pageType) {
                long recLSN = dirtyPageTable.get(pageNum.get());
                LockContext context = getPageLockContext(pageNum.get());

                long pageLSN = bufferManager.fetchPage(context, pageNum.get(), false).getPageLSN();

                // pageLSN of the page is strictly less than the LSN of the record
                shouldRedo = record.getLSN() >= recLSN && record.getLSN() > pageLSN;
            }
            if (shouldRedo) {
                record.redo(diskSpaceManager, bufferManager);
            }

        }

    }

    /**
     * This method performs the undo pass of restart recovery.
     * <p>
     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     * <p>
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     * (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        List<Pair<Long, Transaction>> abortingTransactions = new ArrayList<>();
        for (TransactionTableEntry entry : transactionTable.values()) {
            if (entry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                abortingTransactions.add(new Pair<>(entry.lastLSN, entry.transaction));
            }
        }

        PriorityQueue<Pair<Long, Transaction>> xacts = new PriorityQueue<>(new PairFirstReverseComparator<>());
        xacts.addAll(abortingTransactions);

        while (!xacts.isEmpty()) {
            Pair<Long, Transaction> p = xacts.poll();
            Long lastLSN = p.getFirst();
            LogRecord record = logManager.fetchLogRecord(lastLSN);
            TransactionTableEntry entry = transactionTable.get(p.getSecond().getTransNum());
            if (record.isUndoable()) {
                Pair<LogRecord, Boolean> CLR = record.undo(entry.lastLSN);
                long l = logManager.appendToLog(CLR.getFirst());
                if (CLR.getSecond()) {
                    logManager.flushToLSN(CLR.getFirst().getLSN());
                }
                transactionTable.get(p.getSecond().getTransNum()).lastLSN = l;

                // https://piazza.com/class/k5ecyhh3xdw1dd?cid=945
                if (record.getPageNum().isPresent()) {
                    if (CLR.getFirst().getType() == LogType.ALLOC_PAGE
                            || CLR.getFirst().getType() == LogType.FREE_PAGE
                            || CLR.getFirst().getType() == LogType.UNDO_ALLOC_PAGE
                            || CLR.getFirst().getType() == LogType.UNDO_FREE_PAGE) {
                        dirtyPageTable.remove(record.getPageNum().get());
                    } else if (record.getType() == LogType.UPDATE_PAGE) {
                        dirtyPageTable.put(record.getPageNum().get(), CLR.getFirst().getLSN());
                    } else if (record.getType() == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(record.getPageNum().get(), CLR.getFirst().getLSN());
                    }
                }

                CLR.getFirst().redo(diskSpaceManager, bufferManager);


            }

            long LSN = record.getUndoNextLSN().isPresent() ? record.getUndoNextLSN().get() : record.getPrevLSN().get();
//
            if (LSN == 0) {
                p.getSecond().cleanup();
                p.getSecond().setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(p.getSecond().getTransNum(), entry.lastLSN));
                transactionTable.remove(p.getSecond().getTransNum());
                // https://piazza.com/class/k5ecyhh3xdw1dd?cid=902_f80 says to avoid using end here YOLO
            } else {
                xacts.add(new Pair<>(LSN, p.getSecond()));
            }


        }

    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     *
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     *
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType    type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     *
     * @param transactionContext transaction context to request lock for
     * @param lockContext        lock context to lock
     * @param lockType           type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                        lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}