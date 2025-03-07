/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

type oracle struct {
	isManaged bool // 不改变值，则不需要锁定。

	detectConflicts bool // 确定是否应检查txns是否存在冲突。

	sync.Mutex // For nextTxnTs and commits.
	// writeChLock lock 用于确保事务被写入
	// 通道的顺序与它们的提交时间戳相同。
	writeChLock sync.Mutex
	nextTxnTs   uint64

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	// 用于阻止NewTransaction，因此所有以前的提交对新的读取都是可见的。
	txnMark *y.WaterMark

	// Either of these is used to determine which versions can be permanently
	// 其中任何一个都用于确定哪些版本可以永久保存
	// discarded during compaction.
	// 在compaction过程中丢弃。
	discardTs uint64       // Used by ManagedDB.
	readMark  *y.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// committedTxns包含所有已提交的写入
	// of keys written and their latest commit counter).
	// 写入的密钥及其最新提交计数器
	committedTxns []committedTxn
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	// closer用于阻止水印
	closer *z.Closer
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		isManaged:       opt.managedTxns,
		detectConflicts: opt.DetectConflicts, // 是否支持冲突检测
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		//
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark: &y.WaterMark{Name: "badger.PendingReads"}, //读取的时间戳，下面这三行都是并发控制的
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"}, //提交的时间戳，txn在badger里面是事务的缩写
		closer:   z.NewCloser(2),
	}
	orc.readMark.Init(orc.closer)
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

func (o *oracle) readTs() uint64 { // 利用时间辍作事务的版本号
	if o.isManaged {
		panic("ReadTs should not be retrieved for managed DB")
	}

	var readTs uint64 // TODO:readTs 是时间戳？
	o.Lock()
	readTs = o.nextTxnTs - 1
	o.readMark.Begin(readTs) // 标记当前事务已经进入开始读的阶段
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	//等待所有没有冲突、已分配提交时间戳并且正在进行写值日志和LSM树过程的txn（判断标准是比当前读时间戳小的txn）。不在这里等待可能意味着一些已提交的txn将不会被读取。
	y.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	return readTs
}

func (o *oracle) nextTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs
}

func (o *oracle) incrementNextTs() {
	o.Lock()
	defer o.Unlock()
	o.nextTxnTs++
}

// Any deleted or invalid versions at or below ts would be discarded during
// compaction to reclaim disk space in LSM tree and thence value log.
func (o *oracle) setDiscardTs(ts uint64) {
	o.Lock()
	defer o.Unlock()
	o.discardTs = ts
	o.cleanupCommittedTransactions()
}

func (o *oracle) discardAtOrBelow() uint64 {
	if o.isManaged {
		o.Lock()
		defer o.Unlock()
		return o.discardTs
	}
	return o.readMark.DoneUntil()
}

// hasConflict must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, committedTxn := range o.committedTxns { // committedTxns是活跃事务的数组
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		// 如果commitedTxn.ts小于txn.readTs，则意味着commitedTxn在当前事务开始之前完成。
		// 在这种情况下，我们不需要检查冲突。这种变化假设了线性化。缺乏线性化可能会导致新txn的读取ts低于之前txn的提交ts
		if committedTxn.ts <= txn.readTs {
			continue
		}

		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true // 比当前事务时间戳大的，且对当前读取的key进行过写入，就会产生冲突
			}
		}
	}

	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	//下面这两行标记是原子操作
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) { //检查活跃的并发执行的事务是否有冲突
		return 0, true
	}

	var ts uint64
	if !o.isManaged {
		o.doneRead(txn)                  //标记完成
		o.cleanupCommittedTransactions() //清理已提交的事务

		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextTxnTs
		o.nextTxnTs++
		o.txnMark.Begin(ts) //正式进入提交阶段，badger把整个事务分为读取阶段以及提交阶段

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	y.AssertTrue(ts >= o.lastCleanupTs)

	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		o.committedTxns = append(o.committedTxns, committedTxn{ // committedTxns存的是读阶段完成，但是提交阶段还未完成的事务
			ts:           ts,
			conflictKeys: txn.conflictKeys,
		})
	}

	return ts, false
}

func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs) // 通知readMark完成
	}
}

func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	var maxReadTs uint64
	if o.isManaged {
		maxReadTs = o.discardTs
	} else {
		maxReadTs = o.readMark.DoneUntil() //获取读取事务的水位来当作最大读时间戳，小于这个的表示已经全部提交
	}

	y.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == o.lastCleanupTs {
		return
	}
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		if txn.ts <= maxReadTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}
	o.txnMark.Done(cts)
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64
	size     int64
	count    int64
	db       *DB

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	pendingWrites   map[string]*Entry // cache stores any writes done by txn.
	duplicateWrites []*Entry          // Used in managed mode to store duplicate entries.

	numIterators atomic.Int32
	discarded    bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type pendingWritesIterator struct {
	entries  []*Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() y.ValueStruct {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.meta,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
		Version:   pi.readTs,
	}
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (txn *Txn) checkSize(e *Entry) error {
	count := txn.count + 1
	// Extra bytes for the version in key.
	size := txn.size + e.estimateSizeAndSetThreshold(txn.db.valueThreshold()) + 10
	if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

func (txn *Txn) modify(e *Entry) error {
	const maxKeySize = 65000 // key的长度是uint32的长度，所以其最大是65535，而剩下的535是因为badger会在key后面拼接上一个时间戳

	// 对于kv的检查
	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix): //这里是检测是否包含!badger!，包含的就是因badger系统自身的需要，把一些其自身的必要的数据放到数据库里的数据
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize: // value值大小是否大于vlog文件大小
		return exceedsSize("Value", txn.db.opt.ValueLogFileSize, e.Value)
	case txn.db.opt.InMemory && int64(len(e.Value)) > txn.db.valueThreshold():
		return exceedsSize("Value", txn.db.valueThreshold(), e.Value)
	}

	if err := txn.db.isBanned(e.Key); err != nil {
		return err
	}

	if err := txn.checkSize(e); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection. If conflict detection
	// is disabled, we don't need to store key hashes in this map.
	if txn.db.opt.DetectConflicts {
		fp := z.MemHash(e.Key) // Avoid dealing with byte arrays.  注意拿得是KEY的HASH值去检查冲突（而这个hash值是由内存地址生成的）
		txn.conflictKeys[fp] = struct{}{}
	}
	// If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
	// Add the entry to duplicateWrites only if both the entries have different versions. For
	// same versions, we will overwrite the existing entry.
	if oldEntry, ok := txn.pendingWrites[string(e.Key)]; ok && oldEntry.version != e.version { //同一个key Set了两次（版本号不一样），那么就放到下面这个数组里面
		txn.duplicateWrites = append(txn.duplicateWrites, oldEntry)
	}
	txn.pendingWrites[string(e.Key)] = e
	return nil
	// 注意此时，还没有写到磁盘，还在内存
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(NewEntry(key, val))
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
func (txn *Txn) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	if err := txn.db.isBanned(key); err != nil {
		return nil, err
	}

	item = new(Item)
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) { // 如果是读写事务，则判断当前key是否pendingWrites，如果在，下面就直接拼装返回
			if isDeletedOrExpired(e.meta, e.ExpiresAt) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from cache.
			item.meta = e.meta
			item.val = e.Value
			item.userMeta = e.UserMeta
			item.key = key
			item.status = prefetched
			item.version = txn.readTs
			item.expiresAt = e.ExpiresAt
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		txn.addReadKey(key) // 标记当前KEY是读取的
	}

	//下面就是没直接找到，去LSM TREE取相应的key了
	seek := y.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.txn = txn
	item.expiresAt = vs.ExpiresAt
	return item, nil
}

func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp) //记录当前事务读取了哪些KEY
		txn.readsLock.Unlock()
	}
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if txn.numIterators.Load() > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
	if !txn.db.orc.isManaged {
		txn.db.orc.doneRead(txn)
	}
}

func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel. So, we
	// acquire a writeChLock before getting a commit timestamp, and only release
	// it after pushing the entries to it.
	// 确保事务提交的顺序写入数据库里。因此，我们在获取提交时间戳之前获取一个writeChLock，只有在将条目推送到它之后才释放它。
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()

	commitTs, conflict := orc.newCommitTs(txn)
	if conflict {
		return nil, ErrConflict
	}

	keepTogether := true
	setVersion := func(e *Entry) { //在KEY后拼接版本号，这里是一个闭包
		if e.version == 0 {
			e.version = commitTs
		} else {
			keepTogether = false
		}
	}
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}
	// The duplicateWrites slice will be non-empty only if there are duplicate
	// entries with different versions.
	for _, e := range txn.duplicateWrites {
		setVersion(e)
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+len(txn.duplicateWrites)+1)

	processEntry := func(e *Entry) {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, e.version)
		// Add bitTxn only if these entries are part of a transaction. We
		// support SetEntryAt(..) in managed mode which means a single
		// transaction can have entries with different timestamps. If entries
		// in a single transaction have different timestamps, we don't add the
		// transaction markers.
		if keepTogether {
			e.meta |= bitTxn
		}
		entries = append(entries, e)
	}

	// The following debug information is what led to determining the cause of
	// bank txn violation bug, and it took a whole bunch of effort to narrow it
	// down to here. So, keep this around for at least a couple of months.
	// var b strings.Builder
	// fmt.Fprintf(&b, "Read: %d. Commit: %d. reads: %v. writes: %v. Keys: ",
	// 	txn.readTs, commitTs, txn.reads, txn.conflictKeys)
	for _, e := range txn.pendingWrites {
		processEntry(e)
	}
	for _, e := range txn.duplicateWrites {
		processEntry(e)
	}

	if keepTogether {
		// CommitTs should not be zero if we're inserting transaction markers.
		y.AssertTrue(commitTs != 0)
		e := &Entry{
			Key:   y.KeyWithTs(txnKey, commitTs),
			Value: []byte(strconv.FormatUint(commitTs, 10)),
			meta:  bitFinTxn, //元数据信息
		}
		entries = append(entries, e)
	}

	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs) //告诉orc水位信息，标记完成
		return err
	}
	return ret, nil
}

func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	keepTogether := true
	for _, e := range txn.pendingWrites {
		if e.version != 0 {
			keepTogether = false
		}
	}

	// If keepTogether is True, it implies transaction markers will be added.
	// In that case, commitTs should not be never be zero. This might happen if
	// someone uses txn.Commit instead of txn.CommitAt in managed mode.  This
	// should happen only in managed mode. In normal mode, keepTogether will
	// always be true.
	if keepTogether && txn.db.opt.managedTxns && txn.commitTs == 0 {
		return errors.New("CommitTs cannot be zero. Please use commitAt instead")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, Badger will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
// Commit按照以下步骤提交事务：
// 1.如果没有写入，请立即返回。
// 2.检查自txn启动以来读取的行是否已更新。如果是这样，请返回ErrConflict。
// 3.如果没有冲突，则生成一个提交时间戳并更新已写入行的提交时间。
// 4.批量处理所有写入，将其写入值日志和LSM树。
// 5.如果提供回调，Badger将在检查冲突后立即返回。写入数据库将在后台进行。如果发生冲突，将返回错误，回调将不会运行。如果没有冲突，则在成功完成写入或写入过程中出现任何错误时，将在后台调用回调。
// 如果错误为零，则事务成功提交。如果出现非nil错误，LSM树将不会更新，因此不需要任何回滚。
func (txn *Txn) Commit() error {
	// txn.conflictKeys can be zero if conflict detection is turned off. So we
	// should check txn.pendingWrites.
	if len(txn.pendingWrites) == 0 { //判断当前txn是否有set操作发生过
		// Discard the transaction so that the read is marked done.
		txn.Discard()
		return nil
	}
	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil { //这行做提交时的预处理检查
		return err
	}
	defer txn.Discard() //这个函数可以反复调用

	txnCb, err := txn.commitAndSend() //提交到orical对象（orc）里，告知事务时间戳已经提交了，你需要更新水位线，使比当前水位线小的事务往下执行（保证事务的一致性）
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// TODO: What if some of the txns successfully make it to value log, but others fail.
	// Nothing gets updated to LSM, until a restart happens.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(txn.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		// Discard the transaction so that the read is marked done.
		txn.Discard()
		return
	}

	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// NewTransaction creates a new transaction. Badger supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews. Badger achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//	txn := db.NewTransaction(false)
//	defer txn.Discard()
//	// Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update, false)
}

func (db *DB) newTransaction(update, isManaged bool) *Txn {
	if db.opt.ReadOnly && update {
		// DB is read-only, force read-only transaction.
		// 若只读，则把更新关掉，badger读写是不一样的，只读的话会少做一些事情，比如不用考虑事物冲突
		update = false
	}

	txn := &Txn{
		update: update,
		db:     db,
		count:  1,                       // One extra entry for BitFin.
		size:   int64(len(txnKey) + 10), // Some buffer for the extra entry.
	}
	if update {
		if db.opt.DetectConflicts {
			txn.conflictKeys = make(map[uint64]struct{}) // 这个map对进行修改的key进行记录，使用map也是方便其他事务进行冲突检查
		}
		txn.pendingWrites = make(map[string]*Entry) //所有当前事务插入新key的操作在这里记录
	}
	if !isManaged {
		txn.readTs = db.orc.readTs()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	var txn *Txn
	if db.opt.managedTxns {
		txn = db.NewTransactionAt(math.MaxUint64, false)
	} else {
		txn = db.NewTransaction(false)
	}
	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	if db.opt.managedTxns {
		panic("Update can only be used with managedDB=false.")
	}
	txn := db.NewTransaction(true) //创建一个事物对象
	defer txn.Discard()            //等运行结束时，关闭事务

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
