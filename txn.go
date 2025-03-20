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
	// of keys written and their latest commit counter).
	committedTxns []committedTxn // committedTxns包含近期的已提交事务的信息
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	// closer用于控制水位
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
		readMark: &y.WaterMark{Name: "badger.PendingReads"}, //读取的时间戳堆，下面这三行都是并发控制的
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"}, //提交的时间戳堆
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

	var readTs uint64 // TODO:readTs 是时间戳，但是注意不是物理时间戳，是逻辑上的版本号
	o.Lock()          //获取版本号需要加锁
	readTs = o.nextTxnTs - 1
	o.readMark.Begin(readTs) // 标记当前事务已经进入开始读的阶段
	// readMark 与 txnMark 一样是一个 WaterMark 结构体 。
	// 但它没有利用 WaterMark 结构体等待点位的能力，只利用它的堆数据结构来跟踪当前活跃的事务的时间戳范围，用于找出哪些事务可以过期回收。
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.

	// 等待所有已分配提交时间戳并且正在进行写值日志和LSM树过程落盘的txn（判断标准是比当前读时间戳小的txn）。
	// 不在这里等待可能意味着一些已提交的txn将不会被读取，即读取到旧版本的数据
	y.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	// txnMarkt 字段是 WaterMark 结构体类型，它内部会维护一个堆数据结构，可以用于跟踪事务的时间戳区段的变化通知。
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
	for _, committedTxn := range o.committedTxns { // committedTxns 数组记录近期的已提交事务的信息
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		// 如果commitedTxn.ts小于当前txn.readTs，则意味着commitedTxn在当前事务开始之前完成。
		// 在这种情况下，我们不需要检查冲突。这种变化假设了线性化。缺乏线性化可能会导致新txn的读取ts低于之前txn的提交ts
		if committedTxn.ts <= txn.readTs {
			continue
		}

		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true // 如果遍历的已提交事务比当前事务的readTs大的，且对当前事务读取过的key进行过写入，就会产生冲突
			}
		}
	}

	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	//下面这两行标记是原子操作
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) { //检查活跃的并发执行的事务是否有冲突，冲突检测的逻辑很简单，遍历 committedTxns，找出当前事务开始之后提交的事务，判断自己读到的 key 中，是否存在于其他事务的写列表中
		return 0, true
	}

	var ts uint64
	if !o.isManaged {
		o.doneRead(txn)                  //通知readMark完成
		o.cleanupCommittedTransactions() //触发清理committedTxns的函数

		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextTxnTs
		o.nextTxnTs++
		o.txnMark.Begin(ts) //正式进入提交阶段，badger把整个事务分为读取阶段以及提交阶段

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	y.AssertTrue(ts >= o.lastCleanupTs)

	if o.detectConflicts { //是否进行冲突检查
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		// 我们应该确保在禁用冲突检测时，txns不会添加到o.commitedTxns切片中，否则该切片将继续增长。
		o.committedTxns = append(o.committedTxns, committedTxn{ // committedTxns包含近期的已提交事务的信息，在这里就加过去
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

// 清理committedTxns数组
// 其会清理掉比commitTs比当前活跃事务中最早的readTs还要早的事务
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
		maxReadTs = o.readMark.DoneUntil() // 在 readMark 堆中获取当前活跃事务的最早 readTs
	}

	y.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	// 如果maxReadTs（仍在运行的最旧事务的ReadTs）没有增加，则不运行清理
	// 即oracle 会记录 lastCleanupTs 记录上次清理的时间戳，避免不必要的清理操作。
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

	reads        []uint64            //记录当前事务读取了哪些KEY（记录的是key的hash值）
	conflictKeys map[uint64]struct{} //记录当前事务修改了哪些KEY（同样记录的是key的hash值），用于冲突检查
	readsLock    sync.Mutex          // guards the reads slice. See addReadKey.

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
	for _, e := range txn.pendingWrites { //拿到所有当前事务待写入的KV对
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	// 一般来说，每个事务的待处理写入次数不应太大。
	sort.Slice(entries, func(i, j int) bool { //排序
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

	// 对于kv的检查，看是否合法
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
		fp := z.MemHash(e.Key)            // Avoid dealing with byte arrays.  注意拿得是KEY的HASH值去检查冲突（而这个hash值是由内存地址生成的）
		txn.conflictKeys[fp] = struct{}{} // 这里就是增加写入过的KEY的HASH值来方便之后的冲突检查了
	}
	// If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
	// Add the entry to duplicateWrites only if both the entries have different versions. For
	// same versions, we will overwrite the existing entry.
	// 如果在托管模式下插入了重复条目，请将其移动到重复写入切片。仅当两个条目具有不同版本时，才将条目添加到duplicateWrites。对于相同的版本，我们将覆盖现有条目。
	if oldEntry, ok := txn.pendingWrites[string(e.Key)]; ok && oldEntry.version != e.version { //如果在托管模式下，若同一个key 写入了两次（但是版本号不一样），那么就把老版本的放到下面这个计算重复写入的数组里面
		txn.duplicateWrites = append(txn.duplicateWrites, oldEntry) // 因为如果不放入的话，老版就会被覆盖清理掉
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
	// 如果是读写事务
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) { // 则判断当前key是否在pendingWrites（就是判断是否是当前事务之前提交过的数据），如果在，下面就直接拼装返回
			if isDeletedOrExpired(e.meta, e.ExpiresAt) { // 在，且没有过期就开始拼装返回
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

	//下面就是没在当前事务历史操作内直接找到，去LSM TREE取相应的key了
	seek := y.KeyWithTs(key, txn.readTs) // key后拼接读取时间戳（亦指事务的开始时间戳）
	vs, err := txn.db.get(seek)          // 核心操作，去找结果了
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
		fp := z.MemHash(key) // 注意拿得是KEY的HASH值（而这个hash值是由内存地址生成的）

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		//由于存在多个迭代器的可能性，现在读写事务中的多个线程可以同时读取key。读取切片当前不是线程安全的，每当我们将密钥标记为读取时，都需要锁定它。
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

	commitTs, conflict := orc.newCommitTs(txn) //让orc进行冲突检测和过期事务清理，如果无冲突，获取授时 commitTs，并将当前事务添加到 commitedTxns
	if conflict {
		return nil, ErrConflict //有冲突，返回错误（貌似是一路返回到客户端，并不做纠错处理？）
	}

	keepTogether := true           // true为非托管模式，这个是在托管模式下用的东西，在这个模式下，单个事务可以有不同时间戳的条目，托管模式（managed mode）是干啥的还需要再看
	setVersion := func(e *Entry) { //在KEY后拼接版本号，这里是一个闭包
		if e.version == 0 {
			e.version = commitTs
		} else {
			keepTogether = false
		}
	}
	//下面两个for循环是为 pendingWrites 和 duplicateWrites 中的 Entry 的 version 绑定 commitTs
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}
	// The duplicateWrites slice will be non-empty only if there are duplicate
	// entries with different versions.
	// 只有当存在具有不同版本的重复条目时，duplexeWrites切片才会非空。
	for _, e := range txn.duplicateWrites {
		setVersion(e)
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+len(txn.duplicateWrites)+1)

	processEntry := func(e *Entry) { // 使Entry存储的 key 绑定 commitTs（Entry是存储单个kv对的数据结构）
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, e.version)
		// Add bitTxn only if these entries are part of a transaction. We
		// support SetEntryAt(..) in managed mode which means a single
		// transaction can have entries with different timestamps. If entries
		// in a single transaction have different timestamps, we don't add the
		// transaction markers.
		// 仅当这些条目是事务的一部分时，才添加bitTxn。我们在托管模式下支持SetEntryAt（..），这意味着单个事务可以有不同时间戳的条目。如果单个事务中的条目具有不同的时间戳，我们不会添加事务标记。
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
	// entries 是pendingWrites与duplicateWrites两个缓冲区经过处理（如为key绑定commitTs）后的合并体
	req, err := txn.db.sendToWriteCh(entries) //进行落盘操作，req是返回的回调函数
	if err != nil {                           //报错了
		orc.doneCommit(commitTs) //告诉orc当前commitTs已经提交完成，让其更新水位信息，即移动 txnMark 的点位。且诺有等待的新事务请求readTs则其会通知其的WaitForMark函数
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		// 在将commitTs标记为已完成之前等待。我们不能推迟上面的doneCommit，因为它是从这里的回调调用的。
		orc.doneCommit(commitTs) //告诉orc当前commitTs已经提交完成，让其更新水位信息，即移动 txnMark 的点位。且诺有等待的新事务请求readTs则其会通知其的WaitForMark函数
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
	// 如果keepTogether为True，则意味着将添加事务标记。
	// 在这种情况下，commitTs不应该永远为零。如果有人使用txn，可能会发生这种情况。提交而不是txn。以托管模式提交。这应该只在托管模式下发生。在正常模式下，keepTogether始终为真。
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
	if len(txn.pendingWrites) == 0 { //判断当前txn是否有写入操作发生过，为空直接返回
		// Discard the transaction so that the read is marked done.
		txn.Discard()
		return nil
	}
	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil { //提交前的预处理检查
		return err
	}
	defer txn.Discard() //这个函数可以反复调用

	txnCb, err := txn.commitAndSend() //核心操作，提交到orical对象（orc）里，告知事务时间戳已经提交了，你需要更新水位线，使比当前水位线小的事务往下执行（保证事务的一致性）
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

// NewTransactionAt函数用于创建事务
func (db *DB) newTransaction(update, isManaged bool) *Txn {
	if db.opt.ReadOnly && update {
		// DB is read-only, force read-only transaction.
		// 若只读，则把更新关掉，badger读写是不一样的，只读的话会少做一些事情，比如不用考虑事物冲突,也不用获取commitTS
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
		txn.pendingWrites = make(map[string]*Entry) //所有当前事务写入的操作在这里记录
	}
	if !isManaged {
		txn.readTs = db.orc.readTs() //为当前新增的事务授时，即记录开始时间戳，因为常用于读取数据，所以也叫读取时间戳（直接复制来自 oracle 对象的 nextTxnTs 字段中记录的当前时间戳即可。）
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error { //处理只读事务，只读事务除了begin等少数操作，不会阻塞其他事务
	if db.IsClosed() {
		return ErrDBClosed
	}
	var txn *Txn
	if db.opt.managedTxns {
		txn = db.NewTransactionAt(math.MaxUint64, false) // NewTransactionAt函数用于创建事务
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

	if err := fn(txn); err != nil { //执行传入的函数体
		return err
	}

	return txn.Commit() //进行提交（注意是在提交的时候才会进行写入操作）
}
