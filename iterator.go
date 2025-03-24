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
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

type prefetchStatus uint8

const (
	prefetched prefetchStatus = iota + 1
)

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	key       []byte
	vptr      []byte
	val       []byte
	version   uint64
	expiresAt uint64

	slice *y.Slice // Used only during prefetching.
	next  *Item
	txn   *Txn

	err      error
	wg       sync.WaitGroup
	status   prefetchStatus
	meta     byte // We need to store meta to know about bitValuePointer.
	userMeta byte
}

// String returns a string representation of Item
func (item *Item) String() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)
}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please use KeyCopy.
func (item *Item) Key() []byte {
	return item.key
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.version
}

// Value retrieves the value of the item from the value log.
//
// This method must be called within a transaction. Calling it outside a
// transaction is considered undefined behavior. If an iterator is being used,
// then Item.Value() is defined in the current iteration only, because items are
// reused.
//
// If you need to use a value outside a transaction, please use Item.ValueCopy
// instead, or copy it yourself. Value might change once discard or commit is called.
// Use ValueCopy if you want to do a Set after Get.
func (item *Item) Value(fn func(val []byte) error) error {
	item.wg.Wait()
	if item.status == prefetched {
		if item.err == nil && fn != nil {
			if err := fn(item.val); err != nil {
				return err
			}
		}
		return item.err
	}
	buf, cb, err := item.yieldItemValue()
	defer runCallback(cb)
	if err != nil {
		return err
	}
	if fn != nil {
		return fn(buf)
	}
	return nil
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned. Tip: It might make sense to reuse the returned slice as dst argument for the next call.
//
// This function is useful in long running iterate/update transactions to avoid a write deadlock.
// See Github issue: https://github.com/dgraph-io/badger/issues/315
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	item.wg.Wait()
	if item.status == prefetched {
		return y.SafeCopy(dst, item.val), item.err
	}
	buf, cb, err := item.yieldItemValue()
	defer runCallback(cb)
	return y.SafeCopy(dst, buf), err
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	return true
}

// IsDeletedOrExpired returns true if item contains deleted or expired value.
func (item *Item) IsDeletedOrExpired() bool {
	return isDeletedOrExpired(item.meta, item.expiresAt)
}

// DiscardEarlierVersions returns whether the item was created with the
// option to discard earlier versions of a key when multiple are available.
func (item *Item) DiscardEarlierVersions() bool {
	return item.meta&bitDiscardEarlierVersions > 0
}

func (item *Item) yieldItemValue() ([]byte, func(), error) {
	key := item.Key()     // No need to copy.
	if !item.hasValue() { // 如果已经加载了，就不加载
		return nil, nil, nil
	}

	if item.slice == nil {
		item.slice = new(y.Slice)
	}

	if (item.meta & bitValuePointer) == 0 { //判断是否是kv分离的值
		//不是kv分离
		val := item.slice.Resize(len(item.vptr)) //直接把目标的Value切出来
		copy(val, item.vptr)
		return val, nil, nil
	}
	// 是kv分离
	var vp valuePointer
	vp.Decode(item.vptr) //解码值指针
	db := item.txn.db
	result, cb, err := db.vlog.Read(vp, item.slice) // 核心操作，去磁盘读目标value
	if err != nil {
		db.opt.Errorf("Unable to read: Key: %v, Version : %v, meta: %v, userMeta: %v"+
			" Error: %v", key, item.version, item.meta, item.userMeta, err)
		var txn *Txn
		if db.opt.managedTxns {
			txn = db.NewTransactionAt(math.MaxUint64, false)
		} else {
			txn = db.NewTransaction(false)
		}
		defer txn.Discard()

		iopt := DefaultIteratorOptions
		iopt.AllVersions = true
		iopt.InternalAccess = true
		iopt.PrefetchValues = false

		it := txn.NewKeyIterator(item.Key(), iopt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var vp valuePointer
			if item.meta&bitValuePointer > 0 {
				vp.Decode(item.vptr)
			}
			db.opt.Errorf("Key: %v, Version : %v, meta: %v, userMeta: %v valuePointer: %+v",
				item.Key(), item.version, item.meta, item.userMeta, vp)
		}
	}
	// Don't return error if we cannot read the value. Just log the error.
	return result, cb, nil
}

func runCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func (item *Item) prefetchValue() {
	val, cb, err := item.yieldItemValue() // 核心操作，获取item的value值
	defer runCallback(cb)

	item.err = err
	item.status = prefetched
	if val == nil {
		return
	}
	buf := item.slice.Resize(len(val))
	copy(buf, val)
	item.val = buf
}

// EstimatedSize returns the approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & bitValuePointer) == 0 {
		return int64(len(item.key) + len(item.vptr))
	}
	var vp valuePointer
	vp.Decode(item.vptr)
	return int64(vp.Len) // includes key length.
}

// KeySize returns the size of the key.
// Exact size of the key is key + 8 bytes of timestamp
func (item *Item) KeySize() int64 {
	return int64(len(item.key))
}

// ValueSize returns the approximate size of the value.
//
// This can be called to quickly estimate the size of a value without fetching
// it.
func (item *Item) ValueSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & bitValuePointer) == 0 {
		return int64(len(item.vptr))
	}
	var vp valuePointer
	vp.Decode(item.vptr)

	klen := int64(len(item.key) + 8) // 8 bytes for timestamp.
	// 6 bytes are for the approximate length of the header. Since header is encoded in varint, we
	// cannot find the exact length of header without fetching it.
	return int64(vp.Len) - klen - 6 - crc32.Size
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() byte {
	return item.userMeta
}

// ExpiresAt returns a Unix time value indicating when the item will be
// considered expired. 0 indicates that the item will never expire.
func (item *Item) ExpiresAt() uint64 {
	return item.expiresAt
}

// TODO: Switch this to use linked list container in Go.
type list struct {
	head *Item
	tail *Item
}

func (l *list) push(i *Item) {
	i.next = nil
	if l.tail == nil {
		l.head = i
		l.tail = i
		return
	}
	l.tail.next = i
	l.tail = i
}

func (l *list) pop() *Item {
	if l.head == nil {
		return nil
	}
	i := l.head
	if l.head == l.tail {
		l.tail = nil
		l.head = nil
	} else {
		l.head = i.next
	}
	i.next = nil
	return i
}

// IteratorOptions is used to set options when iterating over Badger key-value
// stores.
//
// This package provides DefaultIteratorOptions which contains options that
// should work for most applications. Consider using that as a starting point
// before customizing it for your own needs.
type IteratorOptions struct {
	// PrefetchSize is the number of KV pairs to prefetch while iterating.
	// Valid only if PrefetchValues is true.
	PrefetchSize int
	// PrefetchValues Indicates whether we should prefetch values during
	// iteration and store them.
	//PrefetchValues指示我们是否应该在迭代期间预取值并存储它们。
	PrefetchValues bool
	Reverse        bool // Direction of iteration. False is forward, true is backward.
	AllVersions    bool // Fetch all valid versions of the same key.
	InternalAccess bool // Used to allow internal access to badger keys.

	// The following option is used to narrow down the SSTables that iterator
	// picks up. If Prefix is specified, only tables which could have this
	// prefix are picked based on their range of keys.
	prefixIsKey bool   // If set, use the prefix for bloom filter lookup.
	Prefix      []byte // Only iterate over this given prefix.
	SinceTs     uint64 // Only read data that has version > SinceTs.
}

func (opt *IteratorOptions) compareToPrefix(key []byte) int {
	// We should compare key without timestamp. For example key - a[TS] might be > "aa" prefix.
	key = y.ParseKey(key)
	if len(key) > len(opt.Prefix) {
		key = key[:len(opt.Prefix)] //这个的意思是取key的前len(opt.Prefix)位
	}
	return bytes.Compare(key, opt.Prefix) // 两个前缀(字节切片)的比较，就是判断大小，如果a>b返回1,如果a<b返回-1，a=b返回0（a是前者，b是后者）
}

// 这个函数是为了挑选table，尽量减少需要遍历的table（用于0层筛选SST）
func (opt *IteratorOptions) pickTable(t table.TableInterface) bool {
	// Ignore this table if its max version is less than the sinceTs.
	if t.MaxVersion() < opt.SinceTs {
		return false
	}
	if len(opt.Prefix) == 0 { //如果遍历器没有指定前缀，那么所有table都需要遍历
		return true
	}
	//下面两个IF是判断是否在区间内（通过取table的最大最小key的前缀然后与IteratorOptions内的前缀Prefix比较）
	if opt.compareToPrefix(t.Smallest()) > 0 {
		return false
	}
	if opt.compareToPrefix(t.Biggest()) < 0 {
		return false
	}
	// 在区间内了，还需要利用布隆过滤器来判断一定不存在前缀key的table（DoesNotHave函数就是使用布隆判断的函数，其每个table都会有一个）
	// Bloom filter lookup would only work if opt.Prefix does NOT have the read
	// timestamp as part of the key.
	if opt.prefixIsKey && t.DoesNotHave(y.Hash(opt.Prefix)) {
		return false
	}
	return true
}

// pickTables picks the necessary table for the iterator. This function also assumes
// that the tables are sorted in the right order.
// 这个也是过滤一些不必遍历的table（用于非0层筛选SST）
func (opt *IteratorOptions) pickTables(all []*table.Table) []*table.Table {
	filterTables := func(tables []*table.Table) []*table.Table { //过滤的闭包函数，主要是判断SinceTs（查询目标的起始版本，只有版本比这个大的才可能会返回）
		if opt.SinceTs > 0 {
			tmp := tables[:0]
			for _, t := range tables {
				if t.MaxVersion() < opt.SinceTs {
					continue
				}
				tmp = append(tmp, t)
			}
			tables = tmp
		}
		return tables
	}

	if len(opt.Prefix) == 0 { //如果无前缀，直接走过滤的闭包函数
		out := make([]*table.Table, len(all))
		copy(out, all)
		return filterTables(out)
	}
	sIdx := sort.Search(len(all), func(i int) bool { //sort.Search是Go提供的结构体遍历，找到第一个表的最大值大于或等于prefix的下标并返回
		// table.Biggest >= opt.prefix
		// if opt.Prefix < table.Biggest, then surely it is not in any of the preceding tables.
		return opt.compareToPrefix(all[i].Biggest()) >= 0 //若当前表的最大值前len（prefix）位大于或等于prefix就为true，否则为false
	})
	if sIdx == len(all) {
		// Not found.
		return []*table.Table{}
	}

	filtered := all[sIdx:] // 判断出最大值，下面判断最小值
	if !opt.prefixIsKey {
		eIdx := sort.Search(len(filtered), func(i int) bool {
			return opt.compareToPrefix(filtered[i].Smallest()) > 0 //若当前表的最小值前len（prefix）大于prefix就为true，否则为false
		})
		out := make([]*table.Table, len(filtered[:eIdx]))
		copy(out, filtered[:eIdx])
		return filterTables(out) //这里的out最终得到的，就是一个数组，其内元素是最大值前len（prefix）位大于或等于prefix且其最小值前len（prefix）大于prefix的SST（因为判断的是前缀，所以有可能是会返回多个SST的）
	}
	// 下面的是前缀直接是完整key的话（用于遍历一个key的所有版本）
	// opt.prefixIsKey == true. This code is optimizing for opt.prefixIsKey part.
	var out []*table.Table
	hash := y.Hash(opt.Prefix)
	for _, t := range filtered {
		// When we encounter the first table whose smallest key is higher than opt.Prefix, we can
		// stop. This is an IMPORTANT optimization, just considering how often we call
		// NewKeyIterator.
		if opt.compareToPrefix(t.Smallest()) > 0 {
			// if table.Smallest > opt.Prefix, then this and all tables after this can be ignored.
			break
		}
		// opt.Prefix is actually the key. So, we can run bloom filter checks
		// as well.
		if t.DoesNotHave(hash) {
			continue
		}
		out = append(out, t)
	}
	return filterTables(out)
}

// DefaultIteratorOptions contains default options when iterating over Badger key-value stores.
var DefaultIteratorOptions = IteratorOptions{
	PrefetchValues: true,
	PrefetchSize:   100,
	Reverse:        false,
	AllVersions:    false,
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	iitr   y.Iterator
	txn    *Txn
	readTs uint64

	opt   IteratorOptions
	item  *Item
	data  list
	waste list

	lastKey []byte // Used to skip over multiple versions of the same key.

	closed  bool
	scanned int // Used to estimate the size of data scanned by iterator. //用于估计迭代器扫描的数据大小。

	// ThreadId is an optional value that can be set to identify which goroutine created
	// the iterator. It can be used, for example, to uniquely identify each of the
	// iterators created by the stream interface
	ThreadId int

	Alloc *z.Allocator
}

// NewIterator returns a new iterator. Depending upon the options, either only keys, or both
// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
// Using prefetch is recommended if you're doing a long running iteration, for performance.
//
// Multiple Iterators:
// For a read-only txn, multiple iterators can be running simultaneously. However, for a read-write
// txn, iterators have the nuance of being a snapshot of the writes for the transaction at the time
// iterator was created. If writes are performed after an iterator is created, then that iterator
// will not be able to see those writes. Only writes performed before an iterator was created can be
// viewed.

// badger 继承了 leveldb 中 iterator 组合的思想，把 pendingWrites 的读取链路封装为了 Iterator，
// 并与 MemTableIterator、TableIterator 等 Iterator 通过 MergeIterator 组合为最终的 Iterator
// badger 会将 commitTs 作为 key 的后缀存储到 LSM Tree 中，Iterator 在迭代中也会对时间戳有感知，按 readTs 时刻的快照数据进行迭代。
func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	if txn.discarded {
		panic(ErrDiscardedTxn)
	}
	if txn.db.IsClosed() {
		panic(ErrDBClosed)
	}

	y.NumIteratorsCreatedAdd(txn.db.opt.MetricsEnabled, 1)

	// Keep track of the number of active iterators.
	txn.numIterators.Add(1)

	// TODO: If Prefix is set, only pick those memtables which have keys with the prefix.
	tables, decr := txn.db.getMemTables() // 返回memtable与immemtable的合集
	defer decr()
	txn.db.vlog.incrIteratorCount()                                   // 内存numActiveIterators，这个数字一旦为0，就可以删除当前vLog对象（就本行的那个vlog）内的ToBeDeleted文件。
	var iters []y.Iterator                                            //创建迭代器数组
	if itr := txn.newPendingWritesIterator(opt.Reverse); itr != nil { //先创建 用于遍历存储当前事务待写入数据的数组的 迭代器
		iters = append(iters, itr)
	}
	for i := 0; i < len(tables); i++ { //为内存里面的每一个memtable及immemtable的跳表创建一个迭代器
		iters = append(iters, tables[i].sl.NewUniIterator(opt.Reverse))
	}
	// lc是层级管理器(levelcontroler)，下面是对每一层创建一个迭代器，并加在iters对象内（注意0层与其他层处理方式不同）
	iters = txn.db.lc.appendIterators(iters, &opt) // This will increment references.
	res := &Iterator{
		txn:    txn,
		iitr:   table.NewMergeIterator(iters, opt.Reverse), // 合并的迭代器（二叉树结构），包含事物的，内存的，外存的（按level展开）
		opt:    opt,
		readTs: txn.readTs,
	}
	return res
}

// NewKeyIterator is just like NewIterator, but allows the user to iterate over all versions of a
// single key. Internally, it sets the Prefix option in provided opt, and uses that prefix to
// additionally run bloom filter lookups before picking tables from the LSM tree.
// NewKeyIterator与NewIterator类似，但允许用户迭代单个键的所有版本。
// 在内部，它在提供的opt中设置Prefix选项，并在从LSM树中拾取表之前使用该前缀额外运行布隆过滤器查找
func (txn *Txn) NewKeyIterator(key []byte, opt IteratorOptions) *Iterator {
	if len(opt.Prefix) > 0 {
		panic("opt.Prefix should be nil for NewKeyIterator.")
	}
	opt.Prefix = key // This key must be without the timestamp.
	opt.prefixIsKey = true
	opt.AllVersions = true
	return txn.NewIterator(opt)
}

func (it *Iterator) newItem() *Item {
	item := it.waste.pop()
	if item == nil {
		item = &Item{slice: new(y.Slice), txn: it.txn}
	}
	return item
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
// Item返回指向当前键值对的指针。
// 此项仅在调用.Next（）之前有效。
func (it *Iterator) Item() *Item {
	tx := it.txn
	tx.addReadKey(it.item.Key())
	return it.item
}

// Valid returns false when iteration is done.
// 迭代完成后，Valid返回false。
func (it *Iterator) Valid() bool {
	if it.item == nil {
		return false
	}
	if it.opt.prefixIsKey {
		return bytes.Equal(it.item.key, it.opt.Prefix)
	}
	return bytes.HasPrefix(it.item.key, it.opt.Prefix)
}

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	if it.iitr == nil {
		it.txn.numIterators.Add(-1)
		return
	}

	it.iitr.Close()
	// It is important to wait for the fill goroutines to finish. Otherwise, we might leave zombie
	// goroutines behind, which are waiting to acquire file read locks after DB has been closed.
	waitFor := func(l list) {
		item := l.pop()
		for item != nil {
			item.wg.Wait()
			item = l.pop()
		}
	}
	waitFor(it.waste)
	waitFor(it.data)

	// TODO: We could handle this error.
	_ = it.txn.db.vlog.decrIteratorCount()
	it.txn.numIterators.Add(-1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
// 接下来，将迭代器前进一步。始终在Next（）后检查it.Valid（），以确保您可以访问有效的it.Item（）。
func (it *Iterator) Next() {
	if it.iitr == nil {
		return
	}
	// Reuse current item
	it.item.wg.Wait()                                                         // Just cleaner to wait before pushing to avoid doing ref counting.//只需在推之前等待清洁，以避免进行引用计数。
	it.scanned += len(it.item.key) + len(it.item.val) + len(it.item.vptr) + 2 //累计扫描的大小
	it.waste.push(it.item)                                                    //将当前扫描过的都放到waste里面

	// Set next item to current
	it.item = it.data.pop()
	for it.iitr.Valid() && hasPrefix(it) {
		if it.parseItem() {
			// parseItem calls one extra next.
			// This is used to deal with the complexity of reverse iteration.
			break
		}
	}
}

func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&bitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

// parseItem is a complex function because it needs to handle both forward and reverse iteration
// implementation. We store keys such that their versions are sorted in descending order. This makes
// forward iteration efficient, but revese iteration complicated. This tradeoff is better because
// forward iteration is more common than reverse. It returns true, if either the iterator is invalid
// or it has pushed an item into it.data list, else it returns false.
//
// This function advances the iterator.
// parseItem（中文为分析item）是一个复杂的函数，因为它需要处理正向和反向迭代实现。我们存储key，使其版本按降序排列。
// 这使得正向迭代高效，但揭示了迭代的复杂性。这种权衡更好，因为正向迭代比反向迭代更常见。如果迭代器无效或已将项推入.data列表，则返回true，否则返回false。
// 此函数用于推进迭代器。
// PS：在预读取时，注意这个函数每次预读取出来一个KV对，若无效，在调用此函数的那里舍弃掉，若有效就返回计数。
// 而也正因此，每次的lastKey只是代表一个上一个取出的key的单值（不是切片），因为每次取出来之后lastKey就在当轮没用了
func (it *Iterator) parseItem() bool {
	mi := it.iitr
	key := mi.Key()

	setItem := func(item *Item) { // 得到有效KV对，放到迭代器的链表中
		if it.item == nil {
			it.item = item
		} else {
			it.data.push(item) // 设置到迭代器的链表里，方便 NOTE: 0 处遍历
		}
	}

	// 下面这行还有下面这个if都是为了跳过badger内部的key
	isInternalKey := bytes.HasPrefix(key, badgerPrefix)
	// Skip badger keys.
	if !it.opt.InternalAccess && isInternalKey {
		mi.Next()
		return false
	}

	// Skip any versions which are beyond the readTs.
	version := y.ParseTs(key)
	// Ignore everything that is above the readTs and below or at the sinceTs.
	if version > it.readTs || (it.opt.SinceTs > 0 && version <= it.opt.SinceTs) { //如果目标key的版本号（时间戳）大于当前查询事务的版本号，说明这是在此迭代器开始之后写入的key不能被读取
		mi.Next()
		return false
	}

	// Skip banned keys only if it does not have badger internal prefix.
	// 跳过被禁用的key，仅当它不是badger内部KV
	if !isInternalKey && it.txn.db.isBanned(key) != nil {
		mi.Next()
		return false
	}

	if it.opt.AllVersions { // 如果指定返回所有版本的key则无论迭代到什么key都直接返回
		// Return deleted or expired values also, otherwise user can't figure out
		// whether the key was deleted.
		//还返回已删除或过期的值，否则用户无法确定key是否已删除。（不包括当前事务之后创建的key）
		item := it.newItem()
		it.fill(item)
		setItem(item)
		mi.Next()
		return true
	}

	// If iterating in forward direction, then just checking the last key against current key would
	// be sufficient.
	// 如果向前迭代，那么只需将最后一个键与当前键进行比较就足够了。
	// PS：应该是只要取出来第一个key就可以了，因为这个是最新的
	if !it.opt.Reverse {
		if y.SameKey(it.lastKey, key) { //若是老版本的已经取出来的key，就排除（在前面的轮询中已经读出来新版的key了)
			mi.Next()
			return false
		}
		// Only track in forward direction.
		// We should update lastKey as soon as we find a different key in our snapshot.
		// Consider keys: a 5, b 7 (del), b 5. When iterating, lastKey = a.
		// Then we see b 7, which is deleted. If we don't store lastKey = b, we'll then return b 5,
		// which is wrong. Therefore, update lastKey here.
		//只跟踪前进方向。
		//我们应该在快照中找到不同的密钥后立即更新lastKey。
		//考虑键：a 5，b 7（del），b 5。迭代时，lastKey=a。
		//然后我们看到b7，它被删除了。如果我们不存储lastKey=b，那么我们将返回b5，
		//这是错误的。因此，请在此处更新lastKey。
		// PS：就是后增加的被删掉了，只剩一个老版本的，这个老版本的也不应该被读出来
		it.lastKey = y.SafeCopy(it.lastKey, mi.Key()) //it.lastKey用来记录当前查询到的key，便于之后的比较版本等
	}

FILL:
	// If deleted, advance and return.
	vs := mi.Value()
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) { //解析value的值，并从中判断是否过期与删除
		mi.Next()
		return false
	}

	item := it.newItem()
	it.fill(item) //获取item
	// fill item based on current cursor position. All Next calls have returned, so reaching here
	// means no Next was called.
	// 根据当前光标位置填充项目。所有Next调用都已返回，因此到达此处意味着没有调用Next。

	mi.Next()                           //判断下一个                           // Advance but no fill item yet.
	if !it.opt.Reverse || !mi.Valid() { // Forward direction, or invalid.
		setItem(item)
		return true
	}

	// Reverse direction.
	nextTs := y.ParseTs(mi.Key())
	mik := y.ParseKey(mi.Key())
	if nextTs <= it.readTs && bytes.Equal(mik, item.key) {
		// This is a valid potential candidate.
		goto FILL
	}
	// Ignore the next candidate. Return the current one.
	setItem(item)
	return true
}

func (it *Iterator) fill(item *Item) {
	vs := it.iitr.Value()
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.expiresAt = vs.ExpiresAt

	item.version = y.ParseTs(it.iitr.Key())
	item.key = y.SafeCopy(item.key, y.ParseKey(it.iitr.Key()))

	item.vptr = y.SafeCopy(item.vptr, vs.Value) // 值指针对象
	item.val = nil
	if it.opt.PrefetchValues { //PrefetchValues指示我们是否应该在迭代期间预取值并存储它们。
		item.wg.Add(1) //这里是go语言的并发控制的一个组件
		go func() {    // 创建一个协程异步的读取vlog文件
			// FIXME we are not handling errors here.
			item.prefetchValue() //从vlog里面去拿
			item.wg.Done()       //这里就是前面有个wg.wait的地方 NOTE:0 处
		}()
	}
}

func hasPrefix(it *Iterator) bool {
	// We shouldn't check prefix in case the iterator is going in reverse. Since in reverse we expect
	// people to append items to the end of prefix.
	if !it.opt.Reverse && len(it.opt.Prefix) > 0 {
		return bytes.HasPrefix(y.ParseKey(it.iitr.Key()), it.opt.Prefix)
	}
	return true
}

func (it *Iterator) prefetch() {
	prefetchSize := 2
	if it.opt.PrefetchValues && it.opt.PrefetchSize > 1 {
		prefetchSize = it.opt.PrefetchSize
	}

	i := it.iitr
	var count int
	it.item = nil
	for i.Valid() && hasPrefix(it) {
		if !it.parseItem() { //核心操作，对单个KV对进行分析
			continue //读出来无效的kv对，如badger的内部KV以及版本不对的，则不计数
		}
		count++
		if count == prefetchSize { //读出来的有效个数达到设置的预取上限
			break
		}
	}
}

// Seek would seek to the provided key if present. If absent, it would seek to the next
// smallest key greater than the provided key if iterating in the forward direction.
// Behavior would be reversed if iterating backwards.
// 如果存在，Seek将查找提供的key。如果不存在，如果向前迭代，它将寻求比提供的键大的下一个最小键。如果向后迭代，行为将发生逆转。
// 如果len(key) = 0，那么就代表是初始化操作
func (it *Iterator) Seek(key []byte) {
	if it.iitr == nil {
		return
	}
	if len(key) > 0 { //非初始化，就是读
		it.txn.addReadKey(key)
	}
	for i := it.data.pop(); i != nil; i = it.data.pop() { // it.data是一个链表，存的是预读取的数据
		i.wg.Wait() //做一个小范围的异步，等待vlog中的值读到内存 NOTE:0
		it.waste.push(i)
	}

	it.lastKey = it.lastKey[:0] // [:0] 操作会创建一个新的切片，这个新切片与原切片共享底层数组（即原始数据实际还在），但新切片的长度为 0，容量不变。
	if len(key) == 0 {          //如果是初始化，则将prefix设置为要搜索的key
		key = it.opt.Prefix
	}
	if len(key) == 0 {
		it.iitr.Rewind() //调整迭代器的平衡二叉树，保证range key是从小到大的顺序读取的
		it.prefetch()    //核心操作，预取操作，真的要读了
		return
	}

	if !it.opt.Reverse {
		key = y.KeyWithTs(key, it.txn.readTs)
	} else {
		key = y.KeyWithTs(key, 0)
	}
	it.iitr.Seek(key)
	it.prefetch()
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
// Rewind会将迭代器光标一直倒退到第零个位置，如果向前迭代，这将是最小的键，如果向后迭代，则是最大的键。
// 它不会跟踪光标是否以Seek（）开头。
func (it *Iterator) Rewind() {
	it.Seek(nil) //查找一个不存在的key就把指针指向0，并且做一些初始化操作
}
