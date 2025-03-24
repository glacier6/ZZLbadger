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
	"encoding/binary"
	stderrors "errors"
	"expvar"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	badgerPrefix = []byte("!badger!")       // Prefix for internal keys used by badger.
	txnKey       = []byte("!badger!txn")    // For indicating end of entries in txn.
	bannedNsKey  = []byte("!badger!banned") // For storing the banned namespaces.
)

type closers struct {
	updateSize  *z.Closer
	compactors  *z.Closer
	memtable    *z.Closer
	writes      *z.Closer
	valueGC     *z.Closer
	pub         *z.Closer
	cacheHealth *z.Closer
}

type lockedKeys struct {
	sync.RWMutex
	keys map[uint64]struct{}
}

func (lk *lockedKeys) add(key uint64) {
	lk.Lock()
	defer lk.Unlock()
	lk.keys[key] = struct{}{}
}

func (lk *lockedKeys) has(key uint64) bool {
	lk.RLock()
	defer lk.RUnlock()
	_, ok := lk.keys[key]
	return ok
}

func (lk *lockedKeys) all() []uint64 {
	lk.RLock()
	defer lk.RUnlock()
	keys := make([]uint64, 0, len(lk.keys))
	for key := range lk.keys {
		keys = append(keys, key)
	}
	return keys
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	testOnlyDBExtensions

	lock sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers closers

	mt  *memTable   // Our latest (actively written) in-memory table
	imm []*memTable // Add here only AFTER pushing to flushChan.

	// Initialized via openMemTables.
	nextMemFid int

	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	writeCh   chan *request
	flushChan chan *memTable // For flushing memtables.
	closeOnce sync.Once      // For closing DB only once.

	blockWrites atomic.Int32
	isClosed    atomic.Uint32

	orc              *oracle
	bannedNamespaces *lockedKeys
	threshold        *vlogThreshold

	pub        *publisher
	registry   *KeyRegistry
	blockCache *ristretto.Cache[[]byte, *table.Block]   //块缓存
	indexCache *ristretto.Cache[uint64, *fb.TableIndex] // 索引缓存
	allocPool  *z.AllocatorPool
}

const (
	kvWriteChCapacity = 1000
)

func checkAndSetOptions(opt *Options) error {
	// It's okay to have zero compactors which will disable all compactions but
	// we cannot have just one compactor otherwise we will end up with all data
	// on level 2.
	if opt.NumCompactors == 1 {
		return errors.New("Cannot have 1 compactor. Need at least 2")
	}

	if opt.InMemory && (opt.Dir != "" || opt.ValueDir != "") {
		return errors.New("Cannot use badger in Disk-less mode with Dir or ValueDir set")
	}
	opt.maxBatchSize = (15 * opt.MemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	// This is the maximum value, vlogThreshold can have if dynamic thresholding is enabled.
	opt.maxValueThreshold = math.Min(maxValueThreshold, float64(opt.maxBatchSize))
	if opt.VLogPercentile < 0.0 || opt.VLogPercentile > 1.0 {
		return errors.New("vlogPercentile must be within range of 0.0-1.0")
	}

	// We are limiting opt.ValueThreshold to maxValueThreshold for now.
	if opt.ValueThreshold > maxValueThreshold {
		return errors.Errorf("Invalid ValueThreshold, must be less or equal to %d",
			maxValueThreshold)
	}

	// If ValueThreshold is greater than opt.maxBatchSize, we won't be able to push any data using
	// the transaction APIs. Transaction batches entries into batches of size opt.maxBatchSize.
	if opt.ValueThreshold > opt.maxBatchSize {
		return errors.Errorf("Valuethreshold %d greater than max batch size of %d. Either "+
			"reduce opt.ValueThreshold or increase opt.BaseTableSize.",
			opt.ValueThreshold, opt.maxBatchSize)
	}
	// ValueLogFileSize should be strictly LESS than 2<<30 otherwise we will
	// overflow the uint32 when we mmap it in OpenMemtable.
	// ValueLogFileSize应严格小于2<<30，否则我们在OpenMemtable中mmap uint32时会溢出uint32。
	if !(opt.ValueLogFileSize < 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return ErrValueLogSize
	}

	if opt.ReadOnly {
		// Do not perform compaction in read only mode.
		opt.CompactL0OnClose = false
	}

	needCache := (opt.Compression != options.None) || (len(opt.EncryptionKey) > 0)
	if needCache && opt.BlockCacheSize == 0 {
		panic("BlockCacheSize should be set since compression/encryption are enabled")
	}
	return nil
}

// 打开并返回一个DB对象 *DB（核心对象）
func Open(opt Options) (*DB, error) {
	if err := checkAndSetOptions(&opt); err != nil {
		return nil, err
	}
	var dirLockGuard, valueDirLockGuard *directoryLockGuard //目录锁，防止DB启用需要用的目录后，其余进程也注册到这个目录,dirLockGuard是LSM树的文件锁，valueDirLockGuard是Vlog文件锁

	// Create directories and acquire lock on it only if badger is not running in InMemory mode.
	// We don't have any directories/files in InMemory mode so we don't need to acquire
	// any locks on them.
	// 如果非纯内存运行
	if !opt.InMemory {
		if err := createDirs(opt); err != nil { //创建目录
			return nil, err
		}
		var err error
		if !opt.BypassLockGuard { // 这if里面是具体的去获得那两个锁
			dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
			if err != nil {
				return nil, err
			}
			defer func() {
				if dirLockGuard != nil {
					_ = dirLockGuard.release()
				}
			}()
			absDir, err := filepath.Abs(opt.Dir) //得到创建的LSM树目录的绝对路径
			if err != nil {
				return nil, err
			}
			absValueDir, err := filepath.Abs(opt.ValueDir) //得到创建的Vlog的绝对路径
			if err != nil {
				return nil, err
			}
			if absValueDir != absDir {
				valueDirLockGuard, err = acquireDirectoryLock(opt.ValueDir, lockFile, opt.ReadOnly)
				if err != nil {
					return nil, err
				}
				defer func() {
					if valueDirLockGuard != nil {
						_ = valueDirLockGuard.release()
					}
				}()
			}
		}
	}

	manifestFile, manifest, err := openOrCreateManifestFile(opt) //打开并创建清单文件（存储每层level有哪些sstable文件及其KEY域）
	if err != nil {
		return nil, err
	}
	// go的钩子语法，当open退出的时候，会释放掉manifestFIle
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	db := &DB{
		imm:           make([]*memTable, 0, opt.NumMemtables), //内存表的切片数组
		flushChan:     make(chan *memTable, opt.NumMemtables), //刷请请求的channel
		writeCh:       make(chan *request, kvWriteChCapacity), //写请求的channel
		opt:           opt,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,      //LSM目录锁
		valueDirGuard: valueDirLockGuard, //VLog目录锁
		orc:           newOracle(opt),    //KV引擎的并发事物的管理器，分配事务的版本号，Badger实现的是MVCC方式，然后通过Oracle来管理，维护了两个小顶堆，一个提交时间戳，一个读时间戳，只读的话只用后者，而UPDATE二者全用
		//而这两个时间戳维护也用了watermark（水位）的概念
		pub:              newPublisher(),
		allocPool:        z.NewAllocatorPool(8),
		bannedNamespaces: &lockedKeys{keys: make(map[uint64]struct{})},
		threshold:        initVlogThreshold(&opt),
	}

	db.syncChan = opt.syncChan // 这个只用于测试

	// Cleanup all the goroutines started by badger in case of an error.
	defer func() {
		if err != nil {
			opt.Errorf("Received err: %v. Cleaning up...", err)
			db.cleanup()
			db = nil
		}
	}()

	if opt.BlockCacheSize > 0 { //如果允许有块缓存（PS：SSTable内部是按照块分割的），注意badger在运行的时候也可以更改块大小
		numInCache := opt.BlockCacheSize / int64(opt.BlockSize) //得到有多少块缓存 （块缓存总大小/单块大小）
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config[[]byte, *table.Block]{ //ristretto是Dgraph社区实现的支持并发高性能的缓存库
			NumCounters: numInCache * 8,     //缓存块数量
			MaxCost:     opt.BlockCacheSize, //缓存空间大小
			BufferItems: 64,
			Metrics:     true,
			OnExit:      table.BlockEvictHandler,
		}
		db.blockCache, err = ristretto.NewCache[[]byte, *table.Block](&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create data cache")
		}
	}

	if opt.IndexCacheSize > 0 { // 如果允许有索引缓存，每个SSTable开头会有一个索引块，方便快速的可以对KEY进行二分查找，而这个索引缓存就是迅速定位当前的key在哪个SSTable
		// Index size is around 5% of the table size.
		indexSz := int64(float64(opt.MemTableSize) * 0.05)
		numInCache := opt.IndexCacheSize / indexSz
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config[uint64, *fb.TableIndex]{
			NumCounters: numInCache * 8,
			MaxCost:     opt.IndexCacheSize,
			BufferItems: 64,
			Metrics:     true,
		}
		db.indexCache, err = ristretto.NewCache(&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create bf cache")
		}
	}

	db.closers.cacheHealth = z.NewCloser(1)
	go db.monitorCache(db.closers.cacheHealth) //对块缓存与索引缓存的监控，会输出使用率

	if db.opt.InMemory {
		db.opt.SyncWrites = false
		// If badger is running in memory mode, push everything into the LSM Tree.
		db.opt.ValueThreshold = math.MaxInt32
	}
	krOpt := KeyRegistryOptions{ //key的注册，与并发事务相关
		ReadOnly:                      opt.ReadOnly,
		Dir:                           opt.Dir,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
		InMemory:                      opt.InMemory,
	}

	if db.registry, err = OpenKeyRegistry(krOpt); err != nil {
		return db, err
	}
	db.calculateSize() // 计算vlog和sst文件的大小，并将其存储在y.LSMSize和y.VlogSize中。
	db.closers.updateSize = z.NewCloser(1)
	go db.updateSize(db.closers.updateSize) // 定时去计算vlog和sst文件的大小，并将其存储在y.LSMSize和y.VlogSize中。

	if err := db.openMemTables(db.opt); err != nil { //应该打开的是上一次系统关闭时遗留的一些在Memtable中的数据，注意打开后，直接把这些上次遗留的放到Immemtable
		return nil, y.Wrapf(err, "while opening memtables")
	}

	if !db.opt.ReadOnly {
		if db.mt, err = db.newMemTable(); err != nil { //创建Memtable，这里有个db.openMemTable，注意与上面的少一个s，这个函数主管创建，即之后要使用的MemTable
			return nil, y.Wrapf(err, "cannot create memtable")
		}
	}

	// 新建一个LevelsController（维护LSM树的level，做日志归并，压缩处理之类的）
	// 就是打开各个已有的SST，加载元数据块，索引块等至内存中
	// newLevelsController里有每层一个的levelHandler，这个里面有存每层的tables
	if db.lc, err = newLevelsController(db, &manifest); err != nil { // 利用清单做初始信息配置
		return db, err
	}

	// Initialize vlog struct.
	// 初始化Vlog，就是在外存打开一个DISCARD的文件（用于GC）并关联为mmap文件
	db.vlog.init(db)

	if !opt.ReadOnly {
		// 下面是启动LSM Tree的日志归并的协程
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors) //启动日志合并

		db.closers.memtable = z.NewCloser(1)
		go func() {
			db.flushMemtable(db.closers.memtable) // Need levels controller to be up. flushMemtable执行的是将memtable刷到磁盘L0层
			//内有handleMemTableFlush是做将memtable中的数据刷入磁盘L0层操作的函数
		}()
		// Flush them to disk asap.
		for _, mt := range db.imm { //刷新immemtable到磁盘
			db.flushChan <- mt
		}
	}
	// We do increment nextTxnTs below. So, no need to do it here.
	db.orc.nextTxnTs = db.MaxVersion()                    //获得当前最大事务的时间戳（最大版本号），通过遍历imm以及等一系列会存储最大版本号的数据
	db.opt.Infof("Set nextTxnTs to %d", db.orc.nextTxnTs) //打印日志

	if err = db.vlog.open(db); err != nil { // 打开并初始化所有vlog文件
		return db, y.Wrapf(err, "During db.vlog.open")
	}

	//下面这三行和事务相关，DB在恢复事务
	// Let's advance nextTxnTs to one more than whatever we observed via
	// replaying the logs.
	//让我们将nextTxnTs提前到比我们通过回放日志观察到的多一个。
	db.orc.txnMark.Done(db.orc.nextTxnTs)
	// In normal mode, we must update readMark so older versions of keys can be removed during
	// compaction when run in offline mode via the flatten tool.
	// 在正常模式下，我们必须更新readMark，以便在脱机模式下通过展开工具运行时，可以在压缩过程中删除旧版本的key。
	db.orc.readMark.Done(db.orc.nextTxnTs)
	db.orc.incrementNextTs()

	go db.threshold.listenForValueThresholdUpdate()

	if err := db.initBannedNamespaces(); err != nil {
		return db, errors.Wrapf(err, "While setting banned keys")
	}

	db.closers.writes = z.NewCloser(1)
	go db.doWrites(db.closers.writes) //创建一个处理写请求的协程

	if !db.opt.InMemory { //开启GC
		db.closers.valueGC = z.NewCloser(1)
		go db.vlog.waitOnGC(db.closers.valueGC)
	}

	db.closers.pub = z.NewCloser(1)
	go db.pub.listenForUpdates(db.closers.pub) // 这个是性能提升的一个点

	//下面是释放锁
	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// initBannedNamespaces retrieves the banned namespaces from the DB and updates in-memory structure.
func (db *DB) initBannedNamespaces() error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	return db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = bannedNsKey
		iopts.PrefetchValues = false
		iopts.InternalAccess = true
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			key := y.BytesToU64(itr.Item().Key()[len(bannedNsKey):])
			db.bannedNamespaces.add(key)
		}
		return nil
	})
}

func (db *DB) MaxVersion() uint64 {
	var maxVersion uint64
	update := func(a uint64) {
		if a > maxVersion {
			maxVersion = a
		}
	}
	db.lock.Lock()
	// In read only mode, we do not create new mem table.
	if !db.opt.ReadOnly {
		update(db.mt.maxVersion)
	}
	for _, mt := range db.imm {
		update(mt.maxVersion)
	}
	db.lock.Unlock()
	for _, ti := range db.Tables() {
		update(ti.MaxVersion)
	}
	return maxVersion
}

func (db *DB) monitorCache(c *z.Closer) {
	defer c.Done()
	count := 0
	analyze := func(name string, metrics *ristretto.Metrics) {
		// If the mean life expectancy is less than 10 seconds, the cache
		// might be too small.
		le := metrics.LifeExpectancySeconds()
		if le == nil {
			return
		}
		lifeTooShort := le.Count > 0 && float64(le.Sum)/float64(le.Count) < 10
		hitRatioTooLow := metrics.Ratio() > 0 && metrics.Ratio() < 0.4
		if lifeTooShort && hitRatioTooLow {
			db.opt.Warningf("%s might be too small. Metrics: %s\n", name, metrics)
			db.opt.Warningf("Cache life expectancy (in seconds): %+v\n", le)

		} else if le.Count > 1000 && count%5 == 0 {
			db.opt.Infof("%s metrics: %s\n", name, metrics)
		}
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-ticker.C:
		}

		analyze("Block cache", db.BlockCacheMetrics())
		analyze("Index cache", db.IndexCacheMetrics())
		count++
	}
}

// cleanup stops all the goroutines started by badger. This is used in open to
// cleanup goroutines in case of an error.
func (db *DB) cleanup() {
	db.stopMemoryFlush()
	db.stopCompactions()

	db.blockCache.Close()
	db.indexCache.Close()
	if db.closers.updateSize != nil {
		db.closers.updateSize.Signal()
	}
	if db.closers.valueGC != nil {
		db.closers.valueGC.Signal()
	}
	if db.closers.writes != nil {
		db.closers.writes.Signal()
	}
	if db.closers.pub != nil {
		db.closers.pub.Signal()
	}

	db.orc.Stop()

	// Do not use vlog.Close() here. vlog.Close truncates the files. We don't
	// want to truncate files unless the user has specified the truncate flag.
}

// BlockCacheMetrics returns the metrics for the underlying block cache.
func (db *DB) BlockCacheMetrics() *ristretto.Metrics {
	if db.blockCache != nil {
		return db.blockCache.Metrics
	}
	return nil
}

// IndexCacheMetrics returns the metrics for the underlying index cache.
func (db *DB) IndexCacheMetrics() *ristretto.Metrics {
	if db.indexCache != nil {
		return db.indexCache.Metrics
	}
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.close()
	})
	return err
}

// IsClosed denotes if the badger DB is closed or not. A DB instance should not
// be used after closing it.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) close() (err error) {
	defer db.allocPool.Release()

	db.opt.Debugf("Closing database")
	db.opt.Infof("Lifetime L0 stalled for: %s\n", time.Duration(db.lc.l0stallsMs.Load()))

	db.blockWrites.Store(1)
	db.isClosed.Store(1)

	if !db.opt.InMemory {
		// Stop value GC first.
		db.closers.valueGC.SignalAndWait()
	}

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Don't accept any more write.
	close(db.writeCh)

	db.closers.pub.SignalAndWait()
	db.closers.cacheHealth.Signal()

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if db.mt != nil {
		if db.mt.sl.Empty() {
			// Remove the memtable if empty.
			db.mt.DecrRef()
		} else {
			db.opt.Debugf("Flushing memtable")
			for {
				pushedMemTable := func() bool {
					db.lock.Lock()
					defer db.lock.Unlock()
					y.AssertTrue(db.mt != nil)
					select {
					case db.flushChan <- db.mt:
						db.imm = append(db.imm, db.mt) // Flusher will attempt to remove this from s.imm.
						db.mt = nil                    // Will segfault if we try writing!
						db.opt.Debugf("pushed to flush chan\n")
						return true
					default:
						// If we fail to push, we need to unlock and wait for a short while.
						// The flushing operation needs to update s.imm. Otherwise, we have a
						// deadlock.
						// TODO: Think about how to do this more cleanly, maybe without any locks.
					}
					return false
				}()
				if pushedMemTable {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	db.stopMemoryFlush()
	db.stopCompactions()

	// Force Compact L0
	// We don't need to care about cstatus since no parallel compaction is running.
	if db.opt.CompactL0OnClose {
		err := db.lc.doCompact(173, compactionPriority{level: 0, score: 1.73})
		switch err {
		case errFillTables:
			// This error only means that there might be enough tables to do a compaction. So, we
			// should not report it to the end user to avoid confusing them.
		case nil:
			db.opt.Debugf("Force compaction on level 0 done")
		default:
			db.opt.Warningf("While forcing compaction on level 0: %v", err)
		}
	}

	// Now close the value log.
	if vlogErr := db.vlog.Close(); vlogErr != nil {
		err = y.Wrap(vlogErr, "DB.Close")
	}

	db.opt.Infof(db.LevelsToString())
	if lcErr := db.lc.close(); err == nil {
		err = y.Wrap(lcErr, "DB.Close")
	}
	db.opt.Debugf("Waiting for closer")
	db.closers.updateSize.SignalAndWait()
	db.orc.Stop()
	db.blockCache.Close()
	db.indexCache.Close()

	db.threshold.close()

	if db.opt.InMemory {
		return
	}

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = y.Wrap(manifestErr, "DB.Close")
	}
	if registryErr := db.registry.Close(); err == nil {
		err = y.Wrap(registryErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := db.syncDir(db.opt.Dir); err == nil {
		err = y.Wrap(syncErr, "DB.Close")
	}
	if syncErr := db.syncDir(db.opt.ValueDir); err == nil {
		err = y.Wrap(syncErr, "DB.Close")
	}

	return err
}

// VerifyChecksum verifies checksum for all tables on all levels.
// This method can be used to verify checksum, if opt.ChecksumVerificationMode is NoVerification.
func (db *DB) VerifyChecksum() error {
	return db.lc.verifyChecksum()
}

const (
	lockFile = "LOCK"
)

// Sync syncs database content to disk. This function provides
// more control to user to sync data whenever required.
func (db *DB) Sync() error {
	/**
	Make an attempt to sync both the logs, the active memtable's WAL and the vLog (1847).
	Cases:
	- All_ok			:: If both the logs sync successfully.

	- Entry_Lost		:: If an entry with a value pointer was present in the active memtable's WAL,
						:: and the WAL was synced but there was an error in syncing the vLog.
						:: The entry will be considered lost and this case will need to be handled during recovery.

	- Entries_Lost		:: If there were errors in syncing both the logs, multiple entries would be lost.

	- Entries_Lost      :: If the active memtable's WAL is not synced but the vLog is synced, it will
						:: result in entries being lost because recovery of the active memtable is done from its WAL.
						:: Check `UpdateSkipList` in memtable.go.

	- Nothing_lost		:: If an entry with its value was present in the active memtable's WAL, and the WAL was synced,
						:: but there was an error in syncing the vLog.
						:: Nothing is lost for this very specific entry because the entry is completely present in the memtable's WAL.

	- Partially_lost    :: If entries were written partially in either of the logs,
						:: the logs will be truncated during recovery.
						:: As a result of truncation, some entries might be lost.
					    :: Assume that 4KB of data is to be synced and invoking `Sync` results only in syncing 3KB
	                    :: of data and then the machine shuts down or the disk failure happens,
						:: this will result in partial writes. [[This case needs verification]]
	*/
	db.lock.RLock()
	memtableSyncError := db.mt.SyncWAL()
	db.lock.RUnlock()

	vLogSyncError := db.vlog.sync()
	return y.CombineErrors(memtableSyncError, vLogSyncError)
}

// getMemtables returns the current memtables and get references.
// getMemTables会返回memtable与immemtable的合集
func (db *DB) getMemTables() ([]*memTable, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*memTable

	// Mutable memtable does not exist in read-only mode.
	if !db.opt.ReadOnly {
		// Get mutable memtable.
		// 获取活跃（可变的）memtable
		tables = append(tables, db.mt) //当前活跃的内存表为第一个内存表
		db.mt.IncrRef()
	}

	// Get immutable memtables.
	// 获取固定（不可变的）immemtable
	last := len(db.imm) - 1 //获取imm的数量
	for i := range db.imm {
		tables = append(tables, db.imm[last-i]) // 按新旧程度越新的放在越前面
		db.imm[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
//
// IMPORTANT: We should never write an entry with an older timestamp for the same key, We need to
// maintain this invariant to search for the latest value of a key, or else we need to search in all
// tables and find the max version among them.  To maintain this invariant, we also need to ensure
// that all versions of a key are always present in the same table from level 1, because compaction
// can push any table down.
//
// Update(23/09/2020) - We have dropped the move key implementation. Earlier we
// were inserting move keys to fix the invalid value pointers but we no longer
// do that. For every get("fooX") call where X is the version, we will search
// for "fooX" in all the levels of the LSM tree. This is expensive but it
// removes the overhead of handling move keys completely.
// get返回给定键在memtable或磁盘中的值。
// 请注意，该值将包括元字节。
//
// 重要提示：我们不应该为同一密钥写入具有旧时间戳的条目，我们需要
// 保持不变以搜索关键字的最新值，否则我们需要在所有值中搜索
// 并在其中找到最大版本。为了保持这种不变性，我们还需要确保
// 从级别1开始，密钥的所有版本始终存在于同一表中，因为压缩
// 可以推倒任何桌子。
//
// 更新（2020年9月23日）-我们已取消移动密钥的实施。早些时候，我们
// 我们正在插入移动键来修复无效的值指针，但我们不再
// 这样做。对于每个get（“fooX”）调用，其中X是版本，我们将搜索
// 对于LSM树的所有级别中的“fooX”。这很贵，但
// 完全消除了处理移动键的开销。

// 注意函数参数中的key后缀有当前事务的readTs
func (db *DB) get(key []byte) (y.ValueStruct, error) {
	if db.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	//先获取memtable
	tables, decr := db.getMemTables() // Lock should be released. 注意getMemTables会返回memtable与immemtable的合集
	defer decr()

	var maxVs y.ValueStruct
	version := y.ParseTs(key) // 从key中解析出前面加的readTs版本号

	y.NumGetsAdd(db.opt.MetricsEnabled, 1)
	for i := 0; i < len(tables); i++ { //从最新开始遍历table
		vs := tables[i].sl.Get(key) // 核心操作，去当前table的跳表中得到目标key
		y.NumMemtableGetsAdd(db.opt.MetricsEnabled, 1)
		if vs.Meta == 0 && vs.Value == nil {
			continue
		}
		// Found the required version of the key, return immediately.
		if vs.Version == version { // 如果查出来的key的版本号与当前要找的版本号（默认正常流程的话应该是当前事务的readTs）相等，则代表是当前事务写入的数据，可以直接返回
			y.NumGetsWithResultsAdd(db.opt.MetricsEnabled, 1) //统计数据的函数
			return vs, nil
		}
		if maxVs.Version < vs.Version { // 找到查出来的最大版本号的key（应该也要小于要找的版本号）
			maxVs = vs
		}
	}
	return db.lc.get(key, maxVs, 0) //这里就去level中找了
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) writeToLSM(b *request) error {
	// We should check the length of b.Prts and b.Entries only when badger is not
	// running in InMemory mode. In InMemory mode, we don't write anything to the
	// value log and that's why the length of b.Ptrs will always be zero.
	// 我们应该只在badgerDB未在InMemory模式下运行时检查b.Prts和b.Entries的长度。
	// 在InMemory模式下，我们不会向值日志写入任何内容，这就是为什么b.Ptrs的长度始终为零的原因。
	if !db.opt.InMemory && len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries { //遍历当前请求的kv数组
		var err error
		if entry.skipVlogAndSetThreshold(db.valueThreshold()) { // db.valueThreshold()获取的是分大小KV的界限，貌似默认是1024字节
			// 当前kv是小value，把值放进memtable
			// Will include deletion / tombstone case.
			err = db.mt.Put(entry.Key,
				y.ValueStruct{
					Value: entry.Value,
					// Ensure value pointer flag is removed. Otherwise, the value will fail
					// to be retrieved during iterator prefetch. `bitValuePointer` is only
					// known to be set in write to LSM when the entry is loaded from a backup
					// with lower ValueThreshold and its value was stored in the value log.
					Meta:      entry.meta &^ bitValuePointer,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		} else { // 当前kv是大KV，在LSM树的value处拼装上前面在处理vlog的时候得到的offset并放进memtable
			// Write pointer to Memtable.
			err = db.mt.Put(entry.Key,
				y.ValueStruct{
					Value:     b.Ptrs[i].Encode(), // 设置为（文件名+offset（偏移））
					Meta:      entry.meta | bitValuePointer,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		}
		if err != nil {
			return y.Wrapf(err, "while writing to memTable")
		}
	}
	if db.opt.SyncWrites { // 如果同步写
		return db.mt.SyncWAL()
	}
	return nil
}

// writeRequests is called serially by only one goroutine.
func (db *DB) writeRequests(reqs []*request) error { //批量处理写请求
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done() // 提醒上层写完成
		}
	}
	db.opt.Debugf("writeRequests called. Writing to value log")
	err := db.vlog.write(reqs) // 写入VLOG（核心代码块）
	if err != nil {
		done(err)
		return err
	}

	db.opt.Debugf("Writing to memtable")
	var count int
	for _, b := range reqs { // 写入LSM中（核心代码块）
		if len(b.Entries) == 0 { // 当前req内没有kv对要写入就跳过
			continue
		}
		count += len(b.Entries) //累加待写入的kv对数量
		var i uint64
		var err error
		// 下面这个ensureRoomForWrite函数实际上就是做的判断memtable是否满足转换为immemtable的条件，并做一些操作（核心代码块）
		for err = db.ensureRoomForWrite(); err == errNoRoom; err = db.ensureRoomForWrite() {
			i++
			if i%100 == 0 {
				db.opt.Debugf("Making room for writes")
			}
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			// 我们需要进行一些轮询，因为RoomForWrite和刷新器都需要访问.imm。
			// 当flushChan已满，您在那里被阻止，并且flusher正在尝试更新s.imm时，
			// 你会陷入死锁。
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			done(err)
			return y.Wrap(err, "writeRequests")
		}
		if err := db.writeToLSM(b); err != nil { //写入LSM树的核心函数，传入的是单个req
			done(err)
			return y.Wrap(err, "writeRequests")
		}
	}

	db.opt.Debugf("Sending updates to subscribers")
	db.pub.sendUpdates(reqs)

	done(nil)
	db.opt.Debugf("%d entries written", count)
	return nil
}

func (db *DB) sendToWriteCh(entries []*Entry) (*request, error) {
	if db.blockWrites.Load() == 1 {
		return nil, ErrBlockedWrites
	}
	var count, size int64
	for _, e := range entries {
		size += e.estimateSizeAndSetThreshold(db.valueThreshold())
		count++
	}
	y.NumBytesWrittenUserAdd(db.opt.MetricsEnabled, size)
	if count >= db.opt.maxBatchCount || size >= db.opt.maxBatchSize {
		return nil, ErrTxnTooBig
	}

	// We can only service one request because we need each txn to be stored in a contiguous section.
	// Txns should not interleave among other txns or rewrites.
	// 我们只能为一个请求提供服务，因为我们需要将每个txn存储在连续的部分中。Txns不应与其他Txns交织或重写。
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.交给doWrites来处理
	y.NumPutsAdd(db.opt.MetricsEnabled, int64(len(entries)))

	return req, nil
}

func (db *DB) doWrites(lc *z.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1) // 一个缓冲区的chan，里面不放具体数据，只是用来阻塞用的

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			db.opt.Errorf("writeRequests: %v", err)
		}
		<-pendingCh // 将那一个缓冲区释放，表示当前这一个写请求已经完成了（箭头左边不放东西就是单纯释放掉了）
	}

	// This variable tracks the number of pending writes.
	// 此变量跟踪挂起的写入次数。
	reqLen := new(expvar.Int) // 获取写请求的长度
	y.PendingWritesSet(db.opt.MetricsEnabled, db.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10) // 最多攒了10次req再去写（貌似是未必必须满才去写）
	for {
		var r *request
		select { //GO的select专用于通道处理的，每个case对应一个通道操作，若某个能执行，则就执行哪一个，多个能执行的，随机选一个执行！即只执行一个
		case r = <-db.writeCh: //读出来sendToWriteCh函数（即写入函数）传过来的那个写请求
		case <-lc.HasBeenClosed():
			goto closedCase //注意goto只是语句执行转移（可以跳出循环，如下），而不是开启协程
		}

		for { //无限循环
			reqs = append(reqs, r) //将前面读出来的写请求加入累计数组
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity { // 是否大于KV写缓存的容量长度（kvWriteChCapacity是常量固定1000）
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			// 要么推到待定，要么继续从writeCh中选择。
			case r = <-db.writeCh: //继续读写请求
			case pendingCh <- struct{}{}: //阻塞去处理写请求
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase: //这个貌似是在系统关闭的时候才会用到？
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		// 所有待处理的请求都被清空。
		// 不要关闭writeCh，因为它已在多个地方使用过。
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.处理写请求，就把这个容量为1的chan填满封死，处理完这个后再释放
				writeRequests(reqs)     // 最后处理下写请求
				return
			}
		}

	writeCase: //已经达到需要处理的地步，开始处理
		go writeRequests(reqs)         //另外开一个协程，异步的去处理当前累积的写请求
		reqs = make([]*request, 0, 10) //清空当前写请求数组
		reqLen.Set(0)
	}
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//
//	Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//
//	err := kv.BatchSetAsync(entries, func(err error)) {
//	   Check(err)
//	}
func (db *DB) batchSetAsync(entries []*Entry, f func(error)) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

var errNoRoom = stderrors.New("No room for write")

// ensureRoomForWrite is always called serially.
// 确保始终以串行方式调用RoomForWrite。
// 下面这个函数实际上就是做的判断memtable是否满足转换为immemtable的条件，并做一些操作
func (db *DB) ensureRoomForWrite() error {
	var err error
	db.lock.Lock()
	defer db.lock.Unlock()

	y.AssertTrue(db.mt != nil) // A nil mt indicates that DB is being closed.
	if !db.mt.isFull() {       //如果memtable非空就返回
		return nil
	}

	select {
	case db.flushChan <- db.mt: // 将当前的memtable写入到flushChan通道中（这个通道内的都是待刷入磁盘的immemtable）
		db.opt.Debugf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
			db.mt.sl.MemSize(), len(db.flushChan))
		// We manage to push this task. Let's modify imm.
		db.imm = append(db.imm, db.mt) // 将当前的满的memtable转换为immemtable放到immemtable数组中
		db.mt, err = db.newMemTable()  // 创建一个新的memtable，来接受新的写入请求
		if err != nil {
			return y.Wrapf(err, "cannot create new mem table")
		}
		// New memtable is empty. We certainly have room.
		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		// 我们需要这样做来解锁并允许刷新器修改imm。
		return errNoRoom
	}
}

func arenaSize(opt Options) int64 {
	return opt.MemTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

// buildL0Table builds a new table from the memtable.
// buildL0Table从memtable构建一个新表。
func buildL0Table(iter y.Iterator, dropPrefixes [][]byte, bopts table.Options) *table.Builder {
	defer iter.Close()

	b := table.NewTableBuilder(bopts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if len(dropPrefixes) > 0 && hasAnyPrefixes(iter.Key(), dropPrefixes) {
			continue
		}
		vs := iter.Value()
		var vp valuePointer
		if vs.Meta&bitValuePointer > 0 {
			vp.Decode(vs.Value)
		}
		b.Add(iter.Key(), iter.Value(), vp.Len)
	}

	return b
}

// handleMemTableFlush must be run serially.
// handleMemTableFlush必须连续运行。handleMemTableFlush是做将memtable中的数据刷入磁盘L0层操作的函数
func (db *DB) handleMemTableFlush(mt *memTable, dropPrefixes [][]byte) error {
	bopts := buildTableOptions(db)           //创建一个表配置
	itr := mt.sl.NewUniIterator(false)       //创建一个迭代器
	builder := buildL0Table(itr, nil, bopts) //buildL0Table从memtable构建一个新表的构造器。
	defer builder.Close()

	// buildL0Table can return nil if the none of the items in the skiplist are
	// added to the builder. This can happen when drop prefix is set and all
	// the items are skipped.
	// 如果skiplist中的所有项目都没有添加到table builder中，buildL0Table可以返回nil。当设置了删除前缀并跳过所有项目时，可能会出现这种情况。
	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileID := db.lc.reserveFileID()
	var tbl *table.Table
	var err error
	if db.opt.InMemory {
		data := builder.Finish()
		tbl, err = table.OpenInMemoryTable(data, fileID, &bopts)
	} else {
		tbl, err = table.CreateTable(table.NewFilename(fileID, db.opt.Dir), builder) //从创建器中真正创建table
	}
	if err != nil {
		return y.Wrap(err, "error while creating table")
	}
	// We own a ref on tbl.
	err = db.lc.addLevel0Table(tbl) // 将当前创建的表加载到levelcontroler的第0层
	_ = tbl.DecrRef()               // Releases our ref.
	return err
}

// flushMemtable must keep running until we send it an empty memtable. If there
// are errors during handling the memtable flush, we'll retry indefinitely.
// flushMemtable必须继续运行，直到我们向它发送一个空的memtable。如果在处理memtable刷新过程中出现错误，我们将无限期重试。
func (db *DB) flushMemtable(lc *z.Closer) {
	defer lc.Done()

	for mt := range db.flushChan {
		if mt == nil {
			continue
		}

		for {
			if err := db.handleMemTableFlush(mt, nil); err != nil {
				// Encountered error. Retry indefinitely.
				db.opt.Errorf("error flushing memtable to disk: %v, retrying", err)
				time.Sleep(time.Second)
				continue
			}

			// Update s.imm. Need a lock.
			db.lock.Lock()
			// This is a single-threaded operation. mt corresponds to the head of
			// db.imm list. Once we flush it, we advance db.imm. The next mt
			// which would arrive here would match db.imm[0], because we acquire a
			// lock over DB when pushing to flushChan.
			// TODO: This logic is dirty AF. Any change and this could easily break.
			y.AssertTrue(mt == db.imm[0])
			db.imm = db.imm[1:]
			mt.DecrRef() // Return memory.
			// unlock
			db.lock.Unlock()
			break
		}
	}
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// This function does a filewalk, calculates the size of vlog and sst files and stores it in
// y.LSMSize and y.VlogSize.
// 此函数执行文件遍历，计算vlog和sst文件的大小，并将其存储在y.LSMSize和y.VlogSize中。
func (db *DB) calculateSize() {
	if db.opt.InMemory {
		return
	}
	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			switch ext {
			case ".sst":
				lsmSize += info.Size()
			case ".vlog":
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			db.opt.Debugf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	lsmSize, vlogSize := totalSize(db.opt.Dir)
	y.LSMSizeSet(db.opt.MetricsEnabled, db.opt.Dir, newInt(lsmSize))
	// If valueDir is different from dir, we'd have to do another walk.
	if db.opt.ValueDir != db.opt.Dir {
		_, vlogSize = totalSize(db.opt.ValueDir)
	}
	y.VlogSizeSet(db.opt.MetricsEnabled, db.opt.ValueDir, newInt(vlogSize))
}

func (db *DB) updateSize(lc *z.Closer) {
	defer lc.Done()
	if db.opt.InMemory {
		return
	}

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// RunValueLogGC triggers a value log garbage collection.

// It picks value log files to perform GC based on statistics that are collected
// during compactions.  If no such statistics are available, then log files are
// picked in random order. The process stops as soon as the first log file is
// encountered which does not result in garbage collection.

// When a log file is picked, it is first sampled. If the sample shows that we
// can discard at least discardRatio space of that file, it would be rewritten.

// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is
// thrown indicating that the call resulted in no file rewrites.

// We recommend setting discardRatio to 0.5, thus indicating that a file be
// rewritten if half the space can be discarded.  This results in a lifetime
// value log write amplification of 2 (1 from original write + 0.5 rewrite +
// 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
// space reclaims, while setting it to a lower value would result in more space
// reclaims at the cost of increased activity on the LSM tree. discardRatio
// must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
// ErrInvalidRequest is returned.

// Only one GC is allowed at a time. If another value log GC is running, or DB
// has been closed, this would return an ErrRejected.

// Note: Every time GC is run, it would produce a spike of activity on the LSM
// tree.

// RunValueLogGC触发值日志垃圾回收。
// 它根据压缩过程中收集的统计数据选择值日志文件来执行GC。如果没有此类统计数据，则以随机顺序选择日志文件。一旦遇到第一个不会导致垃圾回收的日志文件，该进程就会停止。
// 当拾取日志文件时，首先对其进行采样。如果示例显示我们达到了脏键比discardRatio，那么它将被重写。
// 如果对RunValueLogGC的调用没有导致重写，则会抛出ErrNoRewrite，表示该调用没有导致文件重写。
// 我们建议将disardRatio设置为0.5，这样表示如果如果有一半的脏键比，则可以重写文件。这导致生命周期值日志写入放大2（原始写入1+0.5重写+0.25 + 0.125 + ... = 2）
// 将其设置为更高的值将导致更少的空间回收，而将它设置为较低的值将以增加LSM树上的活动为代价导致更多的空间回收。discadRatio必须在（0.0，1.0）范围内，排除两个端点，否则返回ErrInvalidRequest。
// 一次只允许一个GC。如果另一个值日志GC正在运行，或者DB已关闭，这将返回ErrorRejected。
// 注意：每次运行GC时，它都会在LSM树上产生一个活动尖峰。
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if db.opt.InMemory { // 纯内存不需要GC
		return ErrGCInMemoryMode
	}
	if discardRatio >= 1.0 || discardRatio <= 0.0 { // 脏键率不合规
		return ErrInvalidRequest
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio)
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm, vlog int64) {
	if y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir) == nil {
		lsm, vlog = 0, 0
		return
	}
	lsm = y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir).(*expvar.Int).Value()
	vlog = y.VlogSizeGet(db.opt.MetricsEnabled, db.opt.ValueDir).(*expvar.Int).Value()
	return
}

// Sequence represents a Badger sequence.
type Sequence struct {
	lock      sync.Mutex
	db        *DB
	key       []byte
	next      uint64
	leased    uint64
	bandwidth uint64
}

// Next would return the next integer in the sequence, updating the lease by running a transaction
// if needed.
func (seq *Sequence) Next() (uint64, error) {
	seq.lock.Lock()
	defer seq.lock.Unlock()
	if seq.next >= seq.leased {
		if err := seq.updateLease(); err != nil {
			return 0, err
		}
	}
	val := seq.next
	seq.next++
	return val, nil
}

// Release the leased sequence to avoid wasted integers. This should be done right
// before closing the associated DB. However it is valid to use the sequence after
// it was released, causing a new lease with full bandwidth.
func (seq *Sequence) Release() error {
	seq.lock.Lock()
	defer seq.lock.Unlock()
	err := seq.db.Update(func(txn *Txn) error {
		item, err := txn.Get(seq.key)
		if err != nil {
			return err
		}

		var num uint64
		if err := item.Value(func(v []byte) error {
			num = binary.BigEndian.Uint64(v)
			return nil
		}); err != nil {
			return err
		}

		if num == seq.leased {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], seq.next)
			return txn.SetEntry(NewEntry(seq.key, buf[:]))
		}

		return nil
	})
	if err != nil {
		return err
	}
	seq.leased = seq.next
	return nil
}

func (seq *Sequence) updateLease() error {
	return seq.db.Update(func(txn *Txn) error {
		item, err := txn.Get(seq.key)
		switch {
		case err == ErrKeyNotFound:
			seq.next = 0
		case err != nil:
			return err
		default:
			var num uint64
			if err := item.Value(func(v []byte) error {
				num = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				return err
			}
			seq.next = num
		}

		lease := seq.next + seq.bandwidth
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], lease)
		if err = txn.SetEntry(NewEntry(seq.key, buf[:])); err != nil {
			return err
		}
		seq.leased = lease
		return nil
	})
}

// GetSequence would initiate a new sequence object, generating it from the stored lease, if
// available, in the database. Sequence can be used to get a list of monotonically increasing
// integers. Multiple sequences can be created by providing different keys. Bandwidth sets the
// size of the lease, determining how many Next() requests can be served from memory.
//
// GetSequence is not supported on ManagedDB. Calling this would result in a panic.
func (db *DB) GetSequence(key []byte, bandwidth uint64) (*Sequence, error) {
	if db.opt.managedTxns {
		panic("Cannot use GetSequence with managedDB=true.")
	}

	switch {
	case len(key) == 0:
		return nil, ErrEmptyKey
	case bandwidth == 0:
		return nil, ErrZeroBandwidth
	}
	seq := &Sequence{
		db:        db,
		key:       key,
		next:      0,
		leased:    0,
		bandwidth: bandwidth,
	}
	err := seq.updateLease()
	return seq, err
}

// Tables gets the TableInfo objects from the level controller. If withKeysCount
// is true, TableInfo objects also contain counts of keys for the tables.
func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}

// Levels gets the LevelInfo.
func (db *DB) Levels() []LevelInfo {
	return db.lc.getLevelInfo()
}

// EstimateSize can be used to get rough estimate of data size for a given prefix.
func (db *DB) EstimateSize(prefix []byte) (uint64, uint64) {
	var onDiskSize, uncompressedSize uint64
	tables := db.Tables()
	for _, ti := range tables {
		if bytes.HasPrefix(ti.Left, prefix) && bytes.HasPrefix(ti.Right, prefix) {
			onDiskSize += uint64(ti.OnDiskSize)
			uncompressedSize += uint64(ti.UncompressedSize)
		}
	}
	return onDiskSize, uncompressedSize
}

// Ranges can be used to get rough key ranges to divide up iteration over the DB. The ranges here
// would consider the prefix, but would not necessarily start or end with the prefix. In fact, the
// first range would have nil as left key, and the last range would have nil as the right key.
func (db *DB) Ranges(prefix []byte, numRanges int) []*keyRange {
	var splits []string
	tables := db.Tables()

	// We just want table ranges here and not keys count.
	for _, ti := range tables {
		// We don't use ti.Left, because that has a tendency to store !badger keys. Skip over tables
		// at upper levels. Only choose tables from the last level.
		if ti.Level != db.opt.MaxLevels-1 {
			continue
		}
		if bytes.HasPrefix(ti.Right, prefix) {
			splits = append(splits, string(ti.Right))
		}
	}

	// If the number of splits is low, look at the offsets inside the
	// tables to generate more splits.
	if len(splits) < 32 {
		numTables := len(tables)
		if numTables == 0 {
			numTables = 1
		}
		numPerTable := 32 / numTables
		if numPerTable == 0 {
			numPerTable = 1
		}
		splits = db.lc.keySplits(numPerTable, prefix)
	}

	// If the number of splits is still < 32, then look at the memtables.
	if len(splits) < 32 {
		maxPerSplit := 10000
		mtSplits := func(mt *memTable) {
			if mt == nil {
				return
			}
			count := 0
			iter := mt.sl.NewIterator()
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				if count%maxPerSplit == 0 {
					// Add a split every maxPerSplit keys.
					if bytes.HasPrefix(iter.Key(), prefix) {
						splits = append(splits, string(iter.Key()))
					}
				}
				count += 1
			}
			_ = iter.Close()
		}

		db.lock.Lock()
		defer db.lock.Unlock()
		var memTables []*memTable
		memTables = append(memTables, db.imm...)
		for _, mt := range memTables {
			mtSplits(mt)
		}
		mtSplits(db.mt)
	}

	// We have our splits now. Let's convert them to ranges.
	sort.Strings(splits)
	var ranges []*keyRange
	var start []byte
	for _, key := range splits {
		ranges = append(ranges, &keyRange{left: start, right: y.SafeCopy(nil, []byte(key))})
		start = y.SafeCopy(nil, []byte(key))
	}
	ranges = append(ranges, &keyRange{left: start})

	// Figure out the approximate table size this range has to deal with.
	for _, t := range tables {
		tr := keyRange{left: t.Left, right: t.Right}
		for _, r := range ranges {
			if len(r.left) == 0 || len(r.right) == 0 {
				continue
			}
			if r.overlapsWith(tr) {
				r.size += int64(t.UncompressedSize)
			}
		}
	}

	var total int64
	for _, r := range ranges {
		total += r.size
	}
	if total == 0 {
		return ranges
	}
	// Figure out the average size, so we know how to bin the ranges together.
	avg := total / int64(numRanges)

	var out []*keyRange
	var i int
	for i < len(ranges) {
		r := ranges[i]
		cur := &keyRange{left: r.left, size: r.size, right: r.right}
		i++
		for ; i < len(ranges); i++ {
			next := ranges[i]
			if cur.size+next.size > avg {
				break
			}
			cur.right = next.right
			cur.size += next.size
		}
		out = append(out, cur)
	}
	return out
}

// MaxBatchCount returns max possible entries in batch
func (db *DB) MaxBatchCount() int64 {
	return db.opt.maxBatchCount
}

// MaxBatchSize returns max possible batch size
func (db *DB) MaxBatchSize() int64 {
	return db.opt.maxBatchSize
}

func (db *DB) stopMemoryFlush() {
	// Stop memtable flushes.
	if db.closers.memtable != nil {
		close(db.flushChan)
		db.closers.memtable.SignalAndWait()
	}
}

func (db *DB) stopCompactions() {
	// Stop compactions.
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
	}
}

func (db *DB) startCompactions() {
	// Resume compactions.
	if db.closers.compactors != nil {
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)
	}
}

func (db *DB) startMemoryFlush() {
	// Start memory fluhser.
	if db.closers.memtable != nil {
		db.flushChan = make(chan *memTable, db.opt.NumMemtables)
		db.closers.memtable = z.NewCloser(1)
		go func() {
			db.flushMemtable(db.closers.memtable)
		}()
	}
}

// Flatten can be used to force compactions on the LSM tree so all the tables fall on the same
// level. This ensures that all the versions of keys are colocated and not split across multiple
// levels, which is necessary after a restore from backup. During Flatten, live compactions are
// stopped. Ideally, no writes are going on during Flatten. Otherwise, it would create competition
// between flattening the tree and new tables being created at level zero.
func (db *DB) Flatten(workers int) error {

	db.stopCompactions()
	defer db.startCompactions()

	compactAway := func(cp compactionPriority) error {
		db.opt.Infof("Attempting to compact with %+v\n", cp)
		errCh := make(chan error, 1)
		for i := 0; i < workers; i++ {
			go func() {
				errCh <- db.lc.doCompact(175, cp)
			}()
		}
		var success int
		var rerr error
		for i := 0; i < workers; i++ {
			err := <-errCh
			if err != nil {
				rerr = err
				db.opt.Warningf("While running doCompact with %+v. Error: %v\n", cp, err)
			} else {
				success++
			}
		}
		if success == 0 {
			return rerr
		}
		// We could do at least one successful compaction. So, we'll consider this a success.
		db.opt.Infof("%d compactor(s) succeeded. One or more tables from level %d compacted.\n",
			success, cp.level)
		return nil
	}

	hbytes := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}

	t := db.lc.levelTargets()
	for {
		db.opt.Infof("\n")
		var levels []int
		for i, l := range db.lc.levels {
			sz := l.getTotalSize()
			db.opt.Infof("Level: %d. %8s Size. %8s Max.\n",
				i, hbytes(l.getTotalSize()), hbytes(t.targetSz[i]))
			if sz > 0 {
				levels = append(levels, i)
			}
		}
		if len(levels) <= 1 {
			prios := db.lc.pickCompactLevels(nil)
			if len(prios) == 0 || prios[0].score <= 1.0 {
				db.opt.Infof("All tables consolidated into one level. Flattening done.\n")
				return nil
			}
			if err := compactAway(prios[0]); err != nil {
				return err
			}
			continue
		}
		// Create an artificial compaction priority, to ensure that we compact the level.
		cp := compactionPriority{level: levels[0], score: 1.71}
		if err := compactAway(cp); err != nil {
			return err
		}
	}
}

func (db *DB) blockWrite() error {
	// Stop accepting new writes.
	if !db.blockWrites.CompareAndSwap(0, 1) {
		return ErrBlockedWrites
	}

	// Make all pending writes finish. The following will also close writeCh.
	db.closers.writes.SignalAndWait()
	db.opt.Infof("Writes flushed. Stopping compactions now...")
	return nil
}

func (db *DB) unblockWrite() {
	db.closers.writes = z.NewCloser(1)
	go db.doWrites(db.closers.writes)

	// Resume writes.
	db.blockWrites.Store(0)
}

func (db *DB) prepareToDrop() (func(), error) {
	if db.opt.ReadOnly {
		panic("Attempting to drop data in read-only mode.")
	}
	// In order prepare for drop, we need to block the incoming writes and
	// write it to db. Then, flush all the pending memtable. So that, we
	// don't miss any entries.
	if err := db.blockWrite(); err != nil {
		return func() {}, err
	}
	reqs := make([]*request, 0, 10)
	for {
		select {
		case r := <-db.writeCh:
			reqs = append(reqs, r)
		default:
			if err := db.writeRequests(reqs); err != nil {
				db.opt.Errorf("writeRequests: %v", err)
			}
			db.stopMemoryFlush()
			return func() {
				db.opt.Infof("Resuming writes")
				db.startMemoryFlush()
				db.unblockWrite()
			}, nil
		}
	}
}

// DropAll would drop all the data stored in Badger. It does this in the following way.
// - Stop accepting new writes.
// - Pause memtable flushes and compactions.
// - Pick all tables from all levels, create a changeset to delete all these
// tables and apply it to manifest.
// - Pick all log files from value log, and delete all of them. Restart value log files from zero.
// - Resume memtable flushes and compactions.
//
// NOTE: DropAll is resilient to concurrent writes, but not to reads. It is up to the user to not do
// any reads while DropAll is going on, otherwise they may result in panics. Ideally, both reads and
// writes are paused before running DropAll, and resumed after it is finished.
func (db *DB) DropAll() error {
	f, err := db.dropAll()
	if f != nil {
		f()
	}
	return err
}

func (db *DB) dropAll() (func(), error) {
	db.opt.Infof("DropAll called. Blocking writes...")
	f, err := db.prepareToDrop()
	if err != nil {
		return f, err
	}
	// prepareToDrop will stop all the incoming write and flushes any pending memtables.
	// Before we drop, we'll stop the compaction because anyways all the datas are going to
	// be deleted.
	db.stopCompactions()
	resume := func() {
		db.startCompactions()
		f()
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	// Remove inmemory tables. Calling DecrRef for safety. Not sure if they're absolutely needed.
	db.mt.DecrRef()
	for _, mt := range db.imm {
		mt.DecrRef()
	}
	db.imm = db.imm[:0]
	db.mt, err = db.newMemTable() // Set it up for future writes.
	if err != nil {
		return resume, y.Wrapf(err, "cannot open new memtable")
	}

	num, err := db.lc.dropTree()
	if err != nil {
		return resume, err
	}
	db.opt.Infof("Deleted %d SSTables. Now deleting value logs...\n", num)

	num, err = db.vlog.dropAll()
	if err != nil {
		return resume, err
	}
	db.lc.nextFileID.Store(1)
	db.opt.Infof("Deleted %d value log files. DropAll done.\n", num)
	db.blockCache.Clear()
	db.indexCache.Clear()
	db.threshold.Clear(db.opt)
	return resume, nil
}

// DropPrefix would drop all the keys with the provided prefix. It does this in the following way:
//   - Stop accepting new writes.
//   - Stop memtable flushes before acquiring lock. Because we're acquiring lock here
//     and memtable flush stalls for lock, which leads to deadlock
//   - Flush out all memtables, skipping over keys with the given prefix, Kp.
//   - Write out the value log header to memtables when flushing, so we don't accidentally bring Kp
//     back after a restart.
//   - Stop compaction.
//   - Compact L0->L1, skipping over Kp.
//   - Compact rest of the levels, Li->Li, picking tables which have Kp.
//   - Resume memtable flushes, compactions and writes.
func (db *DB) DropPrefix(prefixes ...[]byte) error {
	if len(prefixes) == 0 {
		return nil
	}
	db.opt.Infof("DropPrefix called for %s", prefixes)
	f, err := db.prepareToDrop()
	if err != nil {
		return err
	}
	defer f()

	var filtered [][]byte
	if filtered, err = db.filterPrefixesToDrop(prefixes); err != nil {
		return err
	}
	// If there is no prefix for which the data already exist, do not do anything.
	if len(filtered) == 0 {
		db.opt.Infof("No prefixes to drop")
		return nil
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	db.imm = append(db.imm, db.mt)
	for _, memtable := range db.imm {
		if memtable.sl.Empty() {
			memtable.DecrRef()
			continue
		}
		db.opt.Debugf("Flushing memtable")
		if err := db.handleMemTableFlush(memtable, filtered); err != nil {
			db.opt.Errorf("While trying to flush memtable: %v", err)
			return err
		}
		memtable.DecrRef()
	}
	db.stopCompactions()
	defer db.startCompactions()
	db.imm = db.imm[:0]
	db.mt, err = db.newMemTable()
	if err != nil {
		return y.Wrapf(err, "cannot create new mem table")
	}

	// Drop prefixes from the levels.
	if err := db.lc.dropPrefixes(filtered); err != nil {
		return err
	}
	db.opt.Infof("DropPrefix done")
	return nil
}

func (db *DB) filterPrefixesToDrop(prefixes [][]byte) ([][]byte, error) {
	var filtered [][]byte
	for _, prefix := range prefixes {
		err := db.View(func(txn *Txn) error {
			iopts := DefaultIteratorOptions
			iopts.Prefix = prefix
			iopts.PrefetchValues = false
			itr := txn.NewIterator(iopts)
			defer itr.Close()
			itr.Rewind()
			if itr.ValidForPrefix(prefix) {
				filtered = append(filtered, prefix)
			}
			return nil
		})
		if err != nil {
			return filtered, err
		}
	}
	return filtered, nil
}

// Checks if the key is banned. Returns the respective error if the key belongs to any of the banned
// namepspaces. Else it returns nil.
func (db *DB) isBanned(key []byte) error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	if len(key) <= db.opt.NamespaceOffset+8 {
		return nil
	}
	if db.bannedNamespaces.has(y.BytesToU64(key[db.opt.NamespaceOffset:])) {
		return ErrBannedKey
	}
	return nil
}

// BanNamespace bans a namespace. Read/write to keys belonging to any of such namespace is denied.
func (db *DB) BanNamespace(ns uint64) error {
	if db.opt.NamespaceOffset < 0 {
		return ErrNamespaceMode
	}
	db.opt.Infof("Banning namespace: %d", ns)
	// First set the banned namespaces in DB and then update the in-memory structure.
	key := y.KeyWithTs(append(bannedNsKey, y.U64ToBytes(ns)...), 1)
	entry := []*Entry{{
		Key:   key,
		Value: nil,
	}}
	req, err := db.sendToWriteCh(entry)
	if err != nil {
		return err
	}
	if err := req.Wait(); err != nil {
		return err
	}
	db.bannedNamespaces.add(ns)
	return nil
}

// BannedNamespaces returns the list of prefixes banned for DB.
func (db *DB) BannedNamespaces() []uint64 {
	return db.bannedNamespaces.all()
}

// KVList contains a list of key-value pairs.
type KVList = pb.KVList

// Subscribe can be used to watch key changes for the given key prefixes and the ignore string.
// At least one prefix should be passed, or an error will be returned.
// You can use an empty prefix to monitor all changes to the DB.
// Ignore string is the byte ranges for which prefix matching will be ignored.
// For example: ignore = "2-3", and prefix = "abc" will match for keys "abxxc", "abdfc" etc.
// This function blocks until the given context is done or an error occurs.
// The given function will be called with a new KVList containing the modified keys and the
// corresponding values.
func (db *DB) Subscribe(ctx context.Context, cb func(kv *KVList) error, matches []pb.Match) error {
	if cb == nil {
		return ErrNilCallback
	}

	c := z.NewCloser(1)
	s, err := db.pub.newSubscriber(c, matches)
	if err != nil {
		return y.Wrapf(err, "while creating a new subscriber")
	}
	slurp := func(batch *pb.KVList) error {
		for {
			select {
			case kvs := <-s.sendCh:
				batch.Kv = append(batch.Kv, kvs.Kv...)
			default:
				if len(batch.GetKv()) > 0 {
					return cb(batch)
				}
				return nil
			}
		}
	}

	drain := func() {
		for {
			select {
			case _, ok := <-s.sendCh:
				if !ok {
					// Channel is closed.
					return
				}
			default:
				return
			}
		}
	}
	for {
		select {
		case <-c.HasBeenClosed():
			// No need to delete here. Closer will be called only while
			// closing DB. Subscriber will be deleted by cleanSubscribers.
			err := slurp(new(pb.KVList))
			// Drain if any pending updates.
			c.Done()
			return err
		case <-ctx.Done():
			c.Done()
			s.active.Store(0)
			drain()
			db.pub.deleteSubscriber(s.id)
			// Delete the subscriber to avoid further updates.
			return ctx.Err()
		case batch := <-s.sendCh:
			err := slurp(batch)
			if err != nil {
				c.Done()
				s.active.Store(0)
				drain()
				// Delete the subscriber if there is an error by the callback.
				db.pub.deleteSubscriber(s.id)
				return err
			}
		}
	}
}

func (db *DB) syncDir(dir string) error {
	if db.opt.InMemory {
		return nil
	}
	return syncDir(dir)
}

func createDirs(opt Options) error {
	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return errors.Errorf("Cannot find directory %q for read-only open", path)
			}
			// Try to create the directory
			err = os.MkdirAll(path, 0700)
			if err != nil {
				return y.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}
	return nil
}

// Stream the contents of this DB to a new DB with options outOptions that will be
// created in outDir.
func (db *DB) StreamDB(outOptions Options) error {
	outDir := outOptions.Dir

	// Open output DB.
	outDB, err := OpenManaged(outOptions)
	if err != nil {
		return y.Wrapf(err, "cannot open out DB at %s", outDir)
	}
	defer outDB.Close()
	writer := outDB.NewStreamWriter()
	if err := writer.Prepare(); err != nil {
		return y.Wrapf(err, "cannot create stream writer in out DB at %s", outDir)
	}

	// Stream contents of DB to the output DB.
	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = fmt.Sprintf("Streaming DB to new DB at %s", outDir)

	stream.Send = func(buf *z.Buffer) error {
		return writer.Write(buf)
	}
	if err := stream.Orchestrate(context.Background()); err != nil {
		return y.Wrapf(err, "cannot stream DB to out DB at %s", outDir)
	}
	if err := writer.Flush(); err != nil {
		return y.Wrapf(err, "cannot flush writer")
	}
	return nil
}

// Opts returns a copy of the DB options.
func (db *DB) Opts() Options {
	return db.opt
}

type CacheType int

const (
	BlockCache CacheType = iota
	IndexCache
)

// CacheMaxCost updates the max cost of the given cache (either block or index cache).
// The call will have an effect only if the DB was created with the cache. Otherwise it is
// a no-op. If you pass a negative value, the function will return the current value
// without updating it.
func (db *DB) CacheMaxCost(cache CacheType, maxCost int64) (int64, error) {
	if db == nil {
		return 0, nil
	}

	if maxCost < 0 {
		switch cache {
		case BlockCache:
			return db.blockCache.MaxCost(), nil
		case IndexCache:
			return db.indexCache.MaxCost(), nil
		default:
			return 0, errors.Errorf("invalid cache type")
		}
	}

	switch cache {
	case BlockCache:
		db.blockCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	case IndexCache:
		db.indexCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	default:
		return 0, errors.Errorf("invalid cache type")
	}
}

func (db *DB) LevelsToString() string {
	levels := db.Levels()
	h := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}
	base := func(b bool) string {
		if b {
			return "B"
		}
		return " "
	}

	var b strings.Builder
	b.WriteRune('\n')
	for _, li := range levels {
		b.WriteString(fmt.Sprintf(
			"Level %d [%s]: NumTables: %02d. Size: %s of %s. Score: %.2f->%.2f"+
				" StaleData: %s Target FileSize: %s\n",
			li.Level, base(li.IsBaseLevel), li.NumTables,
			h(li.Size), h(li.TargetSize), li.Score, li.Adjusted, h(li.StaleDatSize),
			h(li.TargetFileSize)))
	}
	b.WriteString("Level Done\n")
	return b.String()
}
