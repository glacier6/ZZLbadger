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
	stderrors "errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// maxVlogFileSize is the maximum size of the vlog file which can be created. Vlog Offset is of
// uint32, so limiting at max uint32.
var maxVlogFileSize uint32 = math.MaxUint32

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	bitDelete                 byte = 1 << 0 // Set if the key has been deleted.
	bitValuePointer           byte = 1 << 1 // Set if the value is NOT stored directly next to key.
	bitDiscardEarlierVersions byte = 1 << 2 // Set if earlier versions can be discarded.
	// Set if item shouldn't be discarded via compactions (used by merge operator)
	bitMergeEntry byte = 1 << 3
	// The MSB 2 bits are for transactions.
	bitTxn    byte = 1 << 6 // Set if the entry is part of a txn.
	bitFinTxn byte = 1 << 7 // Set if the entry is to indicate end of txn in value log.

	mi int64 = 1 << 20 //nolint:unused

	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	vlogHeaderSize = 20
)

var errStop = stderrors.New("Stop iteration")
var errTruncate = stderrors.New("Do truncate")

type logEntry func(e Entry, vp valuePointer) error

type safeRead struct {
	k []byte
	v []byte

	recordOffset uint32
	lf           *logFile
}

// hashReader implements io.Reader, io.ByteReader interfaces. It also keeps track of the number
// bytes read. The hashReader writes to h (hash) what it reads from r.
type hashReader struct {
	r         io.Reader
	h         hash.Hash32
	bytesRead int // Number of bytes read.
}

func newHashReader(r io.Reader) *hashReader {
	hash := crc32.New(y.CastagnoliCrcTable)
	return &hashReader{
		r: r,
		h: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *hashReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil {
		return n, err
	}
	t.bytesRead += n
	return t.h.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *hashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *hashReader) Sum32() uint32 {
	return t.h.Sum32()
}

// Entry reads an entry from the provided reader. It also validates the checksum for every entry
// read. Returns error on failure.
func (r *safeRead) Entry(reader io.Reader) (*Entry, error) {
	tee := newHashReader(reader)
	var h header
	hlen, err := h.DecodeFrom(tee)
	if err != nil {
		return nil, err
	}
	if h.klen > uint32(1<<16) { // Key length must be below uint16.
		return nil, errTruncate
	}
	kl := int(h.klen)
	if cap(r.k) < kl {
		r.k = make([]byte, 2*kl)
	}
	vl := int(h.vlen)
	if cap(r.v) < vl {
		r.v = make([]byte, 2*vl)
	}

	e := &Entry{}
	e.offset = r.recordOffset
	e.hlen = hlen
	buf := make([]byte, h.klen+h.vlen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = errTruncate
		}
		return nil, err
	}
	if r.lf.encryptionEnabled() {
		if buf, err = r.lf.decryptKV(buf[:], r.recordOffset); err != nil {
			return nil, err
		}
	}
	e.Key = buf[:h.klen]
	e.Value = buf[h.klen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = errTruncate
		}
		return nil, err
	}
	crc := y.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, errTruncate
	}
	e.meta = h.meta
	e.UserMeta = h.userMeta
	e.ExpiresAt = h.expiresAt
	return e, nil
}

func (vlog *valueLog) rewrite(f *logFile) error {
	vlog.filesLock.RLock()
	for _, fid := range vlog.filesToBeDeleted { //这个for循环是为了检查当前的Vlog文件是否被删除了
		if fid == f.fid {
			vlog.filesLock.RUnlock() //被删了，就把上面加的锁解开
			return errors.Errorf("value log file already marked for deletion fid: %d", fid)
		}
	}
	maxFid := vlog.maxFid
	y.AssertTruef(f.fid < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)
	vlog.filesLock.RUnlock()

	vlog.opt.Infof("Rewriting fid: %d", f.fid)
	wb := make([]*Entry, 0, 1000) //存放KV数量的缓冲区？
	var size int64

	y.AssertTrue(vlog.db != nil)
	var count, moved int
	fe := func(e Entry) error {
		count++
		if count%100000 == 0 {
			vlog.opt.Debugf("Processing entry %d", count)
		}

		vs, err := vlog.db.get(e.Key) // 从LSM tree里面查当前key的最新版本
		if err != nil {
			return err
		}
		if discardEntry(e, vs, vlog.db) { //判断当前key是否可以删除，如果是可删除的（返回true），就暂时什么操作也不做（直接return nil）
			return nil
		}

		//下面的就是对需要保留的kv进行操作

		// Value is still present in value log.
		if len(vs.Value) == 0 {
			return errors.Errorf("Empty value: %+v", vs)
		}
		var vp valuePointer
		vp.Decode(vs.Value) // 当前value还是一个字节数组，需要进行编解码，序列化

		// If the entry found from the LSM Tree points to a newer vlog file, don't do anything.
		if vp.Fid > f.fid {
			return nil
		}
		// If the entry found from the LSM Tree points to an offset greater than the one
		// read from vlog, don't do anything.
		if vp.Offset > e.offset {
			return nil
		}
		// If the entry read from LSM Tree and vlog file point to the same vlog file and offset,
		// insert them back into the DB.
		// NOTE: It might be possible that the entry read from the LSM Tree points to
		// an older vlog file. See the comments in the else part.
		if vp.Fid == f.fid && vp.Offset == e.offset { //都匹配的上，证明是一个有效的KV
			moved++ //移除的计数位+1
			// This new entry only contains the key, and a pointer to the value.
			ne := new(Entry)
			// Remove only the bitValuePointer and transaction markers. We
			// should keep the other bits.
			ne.meta = e.meta &^ (bitValuePointer | bitTxn | bitFinTxn) //下面这几行做一个拼接处理
			ne.UserMeta = e.UserMeta
			ne.ExpiresAt = e.ExpiresAt
			ne.Key = append([]byte{}, e.Key...)
			ne.Value = append([]byte{}, e.Value...)
			es := ne.estimateSizeAndSetThreshold(vlog.db.valueThreshold())
			// Consider size of value as well while considering the total size
			// of the batch. There have been reports of high memory usage in
			// rewrite because we don't consider the value size. See #1292.
			es += int64(len(e.Value))

			// Ensure length and size of wb is within transaction limits.
			if int64(len(wb)+1) >= vlog.opt.maxBatchCount || //批量的操作
				size+es >= vlog.opt.maxBatchSize {
				if err := vlog.db.batchSet(wb); err != nil { //把判断有效的KV对重新写回LSM tree以及Vlog（批处理，基本与普通写入流程一致，即伪装成一次批量的写请求）
					return err
				}
				size = 0
				wb = wb[:0]
			}
			wb = append(wb, ne)
			size += es
		} else { //nolint:staticcheck
			// It might be possible that the entry read from LSM Tree points to
			// an older vlog file.  This can happen in the following situation.
			// Assume DB is opened with
			// numberOfVersionsToKeep=1
			//
			// Now, if we have ONLY one key in the system "FOO" which has been
			// updated 3 times and the same key has been garbage collected 3
			// times, we'll have 3 versions of the movekey
			// for the same key "FOO".
			//
			// NOTE: moveKeyi is the gc'ed version of the original key with version i
			// We're calling the gc'ed keys as moveKey to simplify the
			// explanantion. We used to add move keys but we no longer do that.
			//
			// Assume we have 3 move keys in L0.
			// - moveKey1 (points to vlog file 10),
			// - moveKey2 (points to vlog file 14) and
			// - moveKey3 (points to vlog file 15).
			//
			// Also, assume there is another move key "moveKey1" (points to
			// vlog file 6) (this is also a move Key for key "FOO" ) on upper
			// levels (let's say 3). The move key "moveKey1" on level 0 was
			// inserted because vlog file 6 was GCed.
			//
			// Here's what the arrangement looks like
			// L0 => (moveKey1 => vlog10), (moveKey2 => vlog14), (moveKey3 => vlog15)
			// L1 => ....
			// L2 => ....
			// L3 => (moveKey1 => vlog6)
			//
			// When L0 compaction runs, it keeps only moveKey3 because the number of versions
			// to keep is set to 1. (we've dropped moveKey1's latest version)
			//
			// The new arrangement of keys is
			// L0 => ....
			// L1 => (moveKey3 => vlog15)
			// L2 => ....
			// L3 => (moveKey1 => vlog6)
			//
			// Now if we try to GC vlog file 10, the entry read from vlog file
			// will point to vlog10 but the entry read from LSM Tree will point
			// to vlog6. The move key read from LSM tree will point to vlog6
			// because we've asked for version 1 of the move key.
			//
			// This might seem like an issue but it's not really an issue
			// because the user has set the number of versions to keep to 1 and
			// the latest version of moveKey points to the correct vlog file
			// and offset. The stale move key on L3 will be eventually dropped
			// by compaction because there is a newer versions in the upper
			// levels.
		}
		return nil
	}

	_, err := f.iterate(vlog.opt.ReadOnly, 0, func(e Entry, vp valuePointer) error { //对Vlog文件内进行迭代
		return fe(e)
	})
	if err != nil {
		return err
	}

	batchSize := 1024
	var loops int
	for i := 0; i < len(wb); { //对于上面那个批处理结束的时候，因未满一个批，仍留在缓冲区中的KV进行处理
		loops++
		if batchSize == 0 {
			vlog.db.opt.Warningf("We shouldn't reach batch size of zero.")
			return ErrNoRewrite
		}
		end := i + batchSize
		if end > len(wb) {
			end = len(wb)
		}
		if err := vlog.db.batchSet(wb[i:end]); err != nil { // 伪装成一次批量的写请求，把未处理的处理掉
			if err == ErrTxnTooBig {
				// Decrease the batch size to half.
				batchSize = batchSize / 2
				continue
			}
			return err
		}
		i += batchSize
	}
	vlog.opt.Infof("Processed %d entries in %d loops", len(wb), loops)
	vlog.opt.Infof("Total entries: %d. Moved: %d", count, moved)
	vlog.opt.Infof("Removing fid: %d", f.fid)
	//下面就是删除老的一整个的Vlog文件（有用的kv在上面已经移动走了）
	var deleteFileNow bool
	// Entries written to LSM. Remove the older file now.
	{
		vlog.filesLock.Lock()
		// Just a sanity-check.
		if _, ok := vlog.filesMap[f.fid]; !ok { //先取出来Fid，没取到的话就报已经被删掉了
			vlog.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", f.fid)
		}
		if vlog.iteratorCount() == 0 {
			delete(vlog.filesMap, f.fid) //先从索引中删除掉
			deleteFileNow = true
		} else {
			vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, f.fid) //记录当前Fid曾经存在，但现在被删除了
		}
		vlog.filesLock.Unlock()
	}

	if deleteFileNow { //是否要现在立即删除Fid
		if err := vlog.deleteLogFile(f); err != nil { //要立即删除
			return err
		}
	}
	return nil
}

func (vlog *valueLog) incrIteratorCount() {
	vlog.numActiveIterators.Add(1)
}

func (vlog *valueLog) iteratorCount() int {
	return int(vlog.numActiveIterators.Load())
}

func (vlog *valueLog) decrIteratorCount() error {
	num := vlog.numActiveIterators.Add(-1)
	if num != 0 {
		return nil
	}

	vlog.filesLock.Lock()
	lfs := make([]*logFile, 0, len(vlog.filesToBeDeleted))
	for _, id := range vlog.filesToBeDeleted {
		lfs = append(lfs, vlog.filesMap[id])
		delete(vlog.filesMap, id)
	}
	vlog.filesToBeDeleted = nil
	vlog.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vlog.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

// 删除Fid对应文件
func (vlog *valueLog) deleteLogFile(lf *logFile) error {
	if lf == nil {
		return nil
	}
	lf.lock.Lock()
	defer lf.lock.Unlock()
	// Delete fid from discard stats as well.
	vlog.discardStats.Update(lf.fid, -1)

	return lf.Delete()
}

func (vlog *valueLog) dropAll() (int, error) {
	// If db is opened in InMemory mode, we don't need to do anything since there are no vlog files.
	if vlog.db.opt.InMemory {
		return 0, nil
	}
	// We don't want to block dropAll on any pending transactions. So, don't worry about iterator
	// count.
	var count int
	deleteAll := func() error {
		vlog.filesLock.Lock()
		defer vlog.filesLock.Unlock()
		for _, lf := range vlog.filesMap {
			if err := vlog.deleteLogFile(lf); err != nil {
				return err
			}
			count++
		}
		vlog.filesMap = make(map[uint32]*logFile)
		vlog.maxFid = 0
		return nil
	}
	if err := deleteAll(); err != nil {
		return count, err
	}

	vlog.db.opt.Infof("Value logs deleted. Creating value log file: 1")
	if _, err := vlog.createVlogFile(); err != nil { // Called while writes are stopped.
		return count, err
	}
	return count, nil
}

func (db *DB) valueThreshold() int64 {
	return db.threshold.valueThreshold.Load()
}

type valueLog struct {
	dirPath string

	// guards our view of which files exist, which to be deleted, how many active iterators
	filesLock        sync.RWMutex
	filesMap         map[uint32]*logFile
	maxFid           uint32
	filesToBeDeleted []uint32
	// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
	// 迭代器的重新计数——当这个值为零时，我们可以删除ToBeDeleted文件。
	numActiveIterators atomic.Int32

	db                *DB
	writableLogOffset atomic.Uint32 // read by read, written by write
	numEntriesWritten uint32
	opt               Options

	garbageCh    chan struct{}
	discardStats *discardStats
}

func vlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.vlog", dirPath, string(os.PathSeparator), fid)
}

func (vlog *valueLog) fpath(fid uint32) string {
	return vlogFilePath(vlog.dirPath, fid)
}

func (vlog *valueLog) populateFilesMap() error {
	vlog.filesMap = make(map[uint32]*logFile)

	files, err := os.ReadDir(vlog.dirPath)
	if err != nil {
		return errFile(err, vlog.dirPath, "Unable to open log dir.")
	}

	found := make(map[uint64]struct{})
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".vlog") { //筛选出后缀为.vlog的文件
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-5], 10, 32)
		if err != nil {
			return errFile(err, file.Name(), "Unable to parse log id.")
		}
		if _, ok := found[fid]; ok {
			return errFile(err, file.Name(), "Duplicate file found. Please delete one.")
		}
		found[fid] = struct{}{}

		lf := &logFile{
			fid:      uint32(fid),
			path:     vlog.fpath(uint32(fid)),
			registry: vlog.db.registry,
		}
		vlog.filesMap[uint32(fid)] = lf
		if vlog.maxFid < uint32(fid) {
			vlog.maxFid = uint32(fid)
		}
	}
	return nil
}

func (vlog *valueLog) createVlogFile() (*logFile, error) {
	fid := vlog.maxFid + 1
	path := vlog.fpath(fid)
	lf := &logFile{
		fid:      fid,
		path:     path,
		registry: vlog.db.registry,
		writeAt:  vlogHeaderSize,
		opt:      vlog.opt,
	}
	err := lf.open(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 2*vlog.opt.ValueLogFileSize) //创建.vlog文件，名字也是类似0001.vlog
	if err != z.NewFile && err != nil {
		return nil, err
	}

	vlog.filesLock.Lock()
	vlog.filesMap[fid] = lf
	y.AssertTrue(vlog.maxFid < fid)
	vlog.maxFid = fid
	// writableLogOffset is only written by write func, by read by Read func.
	// To avoid a race condition, all reads and updates to this variable must be
	// done via atomics.
	vlog.writableLogOffset.Store(vlogHeaderSize)
	vlog.numEntriesWritten = 0
	vlog.filesLock.Unlock()

	return lf, nil
}

func errFile(err error, path string, msg string) error {
	return fmt.Errorf("%s. Path=%s. Error=%v", msg, path, err)
}

// init initializes the value log struct. This initialization needs to happen
// before compactions start.
func (vlog *valueLog) init(db *DB) {
	vlog.opt = db.opt
	vlog.db = db
	// We don't need to open any vlog files or collect stats for GC if DB is opened
	// in InMemory mode. InMemory mode doesn't create any files/directories on disk.
	if vlog.opt.InMemory {
		return
	}
	vlog.dirPath = vlog.opt.ValueDir

	vlog.garbageCh = make(chan struct{}, 1) // Only allow one GC at a time.
	lf, err := InitDiscardStats(vlog.opt)   //这里创建的DISCARD文件是用作GC的
	y.Check(err)
	vlog.discardStats = lf
	// See TestPersistLFDiscardStats for purpose of statement below.
	db.logToSyncChan(endVLogInitMsg)
}

func (vlog *valueLog) open(db *DB) error {
	// We don't need to open any vlog files or collect stats for GC if DB is opened
	// in InMemory mode. InMemory mode doesn't create any files/directories on disk.
	if db.opt.InMemory {
		return nil
	}

	if err := vlog.populateFilesMap(); err != nil {
		return err
	}
	// If no files are found, then create a new file.
	if len(vlog.filesMap) == 0 {
		if vlog.opt.ReadOnly {
			return nil
		}
		_, err := vlog.createVlogFile() //没有Vlog文件，需要创建一个logFile
		return y.Wrapf(err, "Error while creating log file in valueLog.open")
	}
	fids := vlog.sortedFids()  // 按fid排序好vlog文件
	for _, fid := range fids { //  处理排序好的vlog文件
		lf, ok := vlog.filesMap[fid]
		y.AssertTrue(ok)

		// Just open in RDWR mode. This should not create a new log file.
		lf.opt = vlog.opt
		if err := lf.open(vlog.fpath(fid), os.O_RDWR,
			2*vlog.opt.ValueLogFileSize); err != nil {
			return y.Wrapf(err, "Open existing file: %q", lf.path)
		}
		// We shouldn't delete the maxFid file.
		if lf.size.Load() == vlogHeaderSize && fid != vlog.maxFid { //删除空的（即只有头大小的vlog文件）
			vlog.opt.Infof("Deleting empty file: %s", lf.path)
			if err := lf.Delete(); err != nil {
				return y.Wrapf(err, "while trying to delete empty file: %s", lf.path)
			}
			delete(vlog.filesMap, fid)
		}
	}

	if vlog.opt.ReadOnly {
		return nil
	}
	// Now we can read the latest value log file, and see if it needs truncation. We could
	// technically do this over all the value log files, but that would mean slowing down the value
	// log open.
	// 现在我们可以读取最新的值日志文件，并查看它是否需要截断。从技术上讲，我们可以对所有值日志文件执行此操作，但这意味着打开值日志的速度会减慢。
	last, ok := vlog.filesMap[vlog.maxFid]
	y.AssertTrue(ok)
	lastOff, err := last.iterate(vlog.opt.ReadOnly, vlogHeaderSize, //获取实际大小
		func(_ Entry, vp valuePointer) error {
			return nil
		})
	if err != nil {
		return y.Wrapf(err, "while iterating over: %s", last.path)
	}
	if err := last.Truncate(int64(lastOff)); err != nil { //按照实际大小截断这个最新的vlog文件
		return y.Wrapf(err, "while truncating last value log file: %s", last.path)
	}

	// Don't write to the old log file. Always create a new one.
	// 不要写入旧日志文件。总是创建一个新的。
	if _, err := vlog.createVlogFile(); err != nil { // 创建一个新的活跃的vlog文件
		return y.Wrapf(err, "Error while creating log file in valueLog.open")
	}
	return nil
}

func (vlog *valueLog) Close() error {
	if vlog == nil || vlog.db == nil || vlog.db.opt.InMemory {
		return nil
	}

	vlog.opt.Debugf("Stopping garbage collection of values.")
	var err error
	for id, lf := range vlog.filesMap {
		lf.lock.Lock() // We won’t release the lock.
		offset := int64(-1)

		if !vlog.opt.ReadOnly && id == vlog.maxFid {
			offset = int64(vlog.woffset())
		}
		if terr := lf.Close(offset); terr != nil && err == nil {
			err = terr
		}
	}
	if vlog.discardStats != nil {
		vlog.db.captureDiscardStats()
		if terr := vlog.discardStats.Close(-1); terr != nil && err == nil {
			err = terr
		}
	}
	return err
}

// sortedFids returns the file id's not pending deletion, sorted.  Assumes we have shared access to
// filesMap.
func (vlog *valueLog) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range vlog.filesToBeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	ret := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if _, ok := toBeDeleted[fid]; !ok {
			ret = append(ret, fid)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

type request struct {
	// Input values
	Entries []*Entry
	// Output values and wait group stuff below
	Ptrs []valuePointer
	Wg   sync.WaitGroup
	Err  error
	ref  atomic.Int32
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref.Store(0)
}

func (req *request) IncrRef() {
	req.ref.Add(1)
}

func (req *request) DecrRef() {
	nRef := req.ref.Add(-1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}

type requests []*request

func (reqs requests) DecrRef() {
	for _, req := range reqs {
		req.DecrRef()
	}
}

func (reqs requests) IncrRef() {
	for _, req := range reqs {
		req.IncrRef()
	}
}

// sync function syncs content of latest value log file to disk. Syncing of value log directory is
// not required here as it happens every time a value log file rotation happens(check createVlogFile
// function). During rotation, previous value log file also gets synced to disk. It only syncs file
// if fid >= vlog.maxFid. In some cases such as replay(while opening db), it might be called with
// fid < vlog.maxFid. To sync irrespective of file id just call it with math.MaxUint32.
func (vlog *valueLog) sync() error {
	if vlog.opt.SyncWrites || vlog.opt.InMemory {
		return nil
	}

	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	curlf := vlog.filesMap[maxFid]
	// Sometimes it is possible that vlog.maxFid has been increased but file creation
	// with same id is still in progress and this function is called. In those cases
	// entry for the file might not be present in vlog.filesMap.
	if curlf == nil {
		vlog.filesLock.RUnlock()
		return nil
	}
	curlf.lock.RLock()
	vlog.filesLock.RUnlock()

	err := curlf.Sync()
	curlf.lock.RUnlock()
	return err
}

func (vlog *valueLog) woffset() uint32 {
	return vlog.writableLogOffset.Load()
}

// validateWrites will check whether the given requests can fit into 4GB vlog file.
// NOTE: 4GB is the maximum size we can create for vlog because value pointer offset is of type
// uint32. If we create more than 4GB, it will overflow uint32. So, limiting the size to 4GB.
/*
validateWrites将检查给定的请求是否可以放入4GB的vlog文件中。注意：4GB是我们可以为vlog创建的最大大小，因为值指针偏移量的类型是uint32。如果我们创建超过4GB，它将溢出uint32。因此，将大小限制为4GB。
*/
func (vlog *valueLog) validateWrites(reqs []*request) error {
	vlogOffset := uint64(vlog.woffset()) // 这里是得到当前活跃的vlog文件的当前偏移量
	for _, req := range reqs {
		// calculate size of the request.
		size := estimateRequestSize(req)                   //得到单个的req大小
		estimatedVlogOffset := vlogOffset + size           //累加
		if estimatedVlogOffset > uint64(maxVlogFileSize) { //如果累加值大于2的32次方-1
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d", //数据太大，溢出了，已经一个vlog放不下了
				estimatedVlogOffset, maxVlogFileSize)
		}

		if estimatedVlogOffset >= uint64(vlog.opt.ValueLogFileSize) { //如果累加值大于2的30次方-1，注意这里还未到4G，也就是说是提前进行了分割，避免上面的报错
			// We'll create a new vlog file if the estimated offset is greater or equal to
			// max vlog size. So, resetting the vlogOffset.
			// 如果估计的偏移量大于或等于最大vlog大小，我们将创建一个新的vlog文件（即切割）。因此，重置vlogOffset。
			vlogOffset = 0
			continue
		}
		// Estimated vlog offset will become current vlog offset if the vlog is not rotated.
		vlogOffset = estimatedVlogOffset
	}
	return nil
}

// estimateRequestSize returns the size that needed to be written for the given request.
func estimateRequestSize(req *request) uint64 {
	size := uint64(0)
	for _, e := range req.Entries {
		size += uint64(maxHeaderSize + len(e.Key) + len(e.Value) + crc32.Size)
	}
	return size
}

// write is thread-unsafe by design and should not be called concurrently.
func (vlog *valueLog) write(reqs []*request) error { //这里面将数据写入磁盘的vlog中（记住是通过mmap方式写入的）
	if vlog.db.opt.InMemory {
		return nil
	}
	// Validate writes before writing to vlog. Because, we don't want to partially write and return
	// an error.
	if err := vlog.validateWrites(reqs); err != nil { // 进行写请求的检查
		return y.Wrapf(err, "while validating writes")
	}

	vlog.filesLock.RLock()
	maxFid := vlog.maxFid          // 就是Vlog文件的那个前缀数字（取最大的），最大的也是活跃的vlog
	curlf := vlog.filesMap[maxFid] // 表示当前活跃的vlog对象
	vlog.filesLock.RUnlock()

	defer func() {
		if vlog.opt.SyncWrites {
			if err := curlf.Sync(); err != nil {
				vlog.opt.Errorf("Error while curlf sync: %v\n", err)
			}
		}
	}()

	// 一次处理一个kv对（即一个entry）
	write := func(buf *bytes.Buffer) error {
		if buf.Len() == 0 {
			return nil
		}

		n := uint32(buf.Len())
		endOffset := vlog.writableLogOffset.Add(n) // 偏移位置累加
		// Increase the file size if we cannot accommodate this entry.
		// [Aman] Should this be >= or just >? Doesn't make sense to extend the file if it big enough already.
		//如果我们无法容纳此条目，请增加文件大小。
		//[Aman]这应该是>=还是只是>？如果文件已经足够大，扩展它是没有意义的。
		if int(endOffset) >= len(curlf.Data) { // zzlTODO:回来需要再去看看下面这里是干啥的，需要去看Ristretto的使用文档，看函数啥意思（大致是对mmap进行的截断操作）
			if err := curlf.Truncate(int64(endOffset)); err != nil {
				return err
			}
		}

		start := int(endOffset - n)
		y.AssertTrue(copy(curlf.Data[start:], buf.Bytes()) == int(n)) //核心，放入curlf对象内，后续因为是mmap方式再由操作系统放入磁盘即可

		curlf.size.Store(endOffset) //更新偏移位置
		return nil
	}

	//具体落盘操作
	toDisk := func() error {
		if vlog.woffset() > uint32(vlog.opt.ValueLogFileSize) || //如果当前活跃的vlog的偏移量大于最大文件大小限制 或者 当前vlog的写入kv对数量大于最大写入个数限制
			vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries {
			if err := curlf.doneWriting(vlog.woffset()); err != nil { //标志当前vlog文件写满，在该函数内，如果设置了同步则会直接系统调用sync，最后根据offset截断mmap
				return err
			}

			newlf, err := vlog.createVlogFile() //创建新的vlog文件
			if err != nil {
				return err
			}
			curlf = newlf
		}
		return nil
	}

	buf := new(bytes.Buffer)
	//开始真正遍历reqs，每个req里面有一个Entries数组
	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[:0] // 这个数组用来记录处理后的kv对，包含kv分离的以及不分离的，不分离的valuePointer对象为空
		var written, bytesWritten int
		valueSizes := make([]int64, 0, len(b.Entries))
		// 遍历当前请求的kv对数组，挨个执行写入且进行统计
		for j := range b.Entries {
			buf.Reset()

			e := b.Entries[j]
			valueSizes = append(valueSizes, int64(len(e.Value)))     //得到当前单个kv对的v大小
			if e.skipVlogAndSetThreshold(vlog.db.valueThreshold()) { // 是否跳过vlog，为true就是kv全放LSM树
				// valueThreshold就是分大小kv的那个阈值，注意，每个kv对象Entry内都会存一个，所以就算之后改变这个阈值，老系统也能正常运转
				b.Ptrs = append(b.Ptrs, valuePointer{}) // 现在加在这里面的应该就是不用kv分离的
				continue
			}
			var p valuePointer

			p.Fid = curlf.fid
			p.Offset = vlog.woffset() // 得到当前活跃vlog文件的偏移量

			// We should not store transaction marks in the vlog file because it will never have all
			// the entries in a transaction. If we store entries with transaction marks then value
			// GC will not be able to iterate on the entire vlog file.
			// But, we still want the entry to stay intact for the memTable WAL. So, store the meta
			// in a temporary variable and reassign it after writing to the value log.
			// 我们不应该在vlog文件中存储事务标记，因为它永远不会包含事务中的所有条目。如果我们用事务标记存储条目，那么value GC将无法迭代整个vlog文件。
			// 但是，我们仍然希望memTable WAL的条目保持不变。因此，将meta存储在临时变量中，并在写入值日志后重新分配。
			tmpMeta := e.meta //这几行是处理元数据信息的
			e.meta = e.meta &^ (bitTxn | bitFinTxn)
			plen, err := curlf.encodeEntry(buf, e, p.Offset) // Now encode the entry into buffer.将每个条目写入缓冲区，并返回长度该条目的长度（e是单个kv对）
			if err != nil {
				return err
			}
			// Restore the meta.
			e.meta = tmpMeta

			p.Len = uint32(plen)               //记录当前kv对的长度
			b.Ptrs = append(b.Ptrs, p)         //将当前放入vlog的kv对的信息记录起来
			if err := write(buf); err != nil { // 真正开始将当前kv对转换的字节流执行写入
				return err
			}
			written++                 //已写入的个数累计
			bytesWritten += buf.Len() //已写入的字节流长度
			// No need to flush anything, we write to file directly via mmap.
			// 无需刷新任何内容，我们直接通过mmap写入文件。
		}
		y.NumWritesVlogAdd(vlog.opt.MetricsEnabled, int64(written)) //这行还有下一行就是一个计数写入vlog的统计量
		y.NumBytesWrittenVlogAdd(vlog.opt.MetricsEnabled, int64(bytesWritten))

		vlog.numEntriesWritten += uint32(written)
		vlog.db.threshold.update(valueSizes)
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		if err := toDisk(); err != nil {
			return err
		}
	}
	return toDisk()
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (vlog *valueLog) getFileRLocked(vp valuePointer) (*logFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	ret, ok := vlog.filesMap[vp.Fid]
	if !ok {
		// log file has gone away, we can't do anything. Return.
		return nil, errors.Errorf("file with ID: %d not found", vp.Fid)
	}

	// Check for valid offset if we are reading from writable log.
	maxFid := vlog.maxFid
	// In read-only mode we don't need to check for writable offset as we are not writing anything.
	// Moreover, this offset is not set in readonly mode.
	if !vlog.opt.ReadOnly && vp.Fid == maxFid {
		currentOffset := vlog.woffset()
		if vp.Offset >= currentOffset {
			return nil, errors.Errorf(
				"Invalid value pointer offset: %d greater than current offset: %d",
				vp.Offset, currentOffset)
		}
	}

	ret.lock.RLock()
	return ret, nil
}

// Read reads the value log at a given location.
// TODO: Make this read private.
func (vlog *valueLog) Read(vp valuePointer, _ *y.Slice) ([]byte, func(), error) {
	buf, lf, err := vlog.readValueBytes(vp) //关键
	// log file is locked so, decide whether to lock immediately or let the caller to
	// unlock it, after caller uses it.
	cb := vlog.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
	}

	if vlog.opt.VerifyValueChecksum {
		hash := crc32.New(y.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			runCallback(cb)
			return nil, nil, y.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		// Fetch checksum from the end of the buffer.
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != y.BytesToU32(checksum) {
			runCallback(cb)
			return nil, nil, y.Wrapf(y.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}
	var h header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	if lf.encryptionEnabled() {
		kv, err = lf.decryptKV(kv, vp.Offset)
		if err != nil {
			return nil, cb, err
		}
	}
	if uint32(len(kv)) < h.klen+h.vlen {
		vlog.db.opt.Errorf("Invalid read: vp: %+v", vp)
		return nil, nil, errors.Errorf("Invalid read: Len: %d read at:[%d:%d]",
			len(kv), h.klen, h.klen+h.vlen)
	}
	return kv[h.klen : h.klen+h.vlen], cb, nil
}

// getUnlockCallback will returns a function which unlock the logfile if the logfile is mmaped.
// otherwise, it unlock the logfile and return nil.
func (vlog *valueLog) getUnlockCallback(lf *logFile) func() {
	if lf == nil {
		return nil
	}
	return lf.lock.RUnlock
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (vlog *valueLog) readValueBytes(vp valuePointer) ([]byte, *logFile, error) {
	lf, err := vlog.getFileRLocked(vp) //上一个文件锁
	if err != nil {
		return nil, nil, err
	}

	buf, err := lf.read(vp) // 这里就是真正的去读了
	y.NumReadsVlogAdd(vlog.db.opt.MetricsEnabled, 1)
	y.NumBytesReadsVlogAdd(vlog.db.opt.MetricsEnabled, int64(len(buf)))
	return buf, lf, err
}

func (vlog *valueLog) pickLog(discardRatio float64) *logFile {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()

LOOP:
	// Pick a candidate that contains the largest amount of discardable data
	fid, discard := vlog.discardStats.MaxDiscard() //discard是在磁盘中的一个文件，其会标记哪些KV是可以回收的，也记录 FID:该文件中垃圾KV对数量（每个关系就是一个16bit的slot），而这行是获取一个可回收KV数量最多的日志段

	// MaxDiscard will return fid=0 if it doesn't have any discard data. The
	// vlog files start from 1.
	if fid == 0 { //没有需要回收的垃圾
		vlog.opt.Debugf("No file with discard stats")
		return nil
	}
	lf, ok := vlog.filesMap[fid] //取相应的Vlog文件
	// This file was deleted but it's discard stats increased because of compactions. The file
	// doesn't exist so we don't need to do anything. Skip it and retry.
	if !ok { //如果没取到vlog文件
		vlog.discardStats.Update(fid, -1)
		goto LOOP
	}
	// We have a valid file.
	fi, err := lf.Fd.Stat()
	if err != nil {
		vlog.opt.Errorf("Unable to get stats for value log fid: %d err: %+v", fi, err)
		return nil
	}
	if thr := discardRatio * float64(fi.Size()); float64(discard) < thr { // 如果实际需要回收的键值对数量小于设定的阈值
		vlog.opt.Debugf("Discard: %d less than threshold: %.0f for file: %s",
			discard, thr, fi.Name())
		return nil
	}
	if fid < vlog.maxFid { //如果fid小于最大的fid
		// 则代表这个fid是有效的，下面将需要回收的VLOG文件返回
		vlog.opt.Infof("Found value log max discard fid: %d discard: %d\n", fid, discard)
		lf, ok := vlog.filesMap[fid] //再次取相应的Vlog文件（可能是为了并发检查）
		y.AssertTrue(ok)
		return lf
	}

	// Don't randomly pick any value log file.
	return nil
}

func discardEntry(e Entry, vs y.ValueStruct, db *DB) bool {
	if vs.Version != y.ParseTs(e.Key) { // vs.Version是LSM的版本，y.ParseTs(e.Key)得到的是GC里面的版本，只要不等于，就把VLOG里面的当成垃圾回收掉
		// Version not found. Discard.
		return true
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) { // 如果当前key已经过期或者被删除，也返回true
		return true
	}
	if (vs.Meta & bitValuePointer) == 0 { //判断当前kv是否已经kv分离，等于0代表没有进行KV分离
		// Key also stores the value in LSM. Discard.
		return true
	}
	if (vs.Meta & bitFinTxn) > 0 { //事务终止时创建的key也返回true，丢弃
		// Just a txn finish entry. Discard.
		return true
	}
	return false //返回不可回收
}

func (vlog *valueLog) doRunGC(lf *logFile) error {
	_, span := otrace.StartSpan(context.Background(), "Badger.GC")
	span.Annotatef(nil, "GC rewrite for: %v", lf.path)
	defer span.End()
	if err := vlog.rewrite(lf); err != nil { //对Vlog进行重写
		return err
	}
	// Remove the file from discardStats.
	vlog.discardStats.Update(lf.fid, -1)
	return nil
}

func (vlog *valueLog) waitOnGC(lc *z.Closer) {
	defer lc.Done()

	<-lc.HasBeenClosed() // Wait for lc to be closed.

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	vlog.garbageCh <- struct{}{}
}

func (vlog *valueLog) runGC(discardRatio float64) error {
	select {
	case vlog.garbageCh <- struct{}{}: //这两行做了GC的拥塞控制，同一时间只能有一个GC在运行
		// Pick a log file for GC.
		defer func() {
			<-vlog.garbageCh
		}()

		lf := vlog.pickLog(discardRatio) // 选择一个Vlog文件（这里得到的是Vlog文件的FID，并不是真的Vlog文件，所以全部是在内存操作的）
		if lf == nil {
			return ErrNoRewrite
		}
		return vlog.doRunGC(lf)
	default:
		return ErrRejected //GC被拒绝
	}
}

func (vlog *valueLog) updateDiscardStats(stats map[uint32]int64) {
	if vlog.opt.InMemory {
		return
	}
	for fid, discard := range stats {
		vlog.discardStats.Update(fid, discard) //将累计的垃圾数据个数更新到统计表中
	}
	// The following is to coordinate with some test cases where we want to
	// verify that at least one iteration of updateDiscardStats has been completed.
	vlog.db.logToSyncChan(updateDiscardStatsMsg)
}

type vlogThreshold struct {
	logger         Logger
	percentile     float64
	valueThreshold atomic.Int64
	valueCh        chan []int64
	clearCh        chan bool
	closer         *z.Closer
	// Metrics contains a running log of statistics like amount of data stored etc.
	vlMetrics *z.HistogramData
}

func initVlogThreshold(opt *Options) *vlogThreshold {
	getBounds := func() []float64 {
		mxbd := opt.maxValueThreshold
		mnbd := float64(opt.ValueThreshold)
		y.AssertTruef(mxbd >= mnbd, "maximum threshold bound is less than the min threshold")
		size := math.Min(mxbd-mnbd+1, 1024.0)
		bdstp := (mxbd - mnbd) / size
		bounds := make([]float64, int64(size))
		for i := range bounds {
			if i == 0 {
				bounds[0] = mnbd
				continue
			}
			if i == int(size-1) {
				bounds[i] = mxbd
				continue
			}
			bounds[i] = bounds[i-1] + bdstp
		}
		return bounds
	}
	lt := &vlogThreshold{
		logger:     opt.Logger,
		percentile: opt.VLogPercentile,
		valueCh:    make(chan []int64, 1000),
		clearCh:    make(chan bool, 1),
		closer:     z.NewCloser(1),
		vlMetrics:  z.NewHistogramData(getBounds()),
	}
	lt.valueThreshold.Store(opt.ValueThreshold)
	return lt
}

func (v *vlogThreshold) Clear(opt Options) {
	v.valueThreshold.Store(opt.ValueThreshold)
	v.clearCh <- true
}

func (v *vlogThreshold) update(sizes []int64) {
	v.valueCh <- sizes
}

func (v *vlogThreshold) close() {
	v.closer.SignalAndWait()
}

func (v *vlogThreshold) listenForValueThresholdUpdate() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.HasBeenClosed():
			return
		case val := <-v.valueCh:
			for _, e := range val {
				v.vlMetrics.Update(e)
			}
			// we are making it to get Options.VlogPercentile so that values with sizes
			// in range of Options.VlogPercentile will make it to the LSM tree and rest to the
			// value log file.
			p := int64(v.vlMetrics.Percentile(v.percentile))
			if v.valueThreshold.Load() != p {
				if v.logger != nil {
					v.logger.Infof("updating value of threshold to: %d", p)
				}
				v.valueThreshold.Store(p)
			}
		case <-v.clearCh:
			v.vlMetrics.Clear()
		}
	}
}
