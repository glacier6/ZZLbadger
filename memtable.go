/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"bufio"
	"bytes"
	"crypto/aes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// memTable structure stores a skiplist and a corresponding WAL. Writes to memTable are written
// both to the WAL and the skiplist. On a crash, the WAL is replayed to bring the skiplist back to
// its pre-crash form.
type memTable struct {
	// TODO: Give skiplist z.Calloc'd []byte.
	sl         *skl.Skiplist
	wal        *logFile
	maxVersion uint64
	opt        Options
	buf        *bytes.Buffer
}

func (db *DB) openMemTables(opt Options) error {
	// We don't need to open any tables in in-memory mode.
	if db.opt.InMemory {
		return nil
	}
	files, err := os.ReadDir(db.opt.Dir) // 把LSM目录下所有文件读取出来
	if err != nil {
		return errFile(err, db.opt.Dir, "Unable to open mem dir.")
	}

	var fids []int
	for _, file := range files { // 循环找.mem结尾的文件，然后把文件ID放到fids
		if !strings.HasSuffix(file.Name(), memFileExt) { // 检查当前LSM目录下文件名是否包含".mem"，包含的才继续执行当轮循环，一般这个文件名字类似为001.mem，这个文件的内容就是LSM Tree的预写日志文件
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:fsz-len(memFileExt)], 10, 64)
		if err != nil {
			return errFile(err, file.Name(), "Unable to parse log id.")
		}
		fids = append(fids, int(fid))
	}

	// Sort in ascending order.
	sort.Slice(fids, func(i, j int) bool { // 把当前得到的.mem文件按照fid进行排序
		return fids[i] < fids[j]
	})
	for _, fid := range fids { //遍历前面取到的mem文件
		flags := os.O_RDWR
		if db.opt.ReadOnly {
			flags = os.O_RDONLY
		}
		mt, err := db.openMemTable(fid, flags) // 打开当前的.mem文件，并把数据加载进来
		if err != nil {
			return y.Wrapf(err, "while opening fid: %d", fid)
		}
		// If this memtable is empty we don't need to add it. This is a
		// memtable that was completely truncated.
		// 如果此memtable为空，则不需要添加它。这是一个完全截断的内存表。
		if mt.sl.Empty() {
			mt.DecrRef()
			continue
		}
		// These should no longer be written to. So, make them part of the imm.
		// 这些不应该再写了。所以，让它们成为imm的一部分。
		db.imm = append(db.imm, mt)
	}
	if len(fids) != 0 {
		db.nextMemFid = fids[len(fids)-1]
	}
	db.nextMemFid++ //设置下一个.mem文件的id
	return nil
}

const memFileExt string = ".mem"

func (db *DB) openMemTable(fid, flags int) (*memTable, error) {
	filepath := db.mtFilePath(fid)
	s := skl.NewSkiplist(arenaSize(db.opt)) //创建跳表
	mt := &memTable{                        // 创建memTable对象
		sl:  s, //这个指跳表
		opt: db.opt,
		buf: &bytes.Buffer{},
	}
	// We don't need to create the wal for the skiplist in in-memory mode so return the mt.
	if db.opt.InMemory {
		return mt, z.NewFile
	}

	mt.wal = &logFile{ // 创建当前Memtable的预写日志结构，内含有一个mmap
		fid:      uint32(fid),
		path:     filepath,
		registry: db.registry,
		writeAt:  vlogHeaderSize,
		opt:      db.opt,
	}
	lerr := mt.wal.open(filepath, flags, 2*db.opt.MemTableSize) // 当前函数的核心操作，主要是将.mem文件关联为mmap
	if lerr != z.NewFile && lerr != nil {
		return nil, y.Wrapf(lerr, "While opening memtable: %s", filepath)
	}

	// Have a callback set to delete WAL when skiplist reference count goes down to zero. That is,
	// when it gets flushed to L0.
	s.OnClose = func() {
		if err := mt.wal.Delete(); err != nil {
			db.opt.Errorf("while deleting file: %s, err: %v", filepath, err)
		}
	}

	if lerr == z.NewFile {
		return mt, lerr
	}
	err := mt.UpdateSkipList()
	return mt, y.Wrapf(err, "while updating skiplist")
}

func (db *DB) newMemTable() (*memTable, error) {
	mt, err := db.openMemTable(db.nextMemFid, os.O_CREATE|os.O_RDWR) //创建一个新的.mem文件
	if err == z.NewFile {
		db.nextMemFid++
		return mt, nil
	}

	if err != nil {
		db.opt.Errorf("Got error: %v for id: %d\n", err, db.nextMemFid)
		return nil, y.Wrapf(err, "newMemTable")
	}
	return nil, errors.Errorf("File %s already exists", mt.wal.Fd.Name())
}

func (db *DB) mtFilePath(fid int) string {
	return filepath.Join(db.opt.Dir, fmt.Sprintf("%05d%s", fid, memFileExt))
}

func (mt *memTable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *memTable) isFull() bool {
	if mt.sl.MemSize() >= mt.opt.MemTableSize {
		return true
	}
	if mt.opt.InMemory {
		// InMemory mode doesn't have any WAL.
		return false
	}
	return int64(mt.wal.writeAt) >= mt.opt.MemTableSize
}

func (mt *memTable) Put(key []byte, value y.ValueStruct) error {
	entry := &Entry{
		Key:       key,
		Value:     value.Value,
		UserMeta:  value.UserMeta,
		meta:      value.Meta,
		ExpiresAt: value.ExpiresAt,
	}

	// wal is nil only when badger in running in in-memory mode and we don't need the wal.
	// 只有当badgerDB处于内存运行模式时，wal才为零，我们不需要wal
	if mt.wal != nil {
		// If WAL exceeds opt.ValueLogFileSize, we'll force flush the memTable. See logic in
		// ensureRoomForWrite.
		// 如果WAL超过opt.ValueLogFileSize，我们将强制刷新memTable。
		if err := mt.wal.writeEntry(mt.buf, entry, mt.opt); err != nil { //写入预写日志（核心操作）
			return y.Wrapf(err, "cannot write entry to WAL file")
		}
	}
	// We insert the finish marker in the WAL but not in the memtable.
	if entry.meta&bitFinTxn > 0 {
		return nil
	}

	// Write to skiplist and update maxVersion encountered.
	mt.sl.Put(key, value)                               //这里的SL就是跳表了！！把KV压入跳表（核心操作）
	if ts := y.ParseTs(entry.Key); ts > mt.maxVersion { //更新memtable内存储key的最大版本号
		mt.maxVersion = ts
	}
	y.NumBytesWrittenToL0Add(mt.opt.MetricsEnabled, entry.estimateSizeAndSetThreshold(mt.opt.ValueThreshold))
	return nil
}

func (mt *memTable) UpdateSkipList() error {
	if mt.wal == nil || mt.sl == nil {
		return nil
	}
	endOff, err := mt.wal.iterate(true, 0, mt.replayFunction(mt.opt))
	if err != nil {
		return y.Wrapf(err, "while iterating wal: %s", mt.wal.Fd.Name())
	}
	if endOff < mt.wal.size.Load() && mt.opt.ReadOnly {
		return y.Wrapf(ErrTruncateNeeded, "end offset: %d < size: %d", endOff, mt.wal.size.Load())
	}
	return mt.wal.Truncate(int64(endOff))
}

// IncrRef increases the refcount
func (mt *memTable) IncrRef() {
	mt.sl.IncrRef()
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (mt *memTable) DecrRef() {
	mt.sl.DecrRef()
}

func (mt *memTable) replayFunction(opt Options) func(Entry, valuePointer) error {
	first := true
	return func(e Entry, _ valuePointer) error { // Function for replaying.
		if first {
			opt.Debugf("First key=%q\n", e.Key)
		}
		first = false
		if ts := y.ParseTs(e.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		v := y.ValueStruct{
			Value:     e.Value,
			Meta:      e.meta,
			UserMeta:  e.UserMeta,
			ExpiresAt: e.ExpiresAt,
		}
		// This is already encoded correctly. Value would be either a vptr, or a full value
		// depending upon how big the original value was. Skiplist makes a copy of the key and
		// value.
		mt.sl.Put(e.Key, v)
		return nil
	}
}

// vlog文件以及wal日志均是这个类型的文件
type logFile struct {
	*z.MmapFile
	path string
	// This is a lock on the log file. It guards the fd’s value, the file’s
	// existence and the file’s memory map.
	// 这是对日志文件的锁定。它保护fd的值、文件的存在和文件的内存映射。

	// Use shared ownership when reading/writing the file or memory map, use
	// exclusive ownership to open/close the descriptor, unmap or remove the file.
	// 在读取/写入文件或内存映射时使用共享所有权，使用独占所有权打开/关闭描述符，取消映射或删除文件。
	lock     sync.RWMutex
	fid      uint32
	size     atomic.Uint32
	dataKey  *pb.DataKey
	baseIV   []byte
	registry *KeyRegistry
	writeAt  uint32
	opt      Options
}

// Truncate截断
func (lf *logFile) Truncate(end int64) error {
	if fi, err := lf.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", lf.Fd.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	y.AssertTrue(!lf.opt.ReadOnly)
	lf.size.Store(uint32(end))
	return lf.MmapFile.Truncate(end)
}

// encodeEntry will encode entry to the buf
// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
func (lf *logFile) encodeEntry(buf *bytes.Buffer, e *Entry, offset uint32) (int, error) {
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	hash := crc32.New(y.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	y.Check2(writer.Write(headerEnc[:sz]))
	// we'll encrypt only key and value.
	if lf.encryptionEnabled() {
		// TODO: no need to allocate the bytes. we can calculate the encrypted buf one by one
		// since we're using ctr mode of AES encryption. Ordering won't changed. Need some
		// refactoring in XORBlock which will work like stream cipher.
		eBuf := make([]byte, 0, len(e.Key)+len(e.Value))
		eBuf = append(eBuf, e.Key...)
		eBuf = append(eBuf, e.Value...)
		if err := y.XORBlockStream(
			writer, eBuf, lf.dataKey.Data, lf.generateIV(offset)); err != nil {
			return 0, y.Wrapf(err, "Error while encoding entry for vlog.")
		}
	} else {
		// Encryption is disabled so writing directly to the buffer.
		y.Check2(writer.Write(e.Key))
		y.Check2(writer.Write(e.Value))
	}
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	y.Check2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (lf *logFile) writeEntry(buf *bytes.Buffer, e *Entry, opt Options) error {
	buf.Reset()
	plen, err := lf.encodeEntry(buf, e, lf.writeAt)
	if err != nil {
		return err
	}
	y.AssertTrue(plen == copy(lf.Data[lf.writeAt:], buf.Bytes()))
	lf.writeAt += uint32(plen)

	lf.zeroNextEntry()
	return nil
}

func (lf *logFile) decodeEntry(buf []byte, offset uint32) (*Entry, error) {
	var h header
	hlen := h.Decode(buf)
	kv := buf[hlen:]
	if lf.encryptionEnabled() {
		var err error
		// No need to worry about mmap. because, XORBlock allocates a byte array to do the
		// xor. So, the given slice is not being mutated.
		if kv, err = lf.decryptKV(kv, offset); err != nil {
			return nil, err
		}
	}
	e := &Entry{
		meta:      h.meta,
		UserMeta:  h.userMeta,
		ExpiresAt: h.expiresAt,
		offset:    offset,
		Key:       kv[:h.klen],
		Value:     kv[h.klen : h.klen+h.vlen],
	}
	return e, nil
}

func (lf *logFile) decryptKV(buf []byte, offset uint32) ([]byte, error) {
	return y.XORBlockAllocate(buf, lf.dataKey.Data, lf.generateIV(offset))
}

// KeyID returns datakey's ID.
func (lf *logFile) keyID() uint64 {
	if lf.dataKey == nil {
		// If there is no datakey, then we'll return 0. Which means no encryption.
		return 0
	}
	return lf.dataKey.KeyId
}

func (lf *logFile) encryptionEnabled() bool {
	return lf.dataKey != nil
}

// Acquire lock on mmap/file if you are calling this
func (lf *logFile) read(p valuePointer) (buf []byte, err error) { //真正在vlog中读取value的函数
	offset := p.Offset
	// Do not convert size to uint32, because the lf.Data can be of size
	// 4GB, which overflows the uint32 during conversion to make the size 0,
	// causing the read to fail with ErrEOF. See issue #585.
	size := int64(len(lf.Data))
	valsz := p.Len
	lfsz := lf.size.Load()
	if int64(offset) >= size || int64(offset+valsz) > size || //防止越界
		// Ensure that the read is within the file's actual size. It might be possible that
		// the offset+valsz length is beyond the file's actual size. This could happen when
		// dropAll and iterations are running simultaneously.
		int64(offset+valsz) > int64(lfsz) {
		err = y.ErrEOF
	} else {
		buf = lf.Data[offset : offset+valsz]
	}
	return buf, err
}

// generateIV will generate IV by appending given offset with the base IV.
func (lf *logFile) generateIV(offset uint32) []byte {
	iv := make([]byte, aes.BlockSize)
	// baseIV is of 12 bytes.
	y.AssertTrue(12 == copy(iv[:12], lf.baseIV))
	// remaining 4 bytes is obtained from offset.
	binary.BigEndian.PutUint32(iv[12:], offset)
	return iv
}

func (lf *logFile) doneWriting(offset uint32) error {
	if lf.opt.SyncWrites { // 如果配置了同步写
		if err := lf.Sync(); err != nil { // 就进行同步落盘
			return y.Wrapf(err, "Unable to sync value log: %q", lf.path)
		}
	}

	// Before we were acquiring a lock here on lf.lock, because we were invalidating the file
	// descriptor due to reopening it as read-only. Now, we don't invalidate the fd, but unmap it,
	// truncate it and remap it. That creates a window where we have segfaults because the mmap is
	// no longer valid, while someone might be reading it. Therefore, we need a lock here again.
	// 在我们获取lf.lock上的锁之前，因为我们以只读方式重新打开文件描述符，导致文件描述符无效。现在，我们不会使fd无效，而是取消映射、截断并重新映射它。
	// 这会创建一个窗口，在那里我们有segfaults，因为mmap不再有效，而有人可能正在读取它。因此，我们再次需要一个锁。
	lf.lock.Lock()
	defer lf.lock.Unlock()

	if err := lf.Truncate(int64(offset)); err != nil { // 截断Mmap文件
		return y.Wrapf(err, "Unable to truncate file: %q", lf.path)
	}

	// Previously we used to close the file after it was written and reopen it in read-only mode.
	// We no longer open files in read-only mode. We keep all vlog files open in read-write mode.
	return nil
}

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
// iterate迭代日志文件。它不会为每个KV对分配新的内存。
// 因此，kv对仅在函数调用期间有效。
// 下面这个就是从Vlog文件中拿到KV数据，然后进行编解码，序列化到对象，然后方便外面的fe函数进行调用
func (lf *logFile) iterate(readOnly bool, offset uint32, fn logEntry) (uint32, error) {
	if offset == 0 {
		// If offset is set to zero, let's advance past the encryption key header.
		offset = vlogHeaderSize //跳过存元数据信息之类的head，共20字节
	}

	// For now, read directly from file, because it allows
	reader := bufio.NewReader(lf.NewReader(int(offset)))
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}

	var lastCommit uint64
	var validEndOffset uint32 = offset

	var entries []*Entry
	var vptrs []valuePointer

loop:
	for {
		e, err := read.Entry(reader)
		switch {
		// We have not reached the end of the file but the entry we read is
		// zero. This happens because we have truncated the file and
		// zero'ed it out.
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == errTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		case e.isZero():
			break loop
		}

		//下面这五行代码就是对值指针的一个拼接
		var vp valuePointer
		vp.Len = uint32(e.hlen + len(e.Key) + len(e.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = e.offset
		vp.Fid = lf.fid

		switch {
		case e.meta&bitTxn > 0:
			txnTs := y.ParseTs(e.Key)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			if lastCommit != txnTs {
				break loop
			}
			entries = append(entries, e)
			vptrs = append(vptrs, vp)

		case e.meta&bitFinTxn > 0:
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil || lastCommit != txnTs {
				break loop
			}
			// Got the end of txn. Now we can store them.
			lastCommit = 0
			validEndOffset = read.recordOffset

			for i, e := range entries {
				vp := vptrs[i]
				if err := fn(*e, vp); err != nil {
					if err == errStop {
						break
					}
					return 0, errFile(err, lf.path, "Iteration function")
				}
			}
			entries = entries[:0]
			vptrs = vptrs[:0]

		default:
			if lastCommit != 0 {
				// This is most likely an entry which was moved as part of GC.
				// We shouldn't get this entry in the middle of a transaction.
				break loop
			}
			validEndOffset = read.recordOffset

			if err := fn(*e, vp); err != nil {
				if err == errStop {
					break
				}
				return 0, errFile(err, lf.path, "Iteration function")
			}
		}
	}
	return validEndOffset, nil
}

// Zero out the next entry to deal with any crashes.
// 清空下一个条目以处理任何崩溃。
func (lf *logFile) zeroNextEntry() {
	z.ZeroOut(lf.Data, int(lf.writeAt), int(lf.writeAt+maxHeaderSize))
}

func (lf *logFile) open(path string, flags int, fsize int64) error {
	mf, ferr := z.OpenMmapFile(path, flags, int(fsize)) // 打开或创建一个mmap文件（关联到.Mem文件）
	lf.MmapFile = mf                                    //每个vlog对应一个MmapFile

	if ferr == z.NewFile {
		if err := lf.bootstrap(); err != nil {
			os.Remove(path)
			return err
		}
		lf.size.Store(vlogHeaderSize)

	} else if ferr != nil {
		return y.Wrapf(ferr, "while opening file: %s", path)
	}
	lf.size.Store(uint32(len(lf.Data)))

	if lf.size.Load() < vlogHeaderSize { // 如果log文件大小小于最小头大小
		// Every vlog file should have at least vlogHeaderSize. If it is less than vlogHeaderSize
		// then it must have been corrupted. But no need to handle here. log replayer will truncate
		// and bootstrap the logfile. So ignoring here.
		// 每个vlog文件都应该至少有vlogHeaderSize。如果它小于vlogHeaderSize，那么它一定已损坏。但这里不需要处理。日志重放器将截断并引导日志文件。所以忽略这里。
		return nil
	}

	// Copy over the encryption registry data.
	// 复制加密注册表数据。
	buf := make([]byte, vlogHeaderSize)

	y.AssertTruef(vlogHeaderSize == copy(buf, lf.Data),
		"Unable to copy from %s, size %d", path, lf.size.Load())
	keyID := binary.BigEndian.Uint64(buf[:8])
	// retrieve datakey.
	// 检索datakey
	if dk, err := lf.registry.DataKey(keyID); err != nil {
		return y.Wrapf(err, "While opening vlog file %d", lf.fid)
	} else {
		lf.dataKey = dk
	}
	lf.baseIV = buf[8:]
	y.AssertTrue(len(lf.baseIV) == 12)

	// Preserved ferr so we can return if this was a new file.
	return ferr
}

// bootstrap will initialize the log file with key id and baseIV.
// The below figure shows the layout of log file.
// +----------------+------------------+------------------+
// | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
// +----------------+------------------+------------------+
func (lf *logFile) bootstrap() error {
	var err error

	// generate data key for the log file.
	var dk *pb.DataKey
	if dk, err = lf.registry.LatestDataKey(); err != nil {
		return y.Wrapf(err, "Error while retrieving datakey in logFile.bootstarp")
	}
	lf.dataKey = dk

	// We'll always preserve vlogHeaderSize for key id and baseIV.
	buf := make([]byte, vlogHeaderSize)

	// write key id to the buf.
	// key id will be zero if the logfile is in plain text.
	binary.BigEndian.PutUint64(buf[:8], lf.keyID())
	// generate base IV. It'll be used with offset of the vptr to encrypt the entry.
	if _, err := cryptorand.Read(buf[8:]); err != nil {
		return y.Wrapf(err, "Error while creating base IV, while creating logfile")
	}

	// Initialize base IV.
	lf.baseIV = buf[8:]
	y.AssertTrue(len(lf.baseIV) == 12)

	// Copy over to the logFile.
	y.AssertTrue(vlogHeaderSize == copy(lf.Data[0:], buf))

	// Zero out the next entry.
	lf.zeroNextEntry()
	return nil
}
