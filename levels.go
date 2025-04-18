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
	stderrors "errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

type levelsController struct {
	nextFileID atomic.Uint64
	l0stallsMs atomic.Int64

	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	cstatus compactStatus // 合并状态
}

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest. idMap is a set of table file id's that were read from the directory
// listing.
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			kv.opt.Debugf("Table file %d not referenced in MANIFEST\n", id)
			filename := table.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func newLevelsController(db *DB, mf *Manifest) (*levelsController, error) {
	y.AssertTrue(db.opt.NumLevelZeroTablesStall > db.opt.NumLevelZeroTables) //断言
	s := &levelsController{
		kv:     db,
		levels: make([]*levelHandler, db.opt.MaxLevels), //创建level切片（动态数组），下标就是第几层，默认总共是7层，levelHandler管理SSTable
	}
	s.cstatus.tables = make(map[uint64]struct{})                     // cstatus表合并状态
	s.cstatus.levels = make([]*levelCompactStatus, db.opt.MaxLevels) // levelCompactStatus表层级合并状态，内有keyRange

	for i := 0; i < db.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(db, i) //每层一个LevelHandler，管理当前层的SSTable
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	if db.opt.InMemory {
		return s, nil
	}
	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(db, mf, getIDMap(db.opt.Dir)); err != nil {
		return nil, err
	}

	var mu sync.Mutex
	tables := make([][]*table.Table, db.opt.MaxLevels)
	var maxFileID uint64

	// We found that using 3 goroutines allows disk throughput to be utilized to its max.
	// Disk utilization is the main thing we should focus on, while trying to read the data. That's
	// the one factor that remains constant between HDD and SSD.
	//我们发现，使用3个goroutines可以最大限度地利用磁盘吞吐量。
	//在尝试读取数据时，磁盘利用率是我们应该关注的主要问题。那是
	//这是HDD和SSD之间保持不变的一个因素。
	throttle := y.NewThrottle(3)

	start := time.Now()
	var numOpened atomic.Int32
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for fileID, tf := range mf.Tables { //从清单文件中取出各个Table,并且初始化每一个.sst结尾的文件关联为一个mmap文件
		fname := table.NewFilename(fileID, db.opt.Dir)
		select {
		case <-tick.C:
			db.opt.Infof("%d tables out of %d opened in %s\n", numOpened.Load(),
				len(mf.Tables), time.Since(start).Round(time.Millisecond))
		default:
		}
		if err := throttle.Do(); err != nil {
			closeAllTables(tables)
			return nil, err
		}
		if fileID > maxFileID {
			maxFileID = fileID
		}
		go func(fname string, tf TableManifest) {
			var rerr error
			defer func() {
				throttle.Done(rerr)
				numOpened.Add(1)
			}()
			dk, err := db.registry.DataKey(tf.KeyID)
			if err != nil {
				rerr = y.Wrapf(err, "Error while reading datakey")
				return
			}
			topt := buildTableOptions(db)
			// Explicitly set Compression and DataKey based on how the table was generated.
			topt.Compression = tf.Compression
			topt.DataKey = dk

			mf, err := z.OpenMmapFile(fname, db.opt.getFileFlags(), 0)
			if err != nil {
				rerr = y.Wrapf(err, "Opening file: %q", fname)
				return
			}
			t, err := table.OpenTable(mf, topt)
			if err != nil {
				if strings.HasPrefix(err.Error(), "CHECKSUM_MISMATCH:") {
					db.opt.Errorf(err.Error())
					db.opt.Errorf("Ignoring table %s", mf.Fd.Name())
					// Do not set rerr. We will continue without this table.
				} else {
					rerr = y.Wrapf(err, "Opening table: %q", fname)
				}
				return
			}

			mu.Lock()
			tables[tf.Level] = append(tables[tf.Level], t)
			mu.Unlock()
		}(fname, tf)
	}
	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, err
	}
	db.opt.Infof("All %d tables opened in %s\n", numOpened.Load(), //打印日志
		time.Since(start).Round(time.Millisecond))
	s.nextFileID.Store(maxFileID + 1)
	for i, tbls := range tables {
		s.levels[i].initTables(tbls) //初始化LSM树的各层级的SST，主要是排序，0层按文件ID排序，更高层按KEY
	}

	// Make sure key ranges do not overlap etc.
	// 确保KEY范围不重叠等。
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, y.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	// 同步目录（因为我们至少删除了一些文件，或者之前创建了
	// 清单文件）。
	if err := syncDir(db.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close(-1)
		}
	}
}

func (s *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range s.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dropTree picks all tables from all levels, creates a manifest changeset,
// applies it, and then decrements the refs of these tables, which would result
// in their deletion.
func (s *levelsController) dropTree() (int, error) {
	// First pick all tables, so we can create a manifest changelog.
	var all []*table.Table
	for _, l := range s.levels {
		l.RLock()
		all = append(all, l.tables...)
		l.RUnlock()
	}
	if len(all) == 0 {
		return 0, nil
	}

	// Generate the manifest changes.
	changes := []*pb.ManifestChange{}
	for _, table := range all {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	changeSet := pb.ManifestChangeSet{Changes: changes}
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return 0, err
	}

	// Now that manifest has been successfully written, we can delete the tables.
	for _, l := range s.levels {
		l.Lock()
		l.totalSize = 0
		l.tables = l.tables[:0]
		l.Unlock()
	}
	for _, table := range all {
		if err := table.DecrRef(); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}

// dropPrefix runs a L0->L1 compaction, and then runs same level compaction on the rest of the
// levels. For L0->L1 compaction, it runs compactions normally, but skips over
// all the keys with the provided prefix.
// For Li->Li compactions, it picks up the tables which would have the prefix. The
// tables who only have keys with this prefix are quickly dropped. The ones which have other keys
// are run through MergeIterator and compacted to create new tables. All the mechanisms of
// compactions apply, i.e. level sizes and MANIFEST are updated as in the normal flow.
func (s *levelsController) dropPrefixes(prefixes [][]byte) error {
	opt := s.kv.opt
	// Iterate levels in the reverse order because if we were to iterate from
	// lower level (say level 0) to a higher level (say level 3) we could have
	// a state in which level 0 is compacted and an older version of a key exists in lower level.
	// At this point, if someone creates an iterator, they would see an old
	// value for a key from lower levels. Iterating in reverse order ensures we
	// drop the oldest data first so that lookups never return stale data.
	for i := len(s.levels) - 1; i >= 0; i-- {
		l := s.levels[i]

		l.RLock()
		if l.level == 0 {
			size := len(l.tables)
			l.RUnlock()

			if size > 0 {
				cp := compactionPriority{
					level: 0,
					score: 1.74,
					// A unique number greater than 1.0 does two things. Helps identify this
					// function in logs, and forces a compaction.
					dropPrefixes: prefixes,
				}
				if err := s.doCompact(174, cp); err != nil {
					opt.Warningf("While compacting level 0: %v", err)
					return nil
				}
			}
			continue
		}

		// Build a list of compaction tableGroups affecting all the prefixes we
		// need to drop. We need to build tableGroups that satisfy the invariant that
		// bottom tables are consecutive.
		// tableGroup contains groups of consecutive tables.
		var tableGroups [][]*table.Table
		var tableGroup []*table.Table

		finishGroup := func() {
			if len(tableGroup) > 0 {
				tableGroups = append(tableGroups, tableGroup)
				tableGroup = nil
			}
		}

		for _, table := range l.tables {
			if containsAnyPrefixes(table, prefixes) {
				tableGroup = append(tableGroup, table)
			} else {
				finishGroup()
			}
		}
		finishGroup()

		l.RUnlock()

		if len(tableGroups) == 0 {
			continue
		}
		_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
		span.Annotatef(nil, "Compaction level: %v", l.level)
		span.Annotatef(nil, "Drop Prefixes: %v", prefixes)
		defer span.End()
		opt.Infof("Dropping prefix at level %d (%d tableGroups)", l.level, len(tableGroups))
		for _, operation := range tableGroups {
			cd := compactDef{
				span:         span,
				thisLevel:    l,
				nextLevel:    l,
				top:          nil,
				bot:          operation,
				dropPrefixes: prefixes,
				t:            s.levelTargets(),
			}
			cd.t.baseLevel = l.level
			if err := s.runCompactDef(-1, l.level, cd); err != nil {
				opt.Warningf("While running compact def: %+v. Error: %v", cd, err)
				return err
			}
		}
	}
	return nil
}

func (s *levelsController) startCompact(lc *z.Closer) {
	n := s.kv.opt.NumCompactors // 从配置中拿配置项，默认开4个协程（这个是经过测试的，4个协程进行处理是最好的）
	lc.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		go s.runCompactor(i, lc) //NOTE:核心操作，开启压缩器协程
	}
}

type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

// levelTargets calculates the targets for levels in the LSM tree. The idea comes from Dynamic Level
// Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB. The sizes of levels
// are calculated based on the size of the lowest level, typically L6. So, if L6 size is 1GB, then
// L5 target size is 100MB, L4 target size is 10MB and so on.
//
// L0 files don't automatically go to L1. Instead, they get compacted to Lbase, where Lbase is
// chosen based on the first level which is non-empty from top (check L1 through L6). For an empty
// DB, that would be L6.  So, L0 compactions go to L6, then L5, L4 and so on.
//
// Lbase is advanced to the upper levels when its target size exceeds BaseLevelSize. For
// example, when L6 reaches 1.1GB, then L4 target sizes becomes 11MB, thus exceeding the
// BaseLevelSize of 10MB. L3 would then become the new Lbase, with a target size of 1MB <
// BaseLevelSize.
// levelTargets计算LSM树中层级的目标大小。这个想法来自动态水平尺寸（https://rocksdb.org/blog/2015/07/23/dynamic-level.html）在RocksDB。
// 每一层的大小是基于最低层（通常为L6）的大小动态计算的。所以，如果L6大小为1GB，那么L5目标大小为100MB，L4目标大小为10MB，以此类推。这里的这个大小就是下面的那个BaseLevelSize
//
// L0文件不会自动转到L1。相反，它们被压缩到Lbase，其中Lbase是根据从顶部开始的非空的第一层来选择的（检查L1到L6）。
// 对于一个空DB，这将是L6。因此，L0压缩到L6，然后是L5、L4，以此类推。
//
// 当Lbase的目标大小超过BaseLevelSize时，它会被提升到更高的级别。
// 例如，当L6达到1.1GB时，L4的目标大小变为11MB，从而超过了10MB的BaseLevelSize。L3将成为新的Lbase，目标大小为1MB<BaseLevelSize。
func (s *levelsController) levelTargets() targets {
	// NOTE:这个函数就是每个层会有一个目标大小。然后还有一个基线层的概念，基线层就是倒序找 按当前数据规模估算的当前层大小<=层基础大小 的第一个层级，且该基线层用于L0层压缩时当作压缩的目标层来用，即一般尽量去倒序压缩
	// 注意并不是得到合并到哪个目标层
	adjust := func(sz int64) int64 {
		if sz < s.kv.opt.BaseLevelSize {
			return s.kv.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{ //先初始化了一个目标数组
		targetSz: make([]int64, len(s.levels)), //每一层的目标大小多大，初始状态下 0层的targetSz为0，其余层均为BaseLevelSize 10 << 20，貌似这两行的大小单位是MB
		fileSz:   make([]int64, len(s.levels)), //每一层的文件多大，初始状态下 0层的fileSz为MemTableSize 64 << 20，其余层均为2 << 20
	}
	// DB size is the size of the last level.
	// NOTE:数据库的大小就认为是最底层的大小，注意数据库最底层是没有最大上限的，这里只是依据最底层大小得到现在的数据规模（若非要计算上限，貌似也可以计算，就是当基线层为L1时，则L6层的大小为 基础大小 * 10的5次方，约为2000GB）
	dbSize := s.lastLevel().getTotalSize()   //获取L6的实际大小
	for i := len(s.levels) - 1; i > 0; i-- { //从L6倒序的遍历每一层
		ltarget := adjust(dbSize) //得到层基础大小（BaseLevelSize 初始为10485760 即 10 << 20） 与  按现在数据规模当前层size的应分配大小（最底层就是实际大小） 的较大值
		t.targetSz[i] = ltarget
		if t.baseLevel == 0 && ltarget <= s.kv.opt.BaseLevelSize { //这个就是层号倒序不断地去试，得到首个实际大小比预设的层基础大小（BaseLevelSize）小的作为基线层
			t.baseLevel = i
		}
		dbSize /= int64(s.kv.opt.LevelSizeMultiplier) //得到下一层大小，每次折损10倍（badger默认层间比例为10）
	}

	tsz := s.kv.opt.BaseTableSize // BaseTableSize默认为2097152 即2 << 20
	for i := 0; i < len(s.levels); i++ {
		if i == 0 { //如果是0层，文件大小为配置里面的MemTableSize
			// Use MemTableSize for Level 0. Because at Level 0, we stop compactions based on the
			// number of tables, not the size of the level. So, having a 1:1 size ratio between
			// memtable size and the size of L0 files is better than churning out 32 files per
			// memtable (assuming 64MB MemTableSize and 2MB BaseTableSize).
			// 将MemTableSize用于级别0。因为在级别0，我们停止压缩是根据表的数量而不是级别的大小。
			// 因此，memtable大小和L0文件大小相等比每个内存表产生32个文件要好（假设为64MB MemTableSize和2MB BaseTableSize）。
			t.fileSz[i] = s.kv.opt.MemTableSize
		} else if i <= t.baseLevel { // 如果是在基线层之前，文件大小为配置里面的BaseTableSize
			t.fileSz[i] = tsz
		} else { //如果在基线层之后，每层为上一层的文件大小的10倍
			tsz *= int64(s.kv.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}
	//此时，初始状态下 0层的fileSz为MemTableSize 64 << 20，其余层均为2 << 20

	// Bring the base level down to the last empty level.
	for i := t.baseLevel + 1; i < len(s.levels)-1; i++ { //做一个边界处理，即基线层之后，找到最大的空层，然后把该层号赋值给baseLevel
		if s.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the
	// target size, pick the next level as the base level.
	// 如果基线层为空，且下一级别实际大小小于目标大小，则选择下一级别作为基线层。
	b := t.baseLevel
	lvl := s.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

// id表示的是当前协程编号（从0开始编号，默认0，1，2，3四个协程）
func (s *levelsController) runCompactor(id int, lc *z.Closer) {
	defer lc.Done() //做协程的并发控制（应该是关闭协程）

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond) // 创建一个0~1000ms的随即延迟，使得各个协程执行的时间岔开，即是为了减少协程之间的冲突，来提高并发
	// 计时结束，NewTimer函数会自动向randomDelay.C写入一个数据
	select {
	case <-randomDelay.C: //能取出这个数的话，代表计时到了,取不出来就一直阻塞
	case <-lc.HasBeenClosed(): //当调用Signal（）时，HasBeenColosed会收到信号。然后在这里就会有可以从chan中取得数据，就会把计时器关闭
		randomDelay.Stop()
		return
	}

	moveL0toFront := func(prios []compactionPriority) []compactionPriority { //将L0层的优先级提前
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		//如果idx==-1，我们找不到L0。
		//如果idx==0，那么我们不需要做任何事情。L0已经在前面了。
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}

	// 这个函数是合并的基本函数，其在跳层合并以及普通合并中都会用到，p里面存着从哪压缩到哪
	run := func(p compactionPriority) bool {
		err := s.doCompact(id, p) // NOTE:核心操作，执行压缩器
		switch err {
		case nil:
			return true
		case errFillTables:
			// pass
		default:
			s.kv.opt.Warningf("While running doCompact: %v\n", err)
		}
		return false
	}

	var priosBuffer []compactionPriority //合并优先级数组
	runOnce := func() bool {
		prios := s.pickCompactLevels(priosBuffer) //NOTE: 核心操作，计算出当前的 待压缩层 切片，里面包含哪些层目前需要压缩，且按优先级（adjusted）顺序排序
		defer func() {
			priosBuffer = prios
		}()
		if id == 0 {
			// 0号协程对压缩L0层进行特殊操作，只让0号协程处理L0层
			prios = moveL0toFront(prios) //提高L0层的优先级
		}
		for _, p := range prios { //遍历这个待压缩层切片（这个切片已经按照调整分数adjusted大小排过序了），并取出来每层对应的优先级对象 p
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
				// 让0号协程去处理L0层，不用管调整后的分数（优先级）是如何的
			} else if p.adjusted < 1.0 { //如果调整分数小于1，这里也不压缩了
				break
			}
			if run(p) { //已得到需要合并的层级，跳到合并执行函数
				return true
			}
		}

		return false
	}

	tryLmaxToLmaxCompaction := func() { // 最底层自我合并
		p := compactionPriority{ //合并优先级对象
			level: s.lastLevel().level, //获取最后一层的层号（第6层）
			t:     s.levelTargets(),
		}
		run(p)

	}
	count := 0                                      // 专用于2号合并协程的计数器，达到200执行tryLmaxToLmaxCompaction
	ticker := time.NewTicker(50 * time.Millisecond) //创建一个50ms的时钟
	defer ticker.Stop()
	for { //无限循环
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C: //每50ms执行一次合并 ，上面那几个闭包函数合并时在时间上一般的执行顺序（需要看的顺序）为runOnce（），moveL0toFront（），run（），tryLmaxToLmaxCompaction（）
			count++
			// Each ticker is 50ms so 50*200=10seconds.
			if s.kv.opt.LmaxCompaction && id == 2 && count >= 200 { // 注意只有2号协程才会执行底层自我合并的操作，且每10秒才会执行一次这个操作（LmaxCompaction参数控制这个功能是否开启）
				tryLmaxToLmaxCompaction() //执行最底层自我合并
				count = 0
			} else {
				runOnce() //其余的进行普通的压缩
			}
		case <-lc.HasBeenClosed(): //或者有通知要关闭的时候，关闭掉
			return
		}
	}
}

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

func (s *levelsController) lastLevel() *levelHandler {
	return s.levels[len(s.levels)-1]
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// It tries to reuse priosBuffer to reduce memory allocation,
// passing nil is acceptable, then new memory will be allocated.
// pickCompactLevel决定要压缩哪个级别。priosBuffer内会存
// 基于：https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// 它试图重用priosBuffer来减少内存分配，
// 传递nil是可以接受的，然后将分配新的内存。

// 对于非零层级，分数是层级总大小除以目标大小
// NOTE:原始分数的计算方式为，对于 L0层，分数是当前table数除以最大table数  ，其他层为当前层实际大小/当前层目标大小。
// 我们比较每个层的分数，分数最高的层优先进行压缩。
// NOTE:注意该函数是按原始分数>=1.0来判断要选哪些层进行压缩，而用调整分数大小来排序上一步得的需要压缩的层（但是在该函数调用的地方，还会对adjusted<1的筛选）
func (s *levelsController) pickCompactLevels(priosBuffer []compactionPriority) (prios []compactionPriority) {
	t := s.levelTargets() //NOTE:核心操作，得到基线层序号（baseLevel）以及各层在当前数据规模下目标大小数组（targetSz，最小为 层基础大小BaseLevelSize）还有各层的文件大小数组（fileSz）
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// Grow buffer to fit all levels.
	// 增加缓冲区以适应所有级别。
	if cap(priosBuffer) < len(s.levels) {
		priosBuffer = make([]compactionPriority, 0, len(s.levels))
	}
	prios = priosBuffer[:0] // 清空prios

	// Add L0 priority based on the number of tables.
	// L0的优先级按table数量计算出来（即当前0层table数量/0层最大table数量）。
	addPriority(0, float64(s.levels[0].numTables())/float64(s.kv.opt.NumLevelZeroTables))

	// All other levels use size to calculate priority.
	// 所有其他级别都使用大小来计算优先级（即当前层实际大小/当前层目标大小（用上面levelTargets函数计算得到的targetSz））。
	for i := 1; i < len(s.levels); i++ {
		// Don't consider those tables that are already being compacted right now.
		// 不要考虑那些现在已经被压缩的table。
		delSize := s.cstatus.delSize(i)

		l := s.levels[i]
		sz := l.getTotalSize() - delSize
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	y.AssertTrue(len(prios) == len(s.levels))
	// The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
	// If Li-1 has score > 1.0, then we'll divide Li-1 score by Li. If Li score is >= 1.0, then Li-1
	// score is reduced, which means we'll prioritize the compaction of lower levels (L5, L4 and so
	// on) over the higher levels (L0, L1 and so on). On the other hand, if Li score is < 1.0, then
	// we'll increase the priority of Li-1.
	// Overall what this means is, if the bottom level is already overflowing, then de-prioritize
	// compaction of the above level. If the bottom level is not full, then increase the priority of
	// above level.
	// 以下代码借鉴自PebbleDB，可生成更健康的LSM树结构。
	// 如果Li-1的得分>1.0，则我们将Li-1得分除以Li。
	// 如果Li得分>=1.0，则Li-1得分降低，这意味着我们将优先压缩较低级别（L5、L4等）而不是较高级别（L0、L1等）。
	// 另一方面，如果Li得分<1.0，那么我们将提高Li-1的优先级。
	// NOTE:总体而言，这意味着，如果底层已经溢出，则取消对上层压缩的优先级。如果底层未满，则提高上层的优先级。
	var prevLevel int
	for level := t.baseLevel; level < len(s.levels); level++ { // 从基线层开始往更底层遍历，level指当前层，
		if prios[prevLevel].adjusted >= 1 { //如果高一层的调整分数>=1，即溢出了
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			// 通过在分数上设置一个下限来避免荒谬的高分，我们将根据该下限调整一个级别。0.01的值是任意选择的
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted //即若当前层溢出（>1），则该层的高一层调整分数就会变小，而如果当前层未溢出（<1），则该层的高一层调整分数就会放大，与前面的理念相符合
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
	// We'll still sort them by their adjusted score below. Having both these scores allows us to
	// make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
	// compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
	// 选择原始分数>=1.0的所有级别，无论其调整后的分数如何。我们仍然会按照下面调整后的分数对他们进行排序。
	// 拥有这两个分数可以让我们在压缩L0方面做出更好的决定。
	// 如果我们看到分数>=1.0，我们可以进行L0->L0压缩。
	// 如果调整后的分数>=1.0，那么我们可以进行L0->Lbase压缩。
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// Sort by the adjusted score.
	// 按调整分数排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// checkOverlap checks if the given tables overlap with any level from the given "lev" onwards.
// checkOverlap检查给定的table是否与给定“lev”之后的任何级别重叠。
func (s *levelsController) checkOverlap(tables []*table.Table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range s.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// subcompact runs a single sub-compaction, iterating over the specified key-range only.

// We use splits to do a single compaction concurrently. If we have >= 3 tables
// involved in the bottom level during compaction, we choose key ranges to
// split the main compaction up into sub-compactions. Each sub-compaction runs
// concurrently, only iterating over the provided key range, generating tables.
// This speeds up the compaction significantly.
// subcompact运行一个子压缩，仅在指定的键范围内迭代。
// 我们使用拆分同时执行单个压缩。如果在压实过程中，底层涉及>=3个表格，我们会选择关键范围将主压实拆分为子压实。
// 每个子压缩并行运行，只在提供的键范围内迭代，生成表。这大大加快了压实速度。
func (s *levelsController) subcompact(it y.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *y.Throttle, res chan<- *table.Table) {
	//注意kr是当前子压缩要处理的key范围，每个子压缩处理的key范围不一样

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1) //覆盖度检查，检查两层中受影响的SST（即top + bot）的总范围是否与更底层之间有重叠

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	// 选择一个丢弃ts，这样我们就可以丢弃低于此ts的版本。我们永远不应该丢弃从高于此时间戳开始的任何版本，因为这会影响事务提供的快照视图保证。
	discardTs := s.kv.orc.discardAtOrBelow()

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	// 尝试收集统计数据，以便我们可以通知GC的值日志。这将帮助我们找到应该对哪个值日志文件进行GC。
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) { //在日志压缩的过程中，对无用的KV大小进行统计（在GC里面用这个值）
		// We don't need to store/update discard stats when badger is running in Disk-less mode.
		if s.kv.opt.InMemory {
			return
		}
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len) //累加无效KV的Value大小
		}
	}

	// exceedsAllowedOverlap returns true if the given key range would overlap with more than 10
	// tables from level below nextLevel (nextLevel+1). This helps avoid generating tables at Li
	// with huge overlaps with Li+1.
	// 如果给定的键范围与nextLevel（nextLevel+1）以下级别的10个以上表重叠，exceedsAllowedOverlap将返回true。
	// 这有助于避免在Li生成与Li+1有巨大重叠的表。
	exceedsAllowedOverlap := func(kr keyRange) bool {
		n2n := cd.nextLevel.level + 1
		if n2n <= 1 || n2n >= len(s.levels) {
			return false
		}
		n2nl := s.levels[n2n]
		n2nl.RLock()
		defer n2nl.RUnlock()

		l, r := n2nl.overlappingTables(levelHandlerRLocked{}, kr)
		return r-l >= 10
	}

	var (
		lastKey, skipKey       []byte
		numBuilds, numVersions int
		// Denotes if the first key is a series of duplicate keys had
		// "DiscardEarlierVersions" set
		firstKeyHasDiscardSet bool
	)

	addKeys := func(builder *table.Builder) {
		timeStart := time.Now()
		var numKeys, numSkips uint64
		var rangeCheck int
		var tableKr keyRange
		//不断地遍历迭代器，来取kv对，注意取出来的KV对在KEY上是有序的
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) { //如果是包含有可跳过信息的前缀，就跳过
				numSkips++
				updateStats(it.Value()) //将当前跳过的KV大小累加到无效池中
				continue
			}

			// See if we need to skip this key.
			// 如果有需要跳过的key，那么就跳过
			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			//处理相同key的不同版本，被忽略版本的会在这个if里面进行一个统计
			if !y.SameKey(it.Key(), lastKey) {
				// 如果当前遍历key与lastKey不同
				firstKeyHasDiscardSet = false
				if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
					//如果当前的子压缩处理的key范围右界存在，且当前遍历的KV对的KEY要 >= 右界，那么就不归本次压缩处理，跳过
					break
				}
				if builder.ReachedCapacity() { //判断buffer是否写的太多，写的多了话就会在这里做一个切分
					// Only break if we are on a different key, and have reached capacity. We want
					// to ensure that all versions of the key are stored in the same sstable, and
					// not divided across multiple tables at the same level.
					// 只有当我们在另一个关键点上，并且已经达到容量时，才会打破。我们希望确保密钥的所有版本都存储在同一个sstable中，而不是在同一级别的多个表中划分。
					break
				}
				lastKey = y.SafeCopy(lastKey, it.Key()) //将当前key记录起来
				numVersions = 0
				firstKeyHasDiscardSet = it.Value().Meta&bitDiscardEarlierVersions > 0

				if len(tableKr.left) == 0 {
					//如果是遍历的第一个，则将当前KV的KEY，设置为当前SST的左边界
					tableKr.left = y.SafeCopy(tableKr.left, it.Key())
				}
				tableKr.right = lastKey //不断更新右边界（因为遍历的key是有序的，所以可以直接赋值）

				rangeCheck++
				if rangeCheck%5000 == 0 { //每写入5000条key（版本不同但同一个key计数为1）
					// This table's range exceeds the allowed range overlap with the level after
					// next. So, we stop writing to this table. If we don't do this, then we end up
					// doing very expensive compactions involving too many tables. To amortize the
					// cost of this check, we do it only every N keys.
					// 此SST的范围超出了允许的范围，与下一个级别重叠。所以，我们停止向该SST写入。
					// 如果我们不这样做，那么我们最终会做非常昂贵的压缩，涉及太多的SST。为了摊销此检查的成本，我们只对每个N个密钥进行一次检查。
					if exceedsAllowedOverlap(tableKr) {
						// s.kv.opt.Debugf("L%d -> L%d Breaking due to exceedsAllowedOverlap with
						// kr: %s\n", cd.thisLevel.level, cd.nextLevel.level, tableKr)
						break
					}
				}
			}

			vs := it.Value()
			version := y.ParseTs(it.Key())

			isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)

			// Do not discard entries inserted by merge operator. These entries will be
			// discarded once they're merged
			// 不要丢弃合并操作插入的KV。这些KV合并后将被丢弃
			// 判断哪些key应该保留哪些key应该删掉
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 {
				// Keep track of the number of versions encountered for this key. Only consider the
				// versions which are below the minReadTs, otherwise, we might end up discarding the
				// only valid version for a running transaction.
				// 记录此密钥遇到的版本数。只考虑低于minReadTs的版本，否则，我们可能会丢弃正在运行的事务的唯一有效版本。
				numVersions++
				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)
				//如果出现以下情况，请保留当前版本并丢弃所有下一个版本
				//-设置“discard Earlier Versions”位或
				//-我们已经处理了“NumVersionsToKeep”数量的版本
				//（包括当前正在处理的项目）
				lastValidVersion := vs.Meta&bitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep

				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired, skip all the rest of the
					// versions. Ensure that we're only removing versions below readTs.
					// 如果此版本的密钥已删除或过期，请跳过所有其他版本。确保我们只删除低于readTs的版本。
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					// 仅当密钥尚未过期时，才将其添加到表中。我们不想添加已删除/过期的密钥。
					case !isExpired && lastValidVersion:
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
						// 添加此KEY。我们已设置skipKey，因此将跳过以下密钥版本。
					case hasOverlap:
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest. We have set skipKey,
						// so the following key versions would be skipped.
						// 如果此键范围与较低级别有重叠，则保留最新版本的删除标记，丢弃其余部分。我们已设置skipKey，因此将跳过以下键版本。
					default:
						// If no overlap, we can skip all the versions, by continuing here.
						// 如果没有重叠，我们可以跳过所有版本
						numSkips++
						updateStats(vs) // 将当前跳过的KV对，更新到脏键统计中
						continue        // 跳过
					}
				}
			}
			numKeys++
			var vp valuePointer
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
			//下面是依照当前KV的特性，分别压缩保留到不同的位置，如果下次压缩要丢掉，那么就执行AddStaleKey，否则就执行普通的Add即可
			switch {
			case firstKeyHasDiscardSet:
				// This key is same as the last key which had "DiscardEarlierVersions" set. The
				// the next compactions will drop this key if its ts >
				// discardTs (of the next compaction).
				// 此密钥与设置了“DiscardEarlier Versions”的最后一个密钥相同。如果ts>discadTs（下一次压缩），则下一次压实将删除此键。
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			case isExpired:
				// If the key is expired, the next compaction will drop it if
				// its ts > discardTs (of the next compaction).
				// 如果密钥已过期，如果其ts>discadTs（下一次压缩），则下一次压实将丢弃它。
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			default:
				builder.Add(it.Key(), vs, vp.Len)
			}
		}
		s.kv.opt.Debugf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			cd.compactorId, numKeys, numSkips, time.Since(timeStart).Round(time.Millisecond))
	} // End of function: addKeys

	if len(kr.left) > 0 {
		it.Seek(kr.left) //找到开始位置
	} else {
		it.Rewind()
	}
	for it.Valid() { //如果当前迭代的KV对有效
		if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 { //如果不在当前子压缩要处理的范围内，就跳过
			break
		}

		bopts := buildTableOptions(s.kv) //得到新建SST的一些配置项
		// Set TableSize to the target file size for that level.
		// 将TableSize设置为该级别的目标文件大小。
		bopts.TableSize = uint64(cd.t.fileSz[cd.nextLevel.level])
		builder := table.NewTableBuilder(bopts) //依照SST配置项创建一个生成SST的builder

		// This would do the iteration and add keys to builder.
		addKeys(builder) //NOTE:核心操作，把有效的key都加入到builder

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.Empty() {
			// Cleanup builder resources:
			builder.Finish()
			builder.Close()
			continue
		}
		numBuilds++
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		go func(builder *table.Builder, fileID uint64) { //这个协程控制将buffer刷盘到磁盘，按块刷新
			var err error
			defer inflightBuilders.Done(err)
			defer builder.Close()

			var tbl *table.Table
			if s.kv.opt.InMemory {
				// 如果以内存模式，写入到内存即可
				tbl, err = table.OpenInMemoryTable(builder.Finish(), fileID, &bopts)
			} else {
				// 非内存模式，刷入磁盘
				fname := table.NewFilename(fileID, s.kv.opt.Dir)
				tbl, err = table.CreateTable(fname, builder)
			}

			// If we couldn't build the table, return fast.
			if err != nil {
				return
			}
			res <- tbl
		}(builder, s.reserveFileID())
	}
	s.kv.vlog.updateDiscardStats(discardStats) //把discard信息记录起来，在GC中使用
	s.kv.opt.Debugf("Discard stats: %v", discardStats)
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
// compactBuildTables合并topTables和botTables以形成新表列表。
func (s *levelsController) compactBuildTables( // 进行压缩，并得到两层间合并后的新tables（应该是只包含会受影响的SST，而不会把目标层的所有SST都在其中）
	lev int, cd compactDef) ([]*table.Table, func() error, error) {
	// lev是当前层的层号
	topTables := cd.top
	botTables := cd.bot

	numTables := int64(len(topTables) + len(botTables))
	y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, numTables) //统计记录信息
	defer y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, -numTables)

	cd.span.Annotatef(nil, "Top tables count: %v Bottom tables count: %v",
		len(topTables), len(botTables))

	keepTable := func(t *table.Table) bool { //判断传入的表是否还有合并的价值，无效的全部删掉
		for _, prefix := range cd.dropPrefixes {
			if bytes.HasPrefix(t.Smallest(), prefix) &&
				bytes.HasPrefix(t.Biggest(), prefix) {
				// All the keys in this table have the dropPrefix. So, this
				// table does not need to be in the iterator and can be
				// dropped immediately.
				// 此表中的所有键都有dropPrefix（应该是墓碑）。因此，此表不需要位于迭代器中，可以立即删除。
				return false
			}
		}
		return true
	}
	var valid []*table.Table
	for _, table := range botTables { //对目标层影响SST切片遍历，得到有效的SST并存在valid中
		if keepTable(table) {
			valid = append(valid, table)
		}
	}

	//这里也是建立一个堆，然后遍历最小的key，进行table的合并
	newIterator := func() []y.Iterator {
		// Create iterators across all the tables involved first.
		var iters []y.Iterator
		switch {
		case lev == 0: //如果当前层是L0
			iters = appendIteratorsReversed(iters, topTables, table.NOCACHE)
		case len(topTables) > 0:
			y.AssertTrue(len(topTables) == 1)
			iters = []y.Iterator{topTables[0].NewIterator(table.NOCACHE)}
		}
		// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
		// 目标层的级别>=1，我们可以使用ConcatItterator，因为键范围不重叠。
		return append(iters, table.NewConcatIterator(valid, table.NOCACHE))
	}

	res := make(chan *table.Table, 3) //该通道用于暂存新的SST
	inflightBuilders := y.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil { //异步操作，每遍历一次会开启一个异步协程，而每个异步协程会在此累计次数一次
			s.kv.opt.Errorf("cannot start subcompaction: %+v", err)
			return nil, nil, err
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)                   //异步操作，当前异步协程已完成
			it := table.NewMergeIterator(newIterator(), false) //生成一个用于合并的迭代器 NOTE:470
			defer it.Close()
			s.subcompact(it, kr, cd, inflightBuilders, res) //NOTE: 核心操作，进行子任务压缩
		}(kr)
	}

	var newTables []*table.Table //存合并后新的SST
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t) //将新的table压入累计切片
		}
	}()

	// Wait for all table builders to finish and also for newTables accumulator to finish.
	// 等待所有表生成器完成，也等待newTables累加器完成。
	err := inflightBuilders.Finish() // 异步操作，等待所有子任务压缩完成
	close(res)                       // 关闭通道（这个函数会在发完才关闭）
	wg.Wait()                        // Wait for all tables to be picked up.等所有的tables都收拾好。

	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		// 确保创建的文件的目录条目可见。我们不介意在所有文件创建完成后不尽快这样做会带来额外的延迟，因为这是一个后台操作。
		// 即更新内存中的目录文件列表
		err = s.kv.syncDir(s.kv.opt.Dir)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, y.Wrapf(err, "while running compactions for: %+v", cd)
	}

	sort.Slice(newTables, func(i, j int) bool { //将生成的新SST切片从小到大排序
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.nextLevel.level, table.KeyID(), table.CompressionType()))
		// KeyID貌似是压缩的密钥，而不是具体某个kv对的k
	}
	for _, table := range cd.top {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func hasAnyPrefixes(s []byte, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if bytes.HasPrefix(s, prefix) {
			return true
		}
	}

	return false
}

func containsPrefix(table *table.Table, prefix []byte) bool {
	smallValue := table.Smallest()
	largeValue := table.Biggest()
	if bytes.HasPrefix(smallValue, prefix) {
		return true
	}
	if bytes.HasPrefix(largeValue, prefix) {
		return true
	}
	isPresent := func() bool {
		ti := table.NewIterator(0)
		defer ti.Close()
		// In table iterator's Seek, we assume that key has version in last 8 bytes. We set
		// version=0 (ts=math.MaxUint64), so that we don't skip the key prefixed with prefix.
		ti.Seek(y.KeyWithTs(prefix, math.MaxUint64))
		return bytes.HasPrefix(ti.Key(), prefix)
	}

	if bytes.Compare(prefix, smallValue) > 0 &&
		bytes.Compare(prefix, largeValue) < 0 {
		// There may be a case when table contains [0x0000,...., 0xffff]. If we are searching for
		// k=0x0011, we should not directly infer that k is present. It may not be present.
		return isPresent()
	}

	return false
}

func containsAnyPrefixes(table *table.Table, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if containsPrefix(table, prefix) {
			return true
		}
	}

	return false
}

type compactDef struct {
	span *otrace.Span

	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler //溢出层（当前层）
	nextLevel   *levelHandler //目标层

	top []*table.Table // 高层被影响的SST
	bot []*table.Table // 底层被影响的SST

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

// addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
// addSplits允许我们在分割键范围内并行运行多个子压缩。
func (s *levelsController) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0] //清空

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	// 假设我们在cd.bot中有10个表，最小宽度=3。然后，我们将选择0、1、2（选择）、3、4、5（选择），6、7、8（选择）和9（选择，因为最后一张table）。
	// 这给了我们10张表的4个选择。
	// 在边缘案例中，底部的142张桌子导致了48次拆分。这是太多的拆分，因为它会占用表生成器的大量内存。
	// 我们应该保持它，这样我们最多可以分5盘。
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0)) // bot的长度/5 为单次子压缩的目标层的SST切片长度
	if width < 3 {
		width = 3 // 最小为3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange) //得到合并之后的总范围

	addRange := func(right []byte) {
		skr.right = y.Copy(right)
		cd.splits = append(cd.splits, skr)

		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Right is assigned ts=0. The encoding ts bytes take MaxUint64-ts,
			// so, those with smaller TS will be considered larger for the same key.
			// Consider the following.
			// Top table is [A1...C3(deleted)]
			// bot table is [B1....C2]
			// It will generate a split [A1 ... C0], including any records of Key C.
			right := y.KeyWithTs(y.ParseKey(t.Biggest()), 0)
			addRange(right)
		}
	}
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) allTables() []*table.Table {
	ret := make([]*table.Table, 0, len(cd.top)+len(cd.bot))
	ret = append(ret, cd.top...)
	ret = append(ret, cd.bot...)
	return ret
}

func (s *levelsController) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// Only compactor zero can work on this.
		return false
	}

	cd.nextLevel = s.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// Because this level and next level are both level 0, we should NOT acquire
	// the read lock twice, because it can result in a deadlock. So, we don't
	// call compactDef.lockLevels, instead locking the level only once and
	// directly here.

	// As per godocs on RWMutex:
	// If a goroutine holds a RWMutex for reading and another goroutine might
	// call Lock, no goroutine should expect to be able to acquire a read lock
	// until the initial read lock is released. In particular, this prohibits
	// recursive read locking. This is to ensure that the lock eventually
	// becomes available; a blocked Lock call excludes new readers from
	// acquiring the lock.
	// 因为这个级别和下一个级别都是级别0，所以我们不应该两次获取读取锁，因为这可能会导致死锁。因此，我们不调用compactDef.lockLevels，而是在这里直接锁定一次级别。
	// 根据RWMutex上的godocs：
	// 如果一个goroutine持有RWMutex进行读取，而另一个gorutine可能会调用Lock，那么在初始读取锁被释放之前，任何goroutine都不应该期望能够获取读取锁。
	// 特别是，这禁止了递归读取锁定。这是为了确保锁最终可用；被阻止的Lock调用会阻止新的读取器获取锁。
	y.AssertTrue(cd.thisLevel.level == 0)
	y.AssertTrue(cd.nextLevel.level == 0)
	s.levels[0].RLock()
	defer s.levels[0].RUnlock()

	s.cstatus.Lock()
	defer s.cstatus.Unlock()

	top := cd.thisLevel.tables
	var out []*table.Table
	now := time.Now()
	for _, t := range top { //遍历0层的table，选择需要合并压缩的table并填入到out
		if t.Size() >= 2*cd.t.fileSz[0] { // 如果当前table的大小大于等于2倍的MemTableSize
			// This file is already big, don't include it.
			// 当前table的大小已经足够大了，不包含它
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			// Just created it 10s ago. Don't pick for compaction.
			// 10秒前刚刚创建的table。不要选择压缩合并。
			continue
		}
		if _, beingCompacted := s.cstatus.tables[t.ID()]; beingCompacted { //并发控制
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 { //L0层待合并的满足条件的SST少于4个，就不合并了
		// If we don't have enough tables to merge in L0, don't do it.
		// 如果我们没有足够的表在L0中合并，就不要这样做。
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	// 避免发生任何其他L0->Lbase。
	// 下五行做的是一个并发控制
	thisLevel := s.cstatus.levels[cd.thisLevel.level]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		s.cstatus.tables[t.ID()] = struct{}{}
	}

	// For L0->L0 compaction, we set the target file size to max, so the output is always one file.
	// This significantly decreases the L0 table stalls and improves the performance.
	// 对于L0->L0压缩，我们将目标文件大小设置为最大，因此输出始终是一个文件。
	// 这大大减少了L0表的停顿，提高了性能。
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

func (s *levelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.level == 0 {
		panic("Base level can't be zero.")
	}
	// We keep cd.p.adjusted > 0.0 here to allow functions in db.go to artificially trigger
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	// 我们在这里保持cd.p.adjusted>0.0，以允许db.go中的函数人为触发L0->Lbase压缩。这些函数不会设置调整后的分数。
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		// 如果调整后的分数小于1.0，则不要压缩到Lbase。
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables //得到溢出层（高层）包含的tables信息
	if len(top) == 0 {
		return false
	}

	var out []*table.Table
	if len(cd.dropPrefixes) > 0 {
		// Use all tables if drop prefix is set. We don't want to compact only a
		// sub-range. We want to compact all the tables.
		// 如果设置了删除前缀，则使用所有表。我们不想只压缩一个子范围。我们想压缩所有的table。
		out = top

	} else {
		var kr keyRange
		// cd.top[0] is the oldest file. So we start from the oldest file first.
		// cd.top[0]是最旧的文件。所以我们先从最旧的文件开始。
		// 注意下面这个循环貌似并不是能严格的完全的计算出来L0层哪些SST区间是重叠的
		// 因为比如如果第一个与第二个SST不重叠，第二个SST的范围信息就会丢失 （zzlTODO:或许因为BadgerDB的SST的某种特性，是可以完全计算的吗？）
		for _, t := range top { //计算L0层哪些SST区间是重叠的
			dkr := getKeyRange(t)     //得到当前SST的区间范围
			if kr.overlapsWith(dkr) { //判断累计范围kr与当前SST范围dkr是否重叠（注意如果累计范围kr为空，那就默认是重叠的）
				// 如果重叠
				out = append(out, t) // 累计重叠
				kr.extend(dkr)       // 扩充重叠的SST的范围
			} else {
				// 不重叠
				break
			}
		}
	}
	cd.thisRange = getKeyRange(out...) //得到当前层的总范围
	cd.top = out                       //得到当前层重叠的SST

	//注意目标层的table的key范围就是有序的了！！所以下面这一行就是得到目标层SST切片与当前层总范围重叠的开始下标以及结束下标
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right]) //将目标层重叠的SST切出来作为bot

	if len(cd.bot) == 0 {
		//如果完全无重叠
		cd.nextRange = cd.thisRange
	} else {
		//有重叠，目标层的范围取重叠范围
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) //compareAndAdd这个函数记录全局的合并状态，会返回是否可以执行此合并任务
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.

// Say L0 has 10 tables.
// fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
// Next call to fillTablesL0 would run L0ToLbase again, which fails this time.
// So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
// be compacted within L0. Additionally, it would set the compaction range in
// cstatus to inf, so no other L0 -> Lbase compactions can happen.
// Thus, L0 -> L0 must finish for the next L0 -> Lbase to begin.
// fillTablesL0将尝试用Lbase填充L0中的表格。如果不能做到这一点，它将尝试从L0->L0压缩表。
// 假设L0有10个表。illTablesL0ToBase从L0->L5中选取5个表进行压缩。
// 下一次调用fillTablesL0将再次运行L0ToBase，这次失败。
// 所以，我们转而运行fillTablesL0ToL0，它会拾取L0中要压缩的5个表的其余部分。此外，它将cstatus中的压缩范围设置为inf，因此不会发生其他L0->Lbase压缩。
// 因此，L0->L0必须完成，才能开始下一个L0->Lbase。
func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	if ok := s.fillTablesL0ToLbase(cd); ok { // NOTE:核心操作，判断是否可以执行L0与基线层的合并，并且会更新合并任务对象CD的诸多状态
		// 更新的包括（nextRange 取目标层与当前层重叠的范围（如果无重叠，直接等于thisRange），thisRange 当前层的总范围，top 当前层重叠的SST切片，bot 目标层与thisRange重叠的SST）
		return true
	}
	return s.fillTablesL0ToL0(cd) //NOTE:核心操作，当L0ToLbase没有可以合并时，则L0ToL0合并，其为了处理瞬间写到L0的数据很大
	// 这个会更新cd里面的（nextRange 直接置空，thisRange 设为infRange（应该只是为空的且标记inf为true的占位数据），top L0层满足合并条件的SST切片，bot 直接置空）
}

// sortByStaleData sorts tables based on the amount of stale data they have.
// This is useful in removing tombstones.
// sortByStaleData根据表中过时数据的大小对表进行排序。
// 这在移除墓碑时很有用。
func (s *levelsController) sortByStaleDataSize(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// sortByHeuristic sorts tables in increasing order of MaxVersion, so we
// compact older tables first.
// sortByHeuristic按照MaxVersion的递增顺序对表进行排序，因此我们首先压缩旧表。
func (s *levelsController) sortByHeuristic(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	//按最大版本对表进行排序。这就是RocksDB所做的。
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersion() < tables[j].MaxVersion()
	})
}

// This function should be called with lock on levels.
func (s *levelsController) fillMaxLevelTables(tables []*table.Table, cd *compactDef) bool {
	// 这个函数会遍历最底层的SST，先取出来一个（填充到cd.top里面），判断是否大小满足cd.t.fileSz，不满足就按key顺序依次加，直到满足，再去判断这些SST是否可以合并（有无其他协程冲突）
	// 如果可以合并，然后把这些当成cd.bot填充进去，并在s.cstatus记录起来，最后返回即可
	sortedTables := make([]*table.Table, len(tables))
	copy(sortedTables, tables)
	s.sortByStaleDataSize(sortedTables, cd) //按照过时数据的总量来排序

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 { //如果没有过时数据
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table.Table{}
	collectBotTables := func(t *table.Table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool { //得到 第一个当前层所有SST中最小键 >= 当前遍历的SST的最小键 的切片下标
			return y.CompareKeys(tables[i].Smallest(), t.Smallest()) >= 0
		})
		y.AssertTrue(tables[j].ID() == t.ID())
		j++
		// Collect tables until we reach the the required size.
		// 按key排列顺序收集table，直到我们达到所需的大小。
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT) //目标层待合并
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		// If the maxVersion is above the discardTs, we won't clean anything in
		// the compaction. So skip this table.
		// 如果maxVersion高于discadTs，我们将不会清理压缩其中的任何内容。所以跳过这张table。
		if t.MaxVersion() > s.kv.orc.discardAtOrBelow() {
			continue
		}
		if now.Sub(t.CreatedAt) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			// 一小时前刚创建的。不要选择压实。
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		// 如果过时数据大小小于10MB，则可能不值得重写表。跳过它。
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		// 将nextRange设置为与thisRange相同。如果我们不这样做，我们将无法运行多个maxLevel压缩。
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		//如果我们已经在压缩这个范围，什么都不要做。
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) { //并发控制
			continue
		}

		// Found a valid table!
		cd.top = []*table.Table{t} //直接创建一个仅包含该table的切片

		needFileSz := cd.t.fileSz[cd.thisLevel.level]
		if t.Size() >= needFileSz {
			// The table size is what we want so no need to collect more tables.
			// 表大小是我们想要的，所以不需要收集更多的表。
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		// TableSize小于我们想要的大小。收集更多table进行压实。
		// 即如果该级别有多个小表，我们将它们收集在一起形成一个更大的表
		collectBotTables(t, needFileSz)
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) { //判断当前的合并任务是否允许执行，允许的话就记录当前的压缩任务，并且结束填充cd，否则就再继续找
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (s *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table.Table, len(cd.thisLevel.tables)) //该行与之下四行是为了获得当前层的SST切片
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	// 我们正在进行maxLevel到maxLevel的压缩。根据过时的数据大小选择表。
	if cd.thisLevel.isLastLevel() { // 如果正在合并最底层（最底层只能与最底层合并）
		return s.fillMaxLevelTables(tables, cd) //NOTE:核心操作，对最底层合并任务对象进行特殊的填充，且将当前合并任务记录到s.cstatus，并直接返回
		// 更新的包括（nextRange bot的总范围，thisRange 应该是top里那单个SST的区间范围，top 首个取得可能待扩充的包含单个SST的切片，bot 对top扩充后的SST切片（内包含>=1个SST））
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	// 我们挑选table，所以我们先压缩旧table。这类似于RocksDB中的kOldestLargestSeqFirst。
	// 按照MaxVersion的递增顺序对表进行排序，因此我们首先压缩旧表
	s.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) { //做并发
			continue
		}
		cd.top = []*table.Table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange) //得到当前遍历的SST与目标层SST切片重叠的开始下标以及结束下标

		cd.bot = make([]*table.Table, right-left) //生成目标层SST影响切片
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 { //如果无重叠SST
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) { //查看是否可以运行此次合并任务，可以的话就记录并返回，否则就继续找
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...) //有重叠SST

		if s.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

func (s *levelsController) runCompactDef(id, l int, cd compactDef) (err error) { //真正开始做压缩操作,id是执行本次任务的协程编号，l是溢出层层号，cd是合并任务对象
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel //注意这两个已经不是层号了，是对应的levelHandler
	nextLevel := cd.nextLevel

	y.AssertTrue(len(cd.splits) == 0)
	if thisLevel.level == nextLevel.level {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		s.addSplits(&cd) //对bot切片内各table的区间进行拆解，把不重合的区间拆解出来，便于并行运行多个子压缩
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// Table should never be moved directly between levels,
	// always be rewritten to allow discarding invalid versions.
	//表永远不应该在级别之间直接移动，
	//始终重写以允许丢弃无效版本。

	newTables, decr, err := s.compactBuildTables(l, cd) //NOTE:核心操作，进行压缩（已经写到磁盘了），并得到两层间合并后的新tables，decr是一个执行函数，起会把newTables内的每个SST的引用计数减1
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil { //对newTables中每个table执行DecrRef()
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables) //创建一个更改集，貌似只记录一些SST级的更改，不记录更具体的如具体key的一些更改

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil { //NOTE:核心操作，将SST级的更改信息写入清单文件，之后DB才能找到某个SST属于哪个Level
		return err
	}

	getSizes := func(tables []*table.Table) int64 {
		size := int64(0)
		for _, i := range tables {
			size += i.Size()
		}
		return size
	}

	sizeNewTables := int64(0)
	sizeOldTables := int64(0)
	if s.kv.opt.MetricsEnabled {
		sizeNewTables = getSizes(newTables)
		sizeOldTables = getSizes(cd.bot) + getSizes(cd.top)
		y.NumBytesCompactionWrittenAdd(s.kv.opt.MetricsEnabled, nextLevel.strLevel, sizeNewTables) //统计信息更新
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	// NOTE:注意啊，下面这里只是对levelHandler内的SST表进行更新操作
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil { //把目标层老的tables删除掉，并替换成新的表
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil { //把当前层老的tables删除掉
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.
	//注意：对于级别0，当doCompact运行时，可能会添加新表。
	//但是，表只添加到末尾，因此可以删除第一个表。

	//下面是打印统计信息
	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		s.kv.opt.Infof("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n, deleted %d bytes",
			id, expensive, thisLevel.level, nextLevel.level, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond), sizeOldTables-sizeNewTables)
	}

	if cd.thisLevel.level != 0 && len(newTables) > 2*s.kv.opt.LevelSizeMultiplier {
		s.kv.opt.Infof("This Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.top), hex.Dump(cd.thisRange.left), hex.Dump(cd.thisRange.right))
		s.kv.opt.Infof("Next Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.bot), hex.Dump(cd.nextRange.left), hex.Dump(cd.nextRange.right))
	}
	return nil
}

func tablesToString(tables []*table.Table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.ID()))
	}
	res = append(res, ".")
	return res
}

var errFillTables = stderrors.New("Unable to fill tables")

// doCompact picks some table on level l and compacts it away to the next level.
// doCompact在l层拾取一些表并将其压缩到下一层。id是压缩协程的id
func (s *levelsController) doCompact(id int, p compactionPriority) error {
	l := p.level                         // l层是已溢出要压缩的层（或者是最后一层）
	y.AssertTrue(l < s.kv.opt.MaxLevels) // Sanity check.
	if p.t.baseLevel == 0 {              //得到基线层号
		p.t = s.levelTargets()
	}

	_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
	defer span.End()

	cd := compactDef{ //形成合并任务对象
		compactorId:  id, //完成该任务的协程ID
		span:         span,
		p:            p,
		t:            p.t,         // 用于筛选目标层
		thisLevel:    s.levels[l], // 当前层（溢出层）
		dropPrefixes: p.dropPrefixes,
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	// 在选择要压缩的表时，两个级别的表预计将保持不变。
	if l == 0 { //如果溢出的是L0层，选基线层作为合并的目标层（或者也可以执行L0 TO L0的L0自身压缩的操作）
		// 这里做的是为了减少写放大以及空间放大，进行跳层合并
		// 即如果数据库为空的时候，会从0-6依次进行合并，那么L0层的数据可能会合并6次到6层，而这个跳层合并就解决了这个问题，从L0层直接倒序合并，先与Lbase合并
		// Lbase是根据从顶部开始的非空的第一层来选择的（检查L1到L6），即若是全空的话会先与最底层L6合并
		cd.nextLevel = s.levels[p.t.baseLevel]
		if !s.fillTablesL0(&cd) { // NOTE:核心操作，填充合并任务对象内的一些参数，且将当前合并任务记录到s.cstatus(注意每次只会增加一个合并任务，即找到能合并的，就直接记录并返回了)
			return errFillTables
		}
	} else { //如果溢出的是非0层，最后一层选择自身作为目标层，其余层选择比溢出层低一层的层级作为目标层
		cd.nextLevel = cd.thisLevel
		// We're not compacting the last level so pick the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = s.levels[l+1]
		}
		if !s.fillTables(&cd) { // NOTE:核心操作，填充合并任务对象内的一些参数，且将当前合并任务记录到s.cstatus(注意每次只会增加一个合并任务，即找到能合并的，就直接记录并返回了)
			return errFillTables
		}
	}
	defer s.cstatus.delete(cd) // Remove the ranges from compaction status.

	span.Annotatef(nil, "Compaction: %+v", cd)
	if err := s.runCompactDef(id, l, cd); err != nil { // NOTE:核心操作，真正开始执行压缩的函数
		// This compaction couldn't be done successfully.
		s.kv.opt.Warningf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	s.kv.opt.Debugf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.level)
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) error {
	// Add table to manifest file only if it is not opened in memory. We don't want to add a table
	// to the manifest file if it exists only in memory.
	if !t.IsInmemory {
		// We update the manifest _before_ the table becomes part of a levelHandler, because at that
		// point it could get used in some compaction.  This ensures the manifest file gets updated in
		// the proper order. (That means this update happens before that of some compaction which
		// deletes the table.)
		err := s.kv.manifest.addChanges([]*pb.ManifestChange{
			newCreateChange(t.ID(), 0, t.KeyID(), t.CompressionType()),
		})
		if err != nil {
			return err
		}
	}

	for !s.levels[0].tryAddLevel0Table(t) {
		// Before we unstall, we need to make sure that level 0 is healthy.
		timeStart := time.Now()
		for s.levels[0].numTables() >= s.kv.opt.NumLevelZeroTablesStall {
			time.Sleep(10 * time.Millisecond)
		}
		dur := time.Since(timeStart)
		if dur > time.Second {
			s.kv.opt.Infof("L0 was stalled for %s\n", dur.Round(time.Millisecond))
		}
		s.l0stallsMs.Add(int64(dur.Round(time.Millisecond)))
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return y.Wrap(err, "levelsController.Close")
}

// get searches for a given key in all the levels of the LSM tree. It returns
// key version <= the expected version (version in key). If not found,
// it returns an empty y.ValueStruct.
// get函数在LSM树的所有级别中搜索给定的键。它返回key版本<=预期版本（key中的版本，即当前事务的readTs）。如果未找到，则返回一个空的y.ValueStruct。
// 注意startLevel标记从哪一层开始找
// 注意函数参数中的key后缀有当前事务的readTs
func (s *levelsController) get(key []byte, maxVs y.ValueStruct, startLevel int) (
	y.ValueStruct, error) {
	if s.kv.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	// It's important that we iterate the levels from 0 on upward. The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction. (If we do
	// parallelize this, we will need to call the h.RLock() function by increasing order of level
	// number.)
	// 重要的是，我们从0开始向上迭代级别。原因是，如果我们以相反的顺序或并行方式迭代（天真地以某种顺序调用所有h.RLock（）），
	// 我们可以在压缩后读取L级的表，在压缩前读取L+1级的表。（如果我们将其并行化，我们需要通过增加级别号的顺序来调用h.RLock（）函数。）
	version := y.ParseTs(key)    //解析出版本号（当前事务的ReadTs）
	for _, h := range s.levels { //遍历7（默认）层
		// Ignore all levels below startLevel. This is useful for GC when L0 is kept in memory.
		if h.level < startLevel {
			continue
		}
		//下面这一行是NOTE:核心操作，去当前遍历层找目标key
		vs, err := h.get(key) // Calls h.RLock() and h.RUnlock().
		if err != nil {
			return y.ValueStruct{}, y.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		y.NumBytesReadsLSMAdd(s.kv.opt.MetricsEnabled, int64(len(vs.Value))) //统计信息
		if vs.Version == version {                                           //如果找到的版本与当前事务的版本（readTs）相同，直接返回结果
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	if len(maxVs.Value) > 0 {
		y.NumGetsWithResultsAdd(s.kv.opt.MetricsEnabled, 1) //统计信息
	}
	return maxVs, nil
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, opt int) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, opt *IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	for _, level := range s.levels { //遍历每一层增加
		iters = level.appendIterators(iters, opt) //NOTE:核心操作
	}
	return iters
}

// TableInfo represents the information about a table.
type TableInfo struct {
	ID               uint64
	Level            int
	Left             []byte
	Right            []byte
	KeyCount         uint32 // Number of keys in the table
	OnDiskSize       uint32
	StaleDataSize    uint32
	UncompressedSize uint32
	MaxVersion       uint64
	IndexSz          int
	BloomFilterSize  int
}

func (s *levelsController) getTableInfo() (result []TableInfo) {
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			info := TableInfo{
				ID:               t.ID(),
				Level:            l.level,
				Left:             t.Smallest(),
				Right:            t.Biggest(),
				KeyCount:         t.KeyCount(),
				OnDiskSize:       t.OnDiskSize(),
				StaleDataSize:    t.StaleDataSize(),
				IndexSz:          t.IndexSize(),
				BloomFilterSize:  t.BloomFilterSize(),
				UncompressedSize: t.UncompressedSize(),
				MaxVersion:       t.MaxVersion(),
			}
			result = append(result, info)
		}
		l.RUnlock()
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}

type LevelInfo struct {
	Level          int
	NumTables      int
	Size           int64
	TargetSize     int64
	TargetFileSize int64
	IsBaseLevel    bool
	Score          float64
	Adjusted       float64
	StaleDatSize   int64
}

func (s *levelsController) getLevelInfo() []LevelInfo {
	t := s.levelTargets()
	prios := s.pickCompactLevels(nil)
	result := make([]LevelInfo, len(s.levels))
	for i, l := range s.levels {
		l.RLock()
		result[i].Level = i
		result[i].Size = l.totalSize
		result[i].NumTables = len(l.tables)
		result[i].StaleDatSize = l.totalStaleSize

		l.RUnlock()

		result[i].TargetSize = t.targetSz[i]
		result[i].TargetFileSize = t.fileSz[i]
		result[i].IsBaseLevel = t.baseLevel == i
	}
	for _, p := range prios {
		result[p.level].Score = p.score
		result[p.level].Adjusted = p.adjusted
	}
	return result
}

// verifyChecksum verifies checksum for all tables on all levels.
func (s *levelsController) verifyChecksum() error {
	var tables []*table.Table
	for _, l := range s.levels {
		l.RLock()
		tables = tables[:0]
		for _, t := range l.tables {
			tables = append(tables, t)
			t.IncrRef()
		}
		l.RUnlock()

		for _, t := range tables {
			errChkVerify := t.VerifyChecksum()
			if err := t.DecrRef(); err != nil {
				s.kv.opt.Errorf("unable to decrease reference of table: %s while "+
					"verifying checksum with error: %s", t.Filename(), err)
			}

			if errChkVerify != nil {
				return errChkVerify
			}
		}
	}

	return nil
}

// Returns the sorted list of splits for all the levels and tables based
// on the block offsets.
func (s *levelsController) keySplits(numPerTable int, prefix []byte) []string {
	splits := make([]string, 0)
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			tableSplits := t.KeySplits(numPerTable, prefix)
			splits = append(splits, tableSplits...)
		}
		l.RUnlock()
	}
	sort.Strings(splits)
	return splits
}
