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
		go s.runCompactor(i, lc) //开启压缩器协程
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
func (s *levelsController) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < s.kv.opt.BaseLevelSize {
			return s.kv.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{ //先初始化了一个目标数组
		targetSz: make([]int64, len(s.levels)), //每一层的size多大
		fileSz:   make([]int64, len(s.levels)), //每一层的文件多大
	}
	// DB size is the size of the last level.
	dbSize := s.lastLevel().getTotalSize()   //获取第六层的size
	for i := len(s.levels) - 1; i > 0; i-- { //从第六层倒序的遍历每一层
		ltarget := adjust(dbSize) //得到基准线（BaseLevelSize）与当前层size的较大值
		t.targetSz[i] = ltarget
		if t.baseLevel == 0 && ltarget <= s.kv.opt.BaseLevelSize { //这个就是层号倒序不断地去试，得到那个想要的层号
			t.baseLevel = i
		}
		dbSize /= int64(s.kv.opt.LevelSizeMultiplier) //得到下一层大小，每次折损10倍（badger默认层间比例为10）
	}

	tsz := s.kv.opt.BaseTableSize
	for i := 0; i < len(s.levels); i++ {
		if i == 0 { //如果是0层，文件大小为配置里面的MemTableSize
			// Use MemTableSize for Level 0. Because at Level 0, we stop compactions based on the
			// number of tables, not the size of the level. So, having a 1:1 size ratio between
			// memtable size and the size of L0 files is better than churning out 32 files per
			// memtable (assuming 64MB MemTableSize and 2MB BaseTableSize).
			t.fileSz[i] = s.kv.opt.MemTableSize
		} else if i <= t.baseLevel { // 如果是在目标层之前，文件大小为配置里面的BaseTableSize
			t.fileSz[i] = tsz
		} else { //如果在目标层之后，文件大小为10倍的BaseTableSize
			tsz *= int64(s.kv.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// Bring the base level down to the last empty level.
	for i := t.baseLevel + 1; i < len(s.levels)-1; i++ { //做一个边界处理，即目标层之后，找到最大的空层，然后把该层号赋值给baseLevel
		if s.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the
	// target size, pick the next level as the base level.
	b := t.baseLevel
	lvl := s.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

func (s *levelsController) runCompactor(id int, lc *z.Closer) {
	defer lc.Done() //做协程的并发控制（应该是关闭协程）

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond) // 创建一个0~1000ms的随即延迟，是为了减少协程之间的冲突，来提高并发
	select {
	case <-randomDelay.C:
	case <-lc.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	moveL0toFront := func(prios []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}

	run := func(p compactionPriority) bool { // p里面存着从哪压缩到哪
		err := s.doCompact(id, p) // 执行压缩器
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

	var priosBuffer []compactionPriority
	runOnce := func() bool {
		prios := s.pickCompactLevels(priosBuffer)
		defer func() {
			priosBuffer = prios
		}()
		if id == 0 {
			// Worker ID zero prefers to compact L0 always.
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			if run(p) {
				return true
			}
		}

		return false
	}

	tryLmaxToLmaxCompaction := func() {
		p := compactionPriority{ //合并优先级对象
			level: s.lastLevel().level, //获取最后一层的层号（第6层）
			t:     s.levelTargets(),    //目标层，谁向p.level合并
		}
		run(p)

	}
	count := 0
	ticker := time.NewTicker(50 * time.Millisecond) //创建一个50ms的时钟
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C: //每50ms执行一次
			count++
			// Each ticker is 50ms so 50*200=10seconds.
			if s.kv.opt.LmaxCompaction && id == 2 && count >= 200 { // 注意只有2号协程才会执行跳层合并的操作，且每10秒才会执行一次这个操作
				tryLmaxToLmaxCompaction() // 这里做的是那个为了减少写放大，进行跳层合并（如果从0-6依次进行合并，那么L0层的数据可能会合并6次到6层，而这个跳层合并就解决了这个问题，是一个优化，LmaxCompaction参数就是控制这个功能是否开启）
				count = 0
			} else {
				runOnce() //其余的进行普通的压缩
			}
		case <-lc.HasBeenClosed(): //或者关闭掉
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
func (s *levelsController) pickCompactLevels(priosBuffer []compactionPriority) (prios []compactionPriority) {
	t := s.levelTargets()
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
	if cap(priosBuffer) < len(s.levels) {
		priosBuffer = make([]compactionPriority, 0, len(s.levels))
	}
	prios = priosBuffer[:0]

	// Add L0 priority based on the number of tables.
	addPriority(0, float64(s.levels[0].numTables())/float64(s.kv.opt.NumLevelZeroTables))

	// All other levels use size to calculate priority.
	for i := 1; i < len(s.levels); i++ {
		// Don't consider those tables that are already being compacted right now.
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
	var prevLevel int
	for level := t.baseLevel; level < len(s.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
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
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// Sort by the adjusted score.
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// checkOverlap checks if the given tables overlap with any level from the given "lev" onwards.
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
//
// We use splits to do a single compaction concurrently. If we have >= 3 tables
// involved in the bottom level during compaction, we choose key ranges to
// split the main compaction up into sub-compactions. Each sub-compaction runs
// concurrently, only iterating over the provided key range, generating tables.
// This speeds up the compaction significantly.
func (s *levelsController) subcompact(it y.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *y.Throttle, res chan<- *table.Table) {

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1) //覆盖度检查

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	discardTs := s.kv.orc.discardAtOrBelow()

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	//尝试收集统计数据，以便我们可以通知GC的值日志。这将帮助我们找到应该对哪个值日志文件进行GC。
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) { //在日志压缩的过程中，对无用的key数量进行统计（在GC里面用这个值）
		// We don't need to store/update discard stats when badger is running in Disk-less mode.
		if s.kv.opt.InMemory {
			return
		}
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	// exceedsAllowedOverlap returns true if the given key range would overlap with more than 10
	// tables from level below nextLevel (nextLevel+1). This helps avoid generating tables at Li
	// with huge overlaps with Li+1.
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
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) { //如果是包含有可跳过信息的前缀，就跳过
				numSkips++
				updateStats(it.Value())
				continue
			}

			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			if !y.SameKey(it.Key(), lastKey) { //处理相同key的不同版本，被忽略版本的会在这个if里面进行一个统计
				firstKeyHasDiscardSet = false
				if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() { //判断buffer是否写的太多，写的多了话就会在这里做一个切分
					// Only break if we are on a different key, and have reached capacity. We want
					// to ensure that all versions of the key are stored in the same sstable, and
					// not divided across multiple tables at the same level.
					break
				}
				lastKey = y.SafeCopy(lastKey, it.Key())
				numVersions = 0
				firstKeyHasDiscardSet = it.Value().Meta&bitDiscardEarlierVersions > 0

				if len(tableKr.left) == 0 {
					tableKr.left = y.SafeCopy(tableKr.left, it.Key())
				}
				tableKr.right = lastKey

				rangeCheck++
				if rangeCheck%5000 == 0 { //每写入 5000条key（版本不同但同一个key计数为1）
					// This table's range exceeds the allowed range overlap with the level after
					// next. So, we stop writing to this table. If we don't do this, then we end up
					// doing very expensive compactions involving too many tables. To amortize the
					// cost of this check, we do it only every N keys.
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
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 { //判断哪些key应该保留哪些key应该删掉
				// Keep track of the number of versions encountered for this key. Only consider the
				// versions which are below the minReadTs, otherwise, we might end up discarding the
				// only valid version for a running transaction.
				numVersions++
				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)
				lastValidVersion := vs.Meta&bitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep

				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired, skip all the rest of the
					// versions. Ensure that we're only removing versions below readTs.
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					case !isExpired && lastValidVersion:
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
					case hasOverlap:
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest. We have set skipKey,
						// so the following key versions would be skipped.
					default:
						// If no overlap, we can skip all the versions, by continuing here.
						numSkips++
						updateStats(vs) //更新脏键数
						continue        // Skip adding this key.
					}
				}
			}
			numKeys++
			var vp valuePointer
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
			switch {
			case firstKeyHasDiscardSet:
				// This key is same as the last key which had "DiscardEarlierVersions" set. The
				// the next compactions will drop this key if its ts >
				// discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			case isExpired:
				// If the key is expired, the next compaction will drop it if
				// its ts > discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			default:
				builder.Add(it.Key(), vs, vp.Len)
			}
		}
		s.kv.opt.Debugf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			cd.compactorId, numKeys, numSkips, time.Since(timeStart).Round(time.Millisecond))
	} // End of function: addKeys

	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		it.Rewind()
	}
	for it.Valid() { //在此之前，已经把当前层以及目标层的table合并形成一个新的数组。
		if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
			break
		}

		bopts := buildTableOptions(s.kv)
		// Set TableSize to the target file size for that level.
		bopts.TableSize = uint64(cd.t.fileSz[cd.nextLevel.level])
		builder := table.NewTableBuilder(bopts)

		// This would do the iteration and add keys to builder.
		addKeys(builder) //把有效的key都加入到builder

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
		go func(builder *table.Builder, fileID uint64) { //这个协程控制将buffer刷盘到磁盘
			var err error
			defer inflightBuilders.Done(err)
			defer builder.Close()

			var tbl *table.Table
			if s.kv.opt.InMemory {
				tbl, err = table.OpenInMemoryTable(builder.Finish(), fileID, &bopts)
			} else {
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
func (s *levelsController) compactBuildTables( // 得到两层间合并后的新tables
	lev int, cd compactDef) ([]*table.Table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot

	numTables := int64(len(topTables) + len(botTables))
	y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, numTables)
	defer y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, -numTables)

	cd.span.Annotatef(nil, "Top tables count: %v Bottom tables count: %v",
		len(topTables), len(botTables))

	keepTable := func(t *table.Table) bool {
		for _, prefix := range cd.dropPrefixes {
			if bytes.HasPrefix(t.Smallest(), prefix) &&
				bytes.HasPrefix(t.Biggest(), prefix) {
				// All the keys in this table have the dropPrefix. So, this
				// table does not need to be in the iterator and can be
				// dropped immediately.
				return false
			}
		}
		return true
	}
	var valid []*table.Table
	for _, table := range botTables {
		if keepTable(table) {
			valid = append(valid, table)
		}
	}

	newIterator := func() []y.Iterator { //这里也是建立一个堆，然后遍历最小的key，进行table的合并
		// Create iterators across all the tables involved first.
		var iters []y.Iterator
		switch {
		case lev == 0:
			iters = appendIteratorsReversed(iters, topTables, table.NOCACHE)
		case len(topTables) > 0:
			y.AssertTrue(len(topTables) == 1)
			iters = []y.Iterator{topTables[0].NewIterator(table.NOCACHE)}
		}
		// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
		return append(iters, table.NewConcatIterator(valid, table.NOCACHE))
	}

	res := make(chan *table.Table, 3)
	inflightBuilders := y.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			s.kv.opt.Errorf("cannot start subcompaction: %+v", err)
			return nil, nil, err
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := table.NewMergeIterator(newIterator(), false)
			defer it.Close()
			s.subcompact(it, kr, cd, inflightBuilders, res) //进行子任务压缩
		}(kr)
	}

	var newTables []*table.Table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t) //创建新的table
		}
	}()

	// Wait for all table builders to finish and also for newTables accumulator to finish.
	err := inflightBuilders.Finish()
	close(res)
	wg.Wait() // Wait for all tables to be picked up.

	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		err = s.kv.syncDir(s.kv.opt.Dir)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, y.Wrapf(err, "while running compactions for: %+v", cd)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.nextLevel.level, table.KeyID(), table.CompressionType()))
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
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table.Table
	bot []*table.Table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

// addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
func (s *levelsController) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

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
	//
	// As per godocs on RWMutex:
	// If a goroutine holds a RWMutex for reading and another goroutine might
	// call Lock, no goroutine should expect to be able to acquire a read lock
	// until the initial read lock is released. In particular, this prohibits
	// recursive read locking. This is to ensure that the lock eventually
	// becomes available; a blocked Lock call excludes new readers from
	// acquiring the lock.
	y.AssertTrue(cd.thisLevel.level == 0)
	y.AssertTrue(cd.nextLevel.level == 0)
	s.levels[0].RLock()
	defer s.levels[0].RUnlock()

	s.cstatus.Lock()
	defer s.cstatus.Unlock()

	top := cd.thisLevel.tables
	var out []*table.Table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// This file is already big, don't include it.
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			// Just created it 10s ago. Don't pick for compaction.
			continue
		}
		if _, beingCompacted := s.cstatus.tables[t.ID()]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// If we don't have enough tables to merge in L0, don't do it.
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	thisLevel := s.cstatus.levels[cd.thisLevel.level]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		s.cstatus.tables[t.ID()] = struct{}{}
	}

	// For L0->L0 compaction, we set the target file size to max, so the output is always one file.
	// This significantly decreases the L0 table stalls and improves the performance.
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

func (s *levelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.level == 0 {
		panic("Base level can't be zero.")
	}
	// We keep cd.p.adjusted > 0.0 here to allow functions in db.go to artificially trigger
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table.Table
	if len(cd.dropPrefixes) > 0 {
		// Use all tables if drop prefix is set. We don't want to compact only a
		// sub-range. We want to compact all the tables.
		out = top

	} else {
		var kr keyRange
		// cd.top[0] is the oldest file. So we start from the oldest file first.
		for _, t := range top { //计算哪些SST区间是重叠的
			dkr := getKeyRange(t)
			if kr.overlapsWith(dkr) {
				out = append(out, t)
				kr.extend(dkr)
			} else {
				break
			}
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) //compareAndAdd这个函数记录全局的合并状态
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.
//
// Say L0 has 10 tables.
// fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
// Next call to fillTablesL0 would run L0ToLbase again, which fails this time.
// So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
// be compacted within L0. Additionally, it would set the compaction range in
// cstatus to inf, so no other L0 -> Lbase compactions can happen.
// Thus, L0 -> L0 must finish for the next L0 -> Lbase to begin.
func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	if ok := s.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return s.fillTablesL0ToL0(cd) //当L0ToLbase没有可以合并时，则L0ToL0合并，其为了处理瞬间写到L0的数据很大
}

// sortByStaleData sorts tables based on the amount of stale data they have.
// This is useful in removing tombstones.
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
func (s *levelsController) sortByHeuristic(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersion() < tables[j].MaxVersion()
	})
}

// This function should be called with lock on levels.
func (s *levelsController) fillMaxLevelTables(tables []*table.Table, cd *compactDef) bool {
	sortedTables := make([]*table.Table, len(tables))
	copy(sortedTables, tables)
	s.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table.Table{}
	collectBotTables := func(t *table.Table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return y.CompareKeys(tables[i].Smallest(), t.Smallest()) >= 0
		})
		y.AssertTrue(tables[j].ID() == t.ID())
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		// If the maxVersion is above the discardTs, we won't clean anything in
		// the compaction. So skip this table.
		if t.MaxVersion() > s.kv.orc.discardAtOrBelow() {
			continue
		}
		if now.Sub(t.CreatedAt) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table.Table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.level]
		// The table size is what we want so no need to collect more tables.
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
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

	tables := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return s.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	s.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}
		cd.top = []*table.Table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table.Table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

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

func (s *levelsController) runCompactDef(id, l int, cd compactDef) (err error) { //真正开始做压缩操作
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	y.AssertTrue(len(cd.splits) == 0)
	if thisLevel.level == nextLevel.level {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		s.addSplits(&cd) //对各table的区间进行拆解，把不重合的区间拆解出来
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// Table should never be moved directly between levels,
	// always be rewritten to allow discarding invalid versions.

	newTables, decr, err := s.compactBuildTables(l, cd) //得到两层合并后的新tables列表
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables) //和之前的tables进行一个对比

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil { //写入清单文件，之后DB才能找到某个SST属于哪个Level
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
		y.NumBytesCompactionWrittenAdd(s.kv.opt.MetricsEnabled, nextLevel.strLevel, sizeNewTables)
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil { //把老的tables删除掉
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil { //把老的tables删除掉
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

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
// doCompact在l层拾取一些表并将其压缩到下一层。
func (s *levelsController) doCompact(id int, p compactionPriority) error { //id是压缩协程的id
	l := p.level                         // l层是当前层
	y.AssertTrue(l < s.kv.opt.MaxLevels) // Sanity check.
	if p.t.baseLevel == 0 {
		p.t = s.levelTargets()
	}

	_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
	defer span.End()

	cd := compactDef{ //形成合并任务
		compactorId:  id, //任务ID
		span:         span,
		p:            p,
		t:            p.t,
		thisLevel:    s.levels[l], // 当前层
		dropPrefixes: p.dropPrefixes,
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		cd.nextLevel = s.levels[p.t.baseLevel]
		if !s.fillTablesL0(&cd) { //填充L0层（为什么TODO:）
			return errFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// We're not compacting the last level so pick the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = s.levels[l+1]
		}
		if !s.fillTables(&cd) {
			return errFillTables
		}
	}
	defer s.cstatus.delete(cd) // Remove the ranges from compaction status.

	span.Annotatef(nil, "Compaction: %+v", cd)
	if err := s.runCompactDef(id, l, cd); err != nil {
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
		//下面这一行是核心代码，去当前遍历层找目标key
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
		iters = level.appendIterators(iters, opt)
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
