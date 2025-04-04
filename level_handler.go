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
	"fmt"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

type levelHandler struct {
	// Guards tables, totalSize.
	sync.RWMutex

	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
	//对于级别>=1，表按不重叠的键范围排序。
	//对于级别0，表按时间排序。
	//对于级别0，最新的表位于后面。先压缩最旧的那个，它在前面。
	tables         []*table.Table
	totalSize      int64
	totalStaleSize int64

	// The following are initialized once and const.
	level    int
	strLevel string
	db       *DB
}

func (s *levelHandler) isLastLevel() bool {
	return s.level == s.db.opt.MaxLevels-1
}

func (s *levelHandler) getTotalStaleSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalStaleSize
}

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

// initTables replaces s.tables with given tables. This is done during loading.
func (s *levelHandler) initTables(tables []*table.Table) {
	s.Lock()
	defer s.Unlock()

	s.tables = tables
	s.totalSize = 0
	s.totalStaleSize = 0
	for _, t := range tables {
		s.addSize(t)
	}

	if s.level == 0 {
		//0层KEY范围将重叠。只需按文件ID升序排序
		//因为较新的表位于级别0的末尾。
		sort.Slice(s.tables, func(i, j int) bool {
			return s.tables[i].ID() < s.tables[j].ID()
		})
	} else {
		// 非0层，按照KEY排序
		sort.Slice(s.tables, func(i, j int) bool {
			return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
		})
	}
}

// deleteTables remove tables idx0, ..., idx1-1.
func (s *levelHandler) deleteTables(toDel []*table.Table) error {
	s.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table.Table
	for _, t := range s.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.subtractSize(t)
	}
	s.tables = newTables

	s.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
// replaceTables将用 newTables 替换 tables[left:right]。请注意，tables[left:right]是要被删去的旧表。
// 您必须在将更新写入清单后调用decer（）删除旧表。
func (s *levelHandler) replaceTables(toDel, toAdd []*table.Table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	// 需要重新搜索此级别中要替换的表的范围，因为其他goroutine也可能对其进行更改。（他们不能碰我们的表，但如果他们添加/删除其他表，索引就会移动。）
	s.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}
	var newTables []*table.Table //创建一个容纳新SST的切片
	for _, t := range s.tables { //遍历目标层的原有SST，如果不在目标层受影响数组（cd.bot）内，就直接加到newTables，否则就跳过
		_, found := toDelMap[t.ID()]
		if !found { //如果不在cd.bot内
			newTables = append(newTables, t)
			continue
		}
		s.subtractSize(t) //如果在cd.bot内，那么就直接把该SST的大小减去
	}

	// Increase totalSize first.
	for _, t := range toAdd { //遍历合并后的新的SST切片
		s.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t) // 新的SST切片内所有都加入到newTables
	}

	// Assign tables.
	s.tables = newTables
	sort.Slice(s.tables, func(i, j int) bool { //对目标层最终的SST切片进行排序
		return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
	})
	s.Unlock()             // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel) //减少引用，以便于将旧的SST删除掉
}

// addTable adds toAdd table to levelHandler. Normally when we add tables to levelHandler, we sort
// tables based on table.Smallest. This is required for correctness of the system. But in case of
// stream writer this can be avoided. We can just add tables to levelHandler's table list
// and after all addTable calls, we can sort table list(check sortTable method).
// NOTE: levelHandler.sortTables() should be called after call addTable calls are done.
func (s *levelHandler) addTable(t *table.Table) {
	s.Lock()
	defer s.Unlock()

	s.addSize(t) // Increase totalSize first.
	t.IncrRef()
	s.tables = append(s.tables, t)
}

// sortTables sorts tables of levelHandler based on table.Smallest.
// Normally it should be called after all addTable calls.
func (s *levelHandler) sortTables() {
	s.Lock()
	defer s.Unlock()

	sort.Slice(s.tables, func(i, j int) bool {
		return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
	})
}

func decrRefs(tables []*table.Table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelHandler(db *DB, level int) *levelHandler {
	return &levelHandler{
		level:    level,
		strLevel: fmt.Sprintf("l%d", level),
		db:       db,
	}
}

// tryAddLevel0Table returns true if ok and no stalling.
// 如果正常且没有停滞，tryAddLevel0Table返回true。
func (s *levelHandler) tryAddLevel0Table(t *table.Table) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	// Stall (by returning false) if we are above the specified stall setting for L0.
	if len(s.tables) >= s.db.opt.NumLevelZeroTablesStall {
		return false
	}

	s.tables = append(s.tables, t)
	t.IncrRef()
	s.addSize(t)

	return true
}

// This should be called while holding the lock on the level.
func (s *levelHandler) addSize(t *table.Table) {
	s.totalSize += t.Size()
	s.totalStaleSize += int64(t.StaleDataSize())
}

// This should be called while holding the lock on the level.
func (s *levelHandler) subtractSize(t *table.Table) {
	s.totalSize -= t.Size()
	s.totalStaleSize -= int64(t.StaleDataSize())
}
func (s *levelHandler) numTables() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.tables)
}

func (s *levelHandler) close() error {
	s.RLock()
	defer s.RUnlock()
	var err error
	for _, t := range s.tables {
		if closeErr := t.Close(-1); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return y.Wrap(err, "levelHandler.close")
}

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getTableForKey(key []byte) ([]*table.Table, func() error) { //注意这一个函数执行一次不是遍历所有层，是遍历目标层
	s.RLock() //上锁
	defer s.RUnlock()

	if s.level == 0 { //0层的特殊结构（可重叠），特殊处理，将0层所有的SST句柄返回
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		// CAUTION: Reverse the tables.
		//对于级别0，我们需要检查每个表。记得复制一份，因为s.tables可能会发生变化
		//一旦我们退出此函数，并且我们不想在表中查找时锁定s.tables。
		//小心：倒序遍历表。
		out := make([]*table.Table, 0, len(s.tables))
		for i := len(s.tables) - 1; i >= 0; i-- { // 倒序遍历当前层的SST句柄表（从新的开始遍历）
			out = append(out, s.tables[i])
			s.tables[i].IncrRef() //这个是计数的，与是否应该删除文件有关
		}
		return out, func() error {
			for _, t := range out {
				if err := t.DecrRef(); err != nil {
					return err
				}
			}
			return nil
		}
	}
	// For level >= 1, we can do a binary search as key range does not overlap.
	// 对于级别>=1，我们可以进行二分查找，因为关键字范围不重叠。
	idx := sort.Search(len(s.tables), func(i int) bool { //利用SST的键值范围做一个二分查找，返回的idx是范围匹配到的SST在SST表中下标位置
		return y.CompareKeys(s.tables[i].Biggest(), key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil, func() error { return nil }
	}
	tbl := s.tables[idx] //获取匹配的那个SST，并在之后返回
	tbl.IncrRef()
	return []*table.Table{tbl}, tbl.DecrRef
}

// get returns value for a given key or the key after that. If not found, return nil.
// get返回给定键或之后键的值。如果没有找到，返回nil。
func (s *levelHandler) get(key []byte) (y.ValueStruct, error) {
	tables, decr := s.getTableForKey(key) //NOTE:核心操作，获取当前层的可能包含目标key的SST句柄，0层把所有SST返回，其余层用二分查找找到目标那一个返回就可以
	keyNoTs := y.ParseKey(key)            //获取没有时间戳的KEY

	hash := y.Hash(keyNoTs) // 把key映射到了一个hash函数中
	var maxVs y.ValueStruct
	for _, th := range tables { //一个th代表一个SST
		if th.DoesNotHave(hash) { //判断布隆过滤器是否命中（每个SST写入时都会在元数据区包含一个布隆过滤器的数值）
			y.NumLSMBloomHitsAdd(s.db.opt.MetricsEnabled, s.strLevel, 1) //布隆过滤器未命中计数
			continue
		}
		// 下面是查询	SST，即上面判断出阳性了
		it := th.NewIterator(0) //创建一个迭代器
		defer it.Close()

		y.NumLSMGetsAdd(s.db.opt.MetricsEnabled, s.strLevel, 1) //统计信息
		it.Seek(key)                                            //NOTE:核心操作，正式查询，查找到的会放到it迭代器内
		if !it.Valid() {                                        //判断查找到的是否有效
			continue
		}
		if y.SameKey(key, it.Key()) { //判断key是否相等
			if version := y.ParseTs(it.Key()); maxVs.Version < version { //如果找到的版本比之前找到的最大版本还要大，那么就把现在找到的这个kv当作最新版
				maxVs = it.ValueCopy()  //拼接值
				maxVs.Version = version //拼接版本
			}
		}
	}
	return maxVs, decr()
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
// appendIterator将迭代器附加到迭代器数组中，以便合并。
// 注意：这将获取表处理程序的引用。记得关闭这些迭代器。
// 每执行一次函数，遍历一层
func (s *levelHandler) appendIterators(iters []y.Iterator, opt *IteratorOptions) []y.Iterator {
	s.RLock()
	defer s.RUnlock()

	var topt int
	if opt.Reverse {
		topt = table.REVERSED
	}
	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		// Level 0 tables are not in key sorted order, so we need to consider them one by one.
		//记得按相反的顺序添加！
		//应首先添加s.tables末尾的较新表，因为它具有优先权。
		//0级表不是按键排序的，因此我们需要逐一考虑它们。
		var out []*table.Table
		for _, t := range s.tables { //遍历每一个表
			if opt.pickTable(t) { //NOTE:核心操作，判断当前表是否需要添加到迭代器里面
				out = append(out, t)
			}
		}
		return appendIteratorsReversed(iters, out, topt) //将0层判断出需要遍历的表返回
	}

	tables := opt.pickTables(s.tables) //NOTE:核心操作，取出非0层的需要遍历的表
	if len(tables) == 0 {
		return iters
	}
	return append(iters, table.NewConcatIterator(tables, topt)) //为非0层找到的表创建一个迭代器并加入迭代器数组并返回
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
// overlaping tables返回与键范围相交的table。返回一个半区间。
// 此函数应该已经获取了读取锁，这一点非常重要，调用者必须传递一个声明读取锁的空参数。
func (s *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(s.tables), func(i int) bool { //得到包含有序SST切片中第一个 当前SST最大键 >= 目标范围kr下界 的SST的下标
		return y.CompareKeys(kr.left, s.tables[i].Biggest()) <= 0 //如果目标范围kr的left小于等于当前遍历SST的最大键
		// 找各个顺序SST中第一个最大键大于已有范围kr的左边界（即最小）的
	})
	right := sort.Search(len(s.tables), func(i int) bool { //得到包含有序SST切片中第一个 当前SST最小键 > 目标范围kr下界 的SST的下标
		return y.CompareKeys(kr.right, s.tables[i].Smallest()) < 0
	})
	return left, right
}
