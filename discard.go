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
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// discardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type discardStats struct {
	sync.Mutex

	*z.MmapFile
	opt           Options
	nextEmptySlot int
}

const discardFname string = "DISCARD"

func InitDiscardStats(opt Options) (*discardStats, error) {
	fname := filepath.Join(opt.ValueDir, discardFname)

	// 1MB file can store 65.536 discard entries. Each entry is 16 bytes.
	mf, err := z.OpenMmapFile(fname, os.O_CREATE|os.O_RDWR, 1<<20)
	lf := &discardStats{
		MmapFile: mf,
		opt:      opt,
	}
	if err == z.NewFile {
		// We don't need to zero out the entire 1MB.
		lf.zeroOut()

	} else if err != nil {
		return nil, y.Wrapf(err, "while opening file: %s\n", discardFname)
	}

	for slot := 0; slot < lf.maxSlot(); slot++ {
		if lf.get(16*slot) == 0 {
			lf.nextEmptySlot = slot
			break
		}
	}
	sort.Sort(lf)
	opt.Infof("Discard stats nextEmptySlot: %d\n", lf.nextEmptySlot)
	return lf, nil
}

func (lf *discardStats) Len() int {
	return lf.nextEmptySlot
}
func (lf *discardStats) Less(i, j int) bool {
	return lf.get(16*i) < lf.get(16*j)
}
func (lf *discardStats) Swap(i, j int) {
	left := lf.Data[16*i : 16*i+16]
	right := lf.Data[16*j : 16*j+16]
	var tmp [16]byte
	copy(tmp[:], left)
	copy(left, right)
	copy(right, tmp[:])
}

// offset is not slot.
func (lf *discardStats) get(offset int) uint64 {
	return binary.BigEndian.Uint64(lf.Data[offset : offset+8])
}
func (lf *discardStats) set(offset int, val uint64) {
	binary.BigEndian.PutUint64(lf.Data[offset:offset+8], val)
}

// zeroOut would zero out the next slot.
func (lf *discardStats) zeroOut() {
	lf.set(lf.nextEmptySlot*16, 0)
	lf.set(lf.nextEmptySlot*16+8, 0)
}

func (lf *discardStats) maxSlot() int {
	return len(lf.Data) / 16
}

// Update would update the discard stats for the given file id. If discard is
// 0, it would return the current value of discard for the file. If discard is
// < 0, it would set the current value of discard to zero for the file.
func (lf *discardStats) Update(fidu uint32, discard int64) int64 {
	// 这个函数是更新每个vlog文件的垃圾键值对数量
	// lf是一个Mmap的文件，discard是新累加的垃圾个数，为-1则代表是对vlog清理了，需要把累加器置为0
	// 而discard的个数是在LSM树compaction操作时顺带计数的
	fid := uint64(fidu)
	lf.Lock()
	defer lf.Unlock()

	idx := sort.Search(lf.nextEmptySlot, func(slot int) bool { //先进行二分查找
		return lf.get(slot*16) >= fid
	})
	if idx < lf.nextEmptySlot && lf.get(idx*16) == fid {
		off := idx*16 + 8
		curDisc := lf.get(off)
		if discard == 0 {
			return int64(curDisc)
		}
		if discard < 0 { //诺函数传入的是-1表示当前vlog已经处理完了，设置为0
			lf.set(off, 0)
			return 0
		}
		lf.set(off, curDisc+uint64(discard)) //将新的垃圾个数（discard）累加到当前计数中
		return int64(curDisc + uint64(discard))
	}
	if discard <= 0 {
		// No need to add a new entry.
		return 0
	}

	// Could not find the fid. Add the entry.
	// 下面是没有找到这个fid，说明是新的vlog，为这个vlog创建新的slot来存放 （FID:该文件中垃圾KV对数量）
	idx = lf.nextEmptySlot
	lf.set(idx*16, fid)
	lf.set(idx*16+8, uint64(discard))

	// Move to next slot.
	lf.nextEmptySlot++
	for lf.nextEmptySlot >= lf.maxSlot() {
		y.Check(lf.Truncate(2 * int64(len(lf.Data))))
	}
	lf.zeroOut()

	sort.Sort(lf)
	return discard
}

func (lf *discardStats) Iterate(f func(fid, stats uint64)) {
	for slot := 0; slot < lf.nextEmptySlot; slot++ { // 每个slot（16bit）记录 FID:该文件中垃圾KV对数量
		idx := 16 * slot
		f(lf.get(idx), lf.get(idx+8)) //拿fid与脏键数
	}
}

// MaxDiscard returns the file id with maximum discard bytes.
func (lf *discardStats) MaxDiscard() (uint32, int64) {
	lf.Lock()
	defer lf.Unlock()

	var maxFid, maxVal uint64
	lf.Iterate(func(fid, val uint64) { // 这个val可以理解为脏键数
		if maxVal < val {
			maxVal = val
			maxFid = fid
		}
	})
	return uint32(maxFid), int64(maxVal)
}
