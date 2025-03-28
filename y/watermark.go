/*
 * Copyright 2016-2018 Dgraph Labs, Inc. and Contributors
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

package y

import (
	"container/heap"
	"context"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2/z"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int            { return len(u) }
func (u uint64Heap) Less(i, j int) bool  { return u[i] < u[j] }
func (u uint64Heap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{}) { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// mark contains one of more indices, along with a done boolean to indicate the
// status of the index: begin or done. It also contains waiters, who could be
// waiting for the watermark to reach >= a certain index.
type mark struct {
	// Either this is an (index, waiter) pair or (index, done) or (indices, done).
	index   uint64
	waiter  chan struct{}
	indices []uint64
	done    bool // Set to true if the index is done.
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//  1. as many times as Begin(k) has, AND
//  2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	doneUntil atomic.Uint64
	lastIndex atomic.Uint64
	Name      string
	markCh    chan mark
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *z.Closer) {
	w.markCh = make(chan mark, 100)
	go w.process(closer)
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index uint64) {
	w.lastIndex.Store(index)
	w.markCh <- mark{index: index, done: false}
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []uint64) {
	w.lastIndex.Store(indices[len(indices)-1])
	w.markCh <- mark{index: 0, indices: indices, done: false}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index uint64) {
	w.markCh <- mark{index: index, done: true}
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []uint64) {
	w.markCh <- mark{index: 0, indices: indices, done: true}
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() uint64 {
	return w.doneUntil.Load()
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val uint64) {
	w.doneUntil.Store(val)
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() uint64 {
	return w.lastIndex.Load()
}

// WaitForMark waits until the given index is marked as done.
func (w *WaterMark) WaitForMark(ctx context.Context, index uint64) error {
	if w.DoneUntil() >= index { //如果当前已提交的最大事务时间戳大于等于当前创建的readTs时间戳，则返回就可以了，否则就往后执行，等满足了再唤醒
		return nil
	}
	waitCh := make(chan struct{})
	w.markCh <- mark{index: index, waiter: waitCh}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

// process is used to process the Mark channel. This is not thread-safe,
// so only run one goroutine for process. One is sufficient, because
// all goroutine ops use purely memory and cpu.
// Each index has to emit atleast one begin watermark in serial order otherwise waiters
// can get blocked idefinitely. Example: We had an watermark at 100 and a waiter at 101,
// if no watermark is emitted at index 101 then waiter would get stuck indefinitely as it
// can't decide whether the task at 101 has decided not to emit watermark or it didn't get
// scheduled yet.
func (w *WaterMark) process(closer *z.Closer) {
	defer closer.Done()

	var indices uint64Heap
	// pending maps raft proposal index to the number of pending mutations for this proposal.
	pending := make(map[uint64]int)
	waiters := make(map[uint64][]chan struct{})

	heap.Init(&indices) // 初始化堆

	processOne := func(index uint64, done bool) {
		// If not already done, then set. Otherwise, don't undo a done entry.
		prev, present := pending[index]
		if !present {
			heap.Push(&indices, index)
		}

		delta := 1
		if done {
			delta = -1
		}
		pending[index] = prev + delta //对时间戳加一或者减一，加一貌似代表活跃事务+1,减一相反

		// Update mark by going through all indices in order; and checking if they have
		// been done. Stop at the first index, which isn't done.
		doneUntil := w.DoneUntil()
		if doneUntil > index {
			AssertTruef(false, "Name: %s doneUntil: %d. Index: %d", w.Name, doneUntil, index)
		}

		until := doneUntil
		loops := 0

		for len(indices) > 0 {
			min := indices[0]
			if done := pending[min]; done > 0 {
				break // len(indices) will be > 0.
			}
			// Even if done is called multiple times causing it to become
			// negative, we should still pop the index.
			heap.Pop(&indices) // 弹出小顶堆中时间戳最小的
			delete(pending, min)
			until = min
			loops++
		}

		if until != doneUntil {
			AssertTrue(w.doneUntil.CompareAndSwap(doneUntil, until))
		}

		notifyAndRemove := func(idx uint64, toNotify []chan struct{}) {
			for _, ch := range toNotify {
				close(ch)
			}
			delete(waiters, idx) // Release the memory back.
		}

		if until-doneUntil <= uint64(len(waiters)) { //如果水位发生了移动
			// Issue #908 showed that if doneUntil is close to 2^60, while until is zero, this loop
			// can hog up CPU just iterating over integers creating a busy-wait loop. So, only do
			// this path if until - doneUntil is less than the number of waiters.
			for idx := doneUntil + 1; idx <= until; idx++ {
				if toNotify, ok := waiters[idx]; ok {
					notifyAndRemove(idx, toNotify)
				}
			}
		} else {
			for idx, toNotify := range waiters {
				if idx <= until {
					notifyAndRemove(idx, toNotify)
				}
			}
		} // end of notifying waiters.
	}

	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case mark := <-w.markCh:
			if mark.waiter != nil {
				doneUntil := w.doneUntil.Load() // 获取已提交事务的水位（水位是和时间戳同一类型的东西，表示在其之前的时间戳（即版本）均已提交）
				if doneUntil >= mark.index {    //诺已提交事务的时间戳大于等于当前时间戳，就可以直接close，不用等待
					close(mark.waiter)
				} else { //当前时间戳大于当前已提交事务的时间戳，表明当前事务前还有活跃的事物
					ws, ok := waiters[mark.index] //创建等待数组
					if !ok {
						waiters[mark.index] = []chan struct{}{mark.waiter}
					} else {
						waiters[mark.index] = append(ws, mark.waiter)
					}
				}
			} else {
				// it is possible that mark.index is zero. We need to handle that case as well.
				if mark.index > 0 || (mark.index == 0 && len(mark.indices) == 0) {
					processOne(mark.index, mark.done)
				}
				for _, index := range mark.indices { // 遍历所有事务，其目的是为了让所有活跃的事物感知到相互之间的关系？TODO:
					processOne(index, mark.done)
				}
			}
		}
	}
}
