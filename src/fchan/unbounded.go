// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fchan

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Thread-local state for interacting with an unbounded channel
type UnboundedChan struct {
	// pointer to global state
	q *queue
	// pointer into last guess at the true head and tail segments
	head, tail *segment
}

// New initializes a new queue and returns an initial handle to that queue. All
// other handles are allocated by calls to NewHandle()
func New() *UnboundedChan {
	segPtr := &segment{} // 0 values are fine here
	q := &queue{
		H:           0,
		T:           0,
		SpareAllocs: segList{MaxSpares: int64(runtime.GOMAXPROCS(0))},
	}
	h := &UnboundedChan{
		q:    q,
		head: segPtr,
		tail: segPtr,
	}

	return h
}

// NewHandle creates a new handle for the given Queue.
func (u *UnboundedChan) NewHandle() *UnboundedChan {
	return &UnboundedChan{
		q:    u.q,
		head: u.head,
		tail: u.tail,
	}
}

// Enqueue enqueues a Elt into the channel
// TODO(ezrosent) enforce that e is not nil, I think we make that assumption
// here..
func (u *UnboundedChan) Enqueue(e Elt) {
	u.adjust() // don't always do this?
	myInd := index(atomic.AddUint64((*uint64)(&u.q.T), 1) - 1)
	cell, cellInd := myInd.SplitInd()
	seg := u.q.findCell(u.tail, cell)
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])),
		unsafe.Pointer(nil), unsafe.Pointer(e)) {
		return
	}
	wt := (*waiter)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd]))))
	wt.Send(e)
}

// findCell finds a segment at or after start with ID cellID. If one does not
// yet exist, it grows the list of segments.
func (q *queue) findCell(start *segment, cellID index) *segment {
	cur := start
	for cur.ID != cellID {
		next := (*segment)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&cur.Next))))
		if next == nil {
			q.Grow(cur)
			continue
		}
		cur = next
	}
	return cur
}

// adjust moves h's head and tail pointers forward if H and T point to a newer
// segment. The loads and moves do not need to be atomic because H and T only
// ever increase in value. Calling this regularly is probably good for
// performance, and is necessary to ensure that old segments are garbage
// collected.
func (u *UnboundedChan) adjust() {
	H := index(atomic.LoadUint64((*uint64)(&u.q.H)))
	T := index(atomic.LoadUint64((*uint64)(&u.q.T)))
	cellH, _ := H.SplitInd()
	advance(&u.head, cellH)
	cellT, _ := T.SplitInd()
	advance(&u.tail, cellT)
}

// Dequeue an element from the channel, will block if nothing is there
func (u *UnboundedChan) Dequeue() Elt {
	u.adjust()
	myInd := index(atomic.AddUint64((*uint64)(&u.q.H), 1) - 1)
	cell, cellInd := myInd.SplitInd()
	seg := u.q.findCell(u.head, cell)
	elt := seg.Load(cellInd)
	wt := makeWaiter()
	if elt == nil &&
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])),
			unsafe.Pointer(nil), unsafe.Pointer(wt)) {
		if debug {
			dbgPrint("\t[deq] slow path\n")
		}
		return wt.Recv()
	}
	return seg.Load(cellInd)
}
