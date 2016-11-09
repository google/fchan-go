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
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// basic debug infrastructure
const debug = false

var dbgPrint = func(s string, i ...interface{}) { fmt.Printf(s, i...) }

// Elt is the element type of a queue, can be any pointer type
type Elt *interface{}
type index uint64
type listElt *segment

type waiter struct {
	E      Elt
	Wgroup sync.WaitGroup
}

func makeWaiter() *waiter {
	wait := &waiter{}
	wait.Wgroup.Add(1)
	return wait
}

func (w *waiter) Send(e Elt) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&w.E)), unsafe.Pointer(e))
	w.Wgroup.Done()
}

func (w *waiter) Recv() Elt {
	w.Wgroup.Wait()
	return Elt(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&w.E))))
}

/*
type weakWaiter struct {
	cond *sync.Cond
	sync.Mutex
	woke int64
}

func makeWeakWaiter(i int32) *weakWaiter {
	w := &weakWaiter{}
	w.cond = sync.NewCond(w)
	return w
}

func (w *weakWaiter) Signal() {
	w.Lock()
	w.woke++
	w.cond.Signal()
	w.Unlock()
}

func (w *weakWaiter) Wait() {
	w.Lock()
	for w.woke == 0 {
		w.cond.Wait()
	}
	w.Unlock()
}

//*/

/*

// Idea to get beyond the scalability bottleneck when number of goroutines is
// much larger than gomaxprocs. Have an array of channels with large buffers
// (or unbuffered channels?) and group threads into these larger groups. This
// means weakWaiters are attached to queue-level state. It has the disadvantage
// of making ordering a bit more difficult, as later receivers could wake up
// earlier senders. I think this is fine, but it merits some thought.
type weakWaiter chan struct{}

func makeWeakWaiter(i int32) *weakWaiter {
	var ch weakWaiter = make(chan struct{}, i)
	return &ch
}

func (w *weakWaiter) Signal() { *w <- struct{}{} }

func (w *weakWaiter) Wait() { <-(*w) }

//*/

//*
type weakWaiter struct {
	OSize  int32
	Size   int32
	Wgroup sync.WaitGroup
}

func makeWeakWaiter(i int32) *weakWaiter {
	wait := &weakWaiter{Size: i, OSize: i}
	wait.Wgroup.Add(1)
	return wait
}

func (w *weakWaiter) Signal() {
	newVal := atomic.AddInt32(&w.Size, -1)
	orig := atomic.LoadInt32(&w.OSize)
	if newVal+1 == orig {
		w.Wgroup.Done()
	}
}

func (w *weakWaiter) Wait() {
	w.Wgroup.Wait()
}

// */

// segList is a best-effort data-structure for storing spare segment
// allocations. The TryPush and TryPop methods follow standard algorithms for
// lock-free linked lists. They have an inconsistent length counter they
// may underestimate the true length of the data-structure, but this allows
// threads to bail out early. Because the slow path of allocating a new segment
// in grow still works.
type segList struct {
	MaxSpares int64
	Length    int64
	Head      *segLink
}

// spmcLink is a list element in a segList. Note that we cannot just re-use the
// segLink next pointers without modifying the algorithm as TryPush could
// potentitally sever pointers in the live queue data structure. That would
// break everything.
type segLink struct {
	Elt  listElt
	Next *segLink
}

func (s *segList) TryPush(e listElt) {
	// bail out if list is at capacity
	if atomic.LoadInt64(&s.Length) >= s.MaxSpares {
		return
	}
	// add to length. Note that this is not atomic with respect to the append,
	// which means we may be under capacity on occasion. This list is only used
	// in a best-effort capacity, so that is okay.
	atomic.AddInt64(&s.Length, 1)
	if debug {
		dbgPrint("Length now %v\n", s.Length)
	}

	tl := &segLink{
		Elt:  e,
		Next: nil,
	}
	const patience = 4
	i := 0
	for ; i < patience; i++ {
		// attempt to cas Head from nil to tail,
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&s.Head)),
			unsafe.Pointer(nil), unsafe.Pointer(tl)) {
			break
		}

		// try to find an empty element
		tailPtr := (*segLink)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.Head))))
		if tailPtr == nil {
			// if Head was switched to nil, retry
			continue
		}

		// advance tailPtr until it has anil next pointer
		for {
			next := (*segLink)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tailPtr.Next))))
			if next == nil {
				break
			}
			tailPtr = next
		}

		// try and add something to the end of the list
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tailPtr.Next)),
			unsafe.Pointer(nil), unsafe.Pointer(tl)) {
			break
		}
	}
	if i == patience {
		atomic.AddInt64(&s.Length, -1)
	}

	if debug {
		dbgPrint("Successfully pushed to segment list\n")
	}

}

func (s *segList) TryPop() (e listElt, ok bool) {
	const patience = 8
	// it is possible that s has length <= 0 due to a temporary inconsistency
	// between the list itself and the length counter. See the comments in
	// TryPush()
	if atomic.LoadInt64(&s.Length) <= 0 {
		return nil, false
	}
	for i := 0; i < patience; i++ {
		hd := (*segLink)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.Head))))
		if hd == nil {
			return nil, false
		}

		// if head is not nil, try to swap it for its next pointer
		nxt := (*segLink)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&hd.Next))))
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&s.Head)),
			unsafe.Pointer(hd), unsafe.Pointer(nxt)) {
			if debug {
				dbgPrint("Successfully popped off segment list\n")
			}
			atomic.AddInt64(&s.Length, -1)
			return hd.Elt, true
		}
	}
	return nil, false
}

// segment size
const segShift = 12
const segSize = 1 << segShift

// The channel buffer is stored as a linked list of fixed-size arrays of size
// segsize. ID is a monotonically increasing identifier corresponding to the
// index in the buffer of the first element of the segment, divided by segSize
// (see SplitInd).
type segment struct {
	ID   index
	Next *segment
	Data [segSize]Elt
}

// Load atomically loads the element at index i of s
func (s *segment) Load(i index) Elt {
	return Elt(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.Data[i]))))
}

// Queue is the global state of the channel. It contains indices into the head
// and tail of the channel as well as a linked list of spare segments used to
// avoid excess allocations.
type queue struct {
	H           index // head index
	T           index // tail index
	SpareAllocs segList
}

// SplitInd splits i into the ID of the segment to which it refers as well as
// the local index into that segment
func (i index) SplitInd() (cellNum index, cellInd index) {
	cellNum = (i >> segShift)
	cellInd = i - (cellNum * segSize)
	return
}

const spare = true

// grow is called if a thread has arrived at the end of the segment list but
// needs to enqueue/dequeue from an index with a higher cell ID. In this case we
// attempt to assign the segment's next pointer to a new segment. Allocating
// segments can be expensive, so the underlying queue has a 'SpareAlloc' segment
// that can be used to grow the queue, or to store unused segments that the
// thread allocates. The presence of 'SpareAlloc' complicates the protocol quite
// a bit, but it is wait-free (aside from memory allocation) and it will only
// return if tail.Next is non-nil.
func (q *queue) Grow(tail *segment) {
	curTail := atomic.LoadUint64((*uint64)(&tail.ID))
	if spare {
		if next, ok := q.SpareAllocs.TryPop(); ok {
			atomic.StoreUint64((*uint64)(&next.ID), curTail+1)
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.Next)),
				unsafe.Pointer(nil), unsafe.Pointer(next)) {
				return
			}
		}
	}

	newSegment := &segment{ID: index(curTail + 1)}
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.Next)),
		unsafe.Pointer(nil), unsafe.Pointer(newSegment)) {
		if debug {
			dbgPrint("\t\tgrew\n")
		}
		return
	}
	if spare {
		// If we allocated a new segment but failed, attempt to place it in
		// SpareAlloc so someone else can use it.
		q.SpareAllocs.TryPush(newSegment)
	}
}

// advance will search for a segment with ID cell at or after the segment in
// ptr, It returns with ptr either pointing to the cell in question or to the
// last non-nill segment in the list.
func advance(ptr **segment, cell index) {
	for {
		next := (*segment)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&(*ptr).Next))))
		if next == nil || next.ID > cell {
			break
		}
		*ptr = next
	}
}
