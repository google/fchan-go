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

type condStatus uint64

const (
	notReady condStatus = iota
	cancelled
	valid
)

// condWaiter is a type that supports multiple threads to attempt to send a
// value to a single receiver thread. The receiver thread has the option of
// cancelling any ongoing send operations.
type condWaiter struct {
	status       condStatus
	sendW, recvW sync.WaitGroup
	ctr          uint64
	handle       uint
	v            Elt
	sendV        Elt
}

// newCondWaiterV creates a new condWaiter with a value baked in.
func newCondWaiterV(e Elt) *condWaiter {
	res := &condWaiter{
		status: notReady,
		ctr:    0,
		v:      nil,
		sendV:  e,
	}
	res.recvW.Add(1)
	res.sendW.Add(1)
	return res
}

// newCondWaiter creates a new condWaiter.
func newCondWaiter() *condWaiter {
	res := &condWaiter{
		status: notReady,
		ctr:    0,
		v:      nil,
	}
	res.recvW.Add(1)
	res.sendW.Add(1)
	return res
}

// newCondWaiterOneWay creates a new condWaiter that cannot be
// cancelled. This is used for channel operations that are not involved in
// a select.
func newCondWaiterOneWay() *condWaiter {
	res := &condWaiter{
		status: notReady,
		ctr:    0,
		v:      nil,
	}
	res.status = valid
	res.recvW.Add(1)
	return res
}

// validateOneWay is a receive operation for a oneWay condWaiter.
func (c *condWaiter) validateOneWay() Elt {
	c.recvW.Wait()
	return c.v
}

// overlappingSend is used in a BoundedChan where it is possible for
// both a sender and a receiver to wake a waiting thread, and the send
// should be registered as a success if _either_ goroutine succeeded in
// the wake operation.
func (c *condWaiter) overlappingSend(h uint, e Elt) (bool, Elt) {
	if !atomic.CompareAndSwapUint64(&c.ctr, 0, 1) {
		c.sendW.Wait()
		return c.status == valid && c.handle == h, c.sendV
	}
	c.handle = h
	c.v = e
	c.recvW.Done()
	c.sendW.Wait()
	return c.status == valid, c.sendV
}

func (c *condWaiter) send(h uint, e Elt) bool {
	if !atomic.CompareAndSwapUint64(&c.ctr, 0, 1) {
		return false
	}
	c.handle = h
	c.v = e
	c.recvW.Done()
	c.sendW.Wait()
	return c.status == valid
}

func (c *condWaiter) validate() (uint, Elt) {
	c.status = valid
	c.sendW.Done()
	c.recvW.Wait()
	return c.handle, c.v
}

func (c *condWaiter) cancel() {
	if c.status == valid {
		return
	}
	c.status = cancelled
	c.sendW.Done()
}

// basic debug infrastructure
const debug = false

var dbgPrint = func(s string, i ...interface{}) { fmt.Printf(s, i...) }

// Elt is the element type of a queue, can be any pointer type
type Elt *interface{}
type index uint64
type listElt *segment

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
const (
	segShift = 12
	segSize  = 1 << segShift
)

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

type chanLike interface {
	tryPutWaiter(*indexedWaiter) (bool, Elt, func())
	Then(func(interface{})) SelectRecord
	WriteThen(interface{}, func()) SelectRecord
	Enqueue(Elt)
	Dequeue() Elt
	newHandleGen() chanLike
	putWriteWaiter(Elt, indexedWaiter) (bool, func() bool)
}

// SelectRecord is an opaque type containing channel metadata used by Select
type SelectRecord struct {
	ch      chanLike
	cb      func(interface{})
	toWrite Elt
}

type indexedWaiter struct {
	Ix uint
	Cw *condWaiter
}

// Select is an implementation of an operation akin to the built-in `select`
// construct in terms of `UnboundedChan`s and `BoundedChan`s. Select operates on
// values of the `SelectRecord` type, elements of which can be created by the
// two channels' `Then` methods. e.g. for any unbounded chans u_i and bounded
// b_i, one can select on them by calling
// Select(
// 	u_0.Then(func(i interface{}) { fmt.Println("u_0 received %v", i) }),
// 	b_3.Then(...), ... )
func Select(recs ...SelectRecord) {
	SelectSlice(append([]SelectRecord{}, recs...))
}

// SelectSlice is a variant of Select that consumes a slice of `SelectRecord`s
func SelectSlice(recs []SelectRecord) {
	cw := newCondWaiter()
	cleanup := make([]func(), 0, len(recs))
	defer func() {
		for _, cb := range cleanup {
			if cb != nil {
				cb()
			}
		}
	}()
	for i := 0; i < len(recs); i++ {
		u := recs[i].ch
		icw := indexedWaiter{
			Ix: uint(i),
			Cw: cw,
		}
		if tw := recs[i].toWrite; tw != nil {
			ok, cb := u.putWriteWaiter(tw, icw)
			if ok {
				recs[i].cb(nil)
				return
			}
			// cleanup for write selects actually complete the enqueue process, so
			// they have to run before the actual select callback.
			oldCb := recs[i].cb
			recs[i].cb = func(i interface{}) {
				if !cb() {
					u.Enqueue(tw)
				}
				oldCb(i)
			}
		} else {
			ok, elt, cb := u.tryPutWaiter(&icw)
			if ok {
				recs[i].cb(*elt)
				return
			}
			cleanup = append(cleanup, cb)
		}
	}
	ix, elt := cw.validate()
	if elt == nil {
		recs[ix].cb(nil)
		return
	}
	recs[ix].cb(*elt)
}
