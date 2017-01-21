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

var (
	s        = 1
	sentinel = unsafe.Pointer(&s)
)

// we use a type synonym here because otherwise it would be possible for a user
// to append a value of the same underlying type. If that happened then the
// type-assertion in Dequeue could send on one of the elements passed in (which
// is wrong, and could potentially deadlock the program as well).
type waitch chan struct{}

func waitChan() waitch {
	return make(chan struct{}, 2)
}

// possible history of values of a cell
// waitch ::= channel that a sender waits on when it is over buffer size
// recvchan ::= channel that a receiver waits on when it has to receive a value
// - nil -> sentinel -> value
// - nil -> sentinel -> recvChan
// - nil -> value
// - nil -> recvChan
// These two may require someone to send on the waitch before transitioning
// - nil -> waitch -> value
// - nil -> waitch -> recvChan

// BoundedChan is a thread_local handle onto a bounded channel.
type BoundedChan struct {
	q          *queue
	head, tail *segment
	bound      uint64
}

// NewBounded allocates a new queue and returns a handle to that queue. Further
// handles are created by calling NewHandle on the result of NewBounded.
func NewBounded(bufsz uint64) *BoundedChan {
	segPtr := &segment{}
	cur := segPtr
	for b := uint64(segSize); b < bufsz; b += segSize {
		cur.Next = &segment{ID: index(b) >> segShift}
		cur = cur.Next
	}
	q := &queue{
		H:           0,
		T:           0,
		SpareAllocs: segList{MaxSpares: int64(runtime.GOMAXPROCS(0))},
	}
	return &BoundedChan{
		q:     q,
		head:  segPtr,
		tail:  segPtr,
		bound: bufsz,
	}
}

// NewHandle creates a new handle for the given Queue.
func (b *BoundedChan) NewHandle() *BoundedChan {
	return &BoundedChan{
		q:     b.q,
		head:  b.head,
		tail:  b.tail,
		bound: b.bound,
	}
}

func (b *BoundedChan) adjust() {
	// TODO: factor this out into a helper so that bounded and unbounded can
	// use the same code
	H := index(atomic.LoadUint64((*uint64)(&b.q.H)))
	T := index(atomic.LoadUint64((*uint64)(&b.q.T)))
	cellH, _ := H.SplitInd()
	advance(&b.head, cellH)
	cellT, _ := T.SplitInd()
	advance(&b.tail, cellT)
}

// tryCas attempts to cas seg.Data[index] from nil to elt, and if that fails,
// from sentinel to elt.
func tryCas(seg *segment, segInd index, elt unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[segInd])),
		sentinel, elt) ||
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[segInd])),
			unsafe.Pointer(nil), elt) ||
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[segInd])),
			sentinel, elt)
}

// Enqueue sends e on b. If there are already >=bound goroutines blocking, then
// Enqueue will block until sufficiently many elements have been received.
func (b *BoundedChan) Enqueue(e Elt) {
	b.adjust()
	startHead := index(atomic.LoadUint64((*uint64)(&b.q.H)))
	myInd := index(atomic.AddUint64((*uint64)(&b.q.T), 1) - 1)
	cell, cellInd := myInd.SplitInd()
	seg := b.q.findCell(b.tail, cell)
	if myInd > startHead && (myInd-startHead) > index(uint64(b.bound)) {
		// there is a chance that we have to block
		const patience = 4
		for i := 0; i < patience; i++ {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])),
				sentinel, unsafe.Pointer(e)) {
				// Between us reading startHead and now, there were enough
				// increments to make it the case that we should no longer
				// block.
				if debug {
					dbgPrint("[enq] swapped out for sentinel\n")
				}
				return
			}
		}
		var w interface{} = makeWeakWaiter(2, e)
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])),
			unsafe.Pointer(nil), unsafe.Pointer(Elt(&w))) {
			// we successfully swapped in w. No one will overwrite this
			// location unless they send on w first. We block.
			w.(*weakWaiter).Wait()
			return
		} else if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])),
			sentinel, unsafe.Pointer(e)) {
			// Between us reading startHead and now, there were enough
			// increments to make it the case that we should no longer
			// block.
			if debug {
				dbgPrint("[enq] swapped out for sentinel\n")
			}
			return
		}
	} else {
		// normal case. We know we don't have to block because b.q.H can only
		// increase.
		if tryCas(seg, cellInd, unsafe.Pointer(e)) {
			if debug {
				dbgPrint("[enq] successful tryCas\n")
			}
			return
		}
	}
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&seg.Data[cellInd])))
	w := (*waiter)(ptr)
	w.Send(e)
	if debug {
		dbgPrint("[enq] sending to waiter on %v\n", ptr)
	}
	return
}

// Dequeue receives an Elt from b. It blocks if there are no elements enqueued
// there.
func (b *BoundedChan) Dequeue() Elt {
	b.adjust()
	myInd := index(atomic.AddUint64((*uint64)(&b.q.H), 1) - 1)
	cell, segInd := myInd.SplitInd()
	seg := b.q.findCell(b.head, cell)

	// If there are Enqueuers waiting to complete due to the buffer size, we
	// take responsibility for waking up the thread that FA'ed b.q.H + b.bound.
	// If bound is zero, that is just the current thread. Otherwise we have to
	// do some extra work. The thread we are waking up is referred to in names
	// and comments as our 'buddy'.
	var (
		bCell, bInd index
		bSeg        *segment
	)
	if b.bound > 0 {
		buddy := myInd + index(b.bound)
		bCell, bInd = buddy.SplitInd()
		bSeg = b.q.findCell(b.head, bCell)
	}

	w := makeWaiter()
	var res Elt
	if tryCas(seg, segInd, unsafe.Pointer(w)) {
		if debug {
			dbgPrint("[deq] getting res from channel %v\n", w)
		}
		res = w.Recv()
	} else {
		// tryCas failed, which means that through the "possible histories"
		// argument, this must be either an Elt, a waiter or a weakWaiter. It
		// cannot be a waiter because we are the only actor allowed to swap
		// one into this location. Thus it must either be a weakWaiter or an Elt.
		// if it is a weakWaiter, then we must send on it before casing in w,
		// otherwise the other thread could starve. If it is a normal Elt we
		// do the rest of the protocol. This also means that we can safely load
		// an Elt from seg, which is not always the case because sentinel is
		// not an Elt.
		//
		// Step 1: We failed to put our waiter into Ind. That means that either our
		// value is in there, or there is a weakWaiter in there. Either way these
		// are valid elts and we can reliably distinguish them with a type assertion
		elt := seg.Load(segInd)
		res = elt
		if ww, ok := (*elt).(*weakWaiter); ok {
			res = ww.Signal()
		}
	}
	for i := 0; b.bound > 0; i++ {
		if i >= 2 {
			panic("[deq] bug!")
		}
		// We have successfully gotten the value out of our cell. Now we
		// must ensure that our buddy is either woken up if they are
		// waiting, or that they will know not to sleep.
		// if bElt is not nil, it either has an Elt in it or a weakWater. If
		// it has a waitch then we need to send on it to wake up the buddy.
		// If it is not nill then we attempt to cas sentinel into the buddy
		// index. If we fail then the buddy may have cas'ed in a wait
		// channel so we must go again. However that will only happen once.
		bElt := bSeg.Load(bInd)
		// could this be sentinel? I don't think so..
		if bElt != nil {
			if ww, ok := (*bElt).(*weakWaiter); ok {
				ww.Signal()
			}
			// there is a real queue value in bSeg.Data[bInd], therefore
			// buddy cannot be waiting.
			break
		}
		// Let buddy know that they do not have to block
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&bSeg.Data[bInd])),
			unsafe.Pointer(nil), sentinel) {
			break
		}
	}
	return res
}
