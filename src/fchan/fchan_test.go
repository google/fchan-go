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
	"reflect"
	"runtime"
	"sync"
	"testing"
)

const perThread = 128

func assertSame(t *testing.T, s1, s2 []int) {
	readMap := func(i []int) map[int]int {
		res := make(map[int]int)
		for _, ii := range i {
			res[ii]++
		}
		return res
	}
	rm1, rm2 := readMap(s1), readMap(s2)
	if !reflect.DeepEqual(rm1, rm2) {
		// print out diffing information
		t.Error("Slices don't match!")
		allKeys := make(map[int]bool)
		for k := range rm1 {
			allKeys[k] = true
		}
		for k := range rm2 {
			allKeys[k] = true
		}
		lookupIn := func(k int, m map[int]int) interface{} {
			v, ok := m[k]
			if !ok {
				return "not found"
			}
			return v
		}
		for k := range allKeys {
			v1 := lookupIn(k, rm1)
			v2 := lookupIn(k, rm2)
			if !reflect.DeepEqual(v1, v2) {
				t.Logf("key %v, v1: %v, v2: %v", k, v1, v2)
			}
		}
	}
}

func unorderedEltsEq(s1, s2 []int) bool {
	readMap := func(i []int) map[int]int {
		res := make(map[int]int)
		for _, ii := range i {
			res[ii]++
		}
		return res
	}
	return reflect.DeepEqual(readMap(s1), readMap(s2))
}

func TestBoundedQueueElements(t *testing.T) {
	const numInputs = (1 << 20)
	bounds := []uint64{0, 1, 1024, segSize}
	for _, bound := range bounds {
		var inputs []int
		var wg sync.WaitGroup
		for i := 0; i < numInputs; i++ {
			inputs = append(inputs, i)
		}
		h := NewBounded(bound)

		ch := make(chan int, 1024)
		for i := 0; i < numInputs/perThread; i++ {
			wg.Add(1)
			go func(i int) {
				hn := h.NewHandle()
				for j := 0; j < perThread; j++ {
					var inp interface{} = inputs[i*perThread+j]
					hn.Enqueue(&inp)
				}
				wg.Done()
			}(i)
			wg.Add(1)
			go func() {
				hn := h.NewHandle()
				for j := 0; j < perThread; j++ {
					out := hn.Dequeue()
					outInt := (*out).(int)
					ch <- outInt
				}
				wg.Done()
			}()
		}

		var outs []int
		for i := 0; i < numInputs; i++ {
			outs = append(outs, <-ch)
		}
		close(ch)
		if !unorderedEltsEq(outs, inputs) {
			t.Errorf("expected %v, got %v", inputs, outs)
		}
		wg.Wait()
	}
}

func SelectWriteTestHelp(t *testing.T, chans []chanLike) {
	nChans := len(chans)
	numInputs := nChans << 14
	chRes := make(chan int, numInputs)
	chSent := make(chan int, numInputs)
	quitch := New()

	// dequeue and join to resch
	for _, ch := range chans {
		go func(ch chanLike) {
			for more := true; more; {
				Select(
					quitch.Then(func(interface{}) {
						more = false
					}),
					ch.Then(func(i interface{}) {
						chRes <- (i).(int)
					}))
			}
		}(ch.newHandleGen())
	}

	for i := 0; i < numInputs/perThread; i++ {
		go func(i int) {
			for j := 0; j < perThread; j++ {
				ss := make([]SelectRecord, 0, len(chans))
				for k, ch := range chans {
					k := k
					ss = append(ss, ch.WriteThen(i*j+k, func() {
						chSent <- i*j + k
					}))
				}
				SelectSlice(ss)
			}
		}(i)
	}

	expected := make([]int, 0, numInputs)
	for i := 0; i < numInputs; i++ {
		expected = append(expected, <-chSent)
	}
	actual := make([]int, 0, numInputs)
	for i := 0; i < numInputs; i++ {
		actual = append(actual, <-chRes)
	}
	// shut down background threads
	var q interface{} = struct{}{}
	for range chans {
		quitch.Enqueue(&q)
	}
	close(chRes)
	for j := range chRes {
		t.Errorf("Additional input received! %d", j)
	}
	assertSame(t, expected, actual)
}

func TestWriteSelect(t *testing.T) {
	SelectWriteTestHelp(t, []chanLike{NewBounded(0), NewBounded(1), NewBounded(1024)})
}

func TestWriteSelectWithUnbounded(t *testing.T) {
	SelectWriteTestHelp(t, []chanLike{NewBounded(0), NewBounded(1), New(), NewBounded(1024)})
}

func SelectTestHelp(t *testing.T, chans []chanLike) {
	nChans := len(chans)
	numInputs := nChans << 12
	var wg sync.WaitGroup
	iters := (numInputs / nChans) / perThread
	chRes := make(chan int, numInputs)
	expected := []int{}
	for i := 0; i < iters; i++ {
		for j := 0; j < nChans; j++ {
			for k := 0; k < perThread; k++ {
				expected = append(expected, i*perThread+k)
			}
		}
	}
	for i := 0; i < iters; i++ {
		wg.Add(nChans + 1)
		for j, ch := range chans {
			chh := ch.newHandleGen()
			go func(i, j int) {
				for k := 0; k < perThread; k++ {
					// add j here...
					var inp interface{} = i*perThread + k + j
					chh.Enqueue(&inp)
				}
				wg.Done()
			}(i, j)
		}
		go func() {
			for i := 0; i < nChans*perThread; i++ {
				slices := make([]SelectRecord, 0, nChans)
				for j, ch := range chans {
					slices = func(j int) []SelectRecord {
						return append(slices,
							ch.Then(func(i interface{}) {
								// ...then remove it here
								chRes <- i.(int) - j
							}))
					}(j)
				}
				SelectSlice(slices)
			}
			wg.Done()
		}()
	}
	actual := []int{}
	for i := 0; i < numInputs; i++ {
		actual = append(actual, <-chRes)
	}
	close(chRes)
	for j := range chRes {
		t.Errorf("Additional input received! %d", j)
	}
	wg.Wait()
	assertSame(t, expected, actual)
}

func TestUnboundedSelect(t *testing.T) {
	SelectTestHelp(t, []chanLike{New(), New(), New()})
}

func TestBoundedSelect(t *testing.T) {
	SelectTestHelp(t,
		[]chanLike{NewBounded(3 << 16), NewBounded(0), NewBounded(1), NewBounded(1024)})
}

func TestMixedSelect(t *testing.T) {
	SelectTestHelp(t,
		[]chanLike{New(), NewBounded(0), NewBounded(1), New(), NewBounded(1024)})
}

func TestMixedSelectMany(t *testing.T) {
	const nChannels = 32
	const nPer = nChannels / 2
	chans := make([]chanLike, 0, nChannels)
	for i := 0; i < nPer; i++ {
		chans = append(chans, New())
	}
	for i := 0; i < nPer; i++ {
		chans = append(chans, NewBounded(uint64(i*1024)))
	}
	SelectTestHelp(t, chans)
}

func TestQueueElements(t *testing.T) {
	const numInputs = 1 << 20
	iters := numInputs / perThread
	var inputs []int
	var wg sync.WaitGroup
	for i := 0; i < numInputs; i++ {
		inputs = append(inputs, i)
	}
	h := New()

	ch := make(chan int, 1024)
	for i := 0; i < iters; i++ {
		wg.Add(1)
		go func(i int) {
			hn := h.NewHandle()
			for j := 0; j < perThread; j++ {
				var inp interface{} = inputs[i*perThread+j]
				hn.Enqueue(&inp)
			}
			wg.Done()
		}(i)
		wg.Add(1)
		go func() {
			hn := h.NewHandle()
			for j := 0; j < perThread; j++ {
				out := hn.Dequeue()
				ch <- (*out).(int)
			}
			wg.Done()
		}()
	}

	var outs []int
	for i := 0; i < numInputs; i++ {
		outs = append(outs, <-ch)
	}
	close(ch)
	if !unorderedEltsEq(outs, inputs) {
		t.Errorf("expected %v, got %v", inputs, outs)
	}
	wg.Wait()
}

func TestSerialQueue(t *testing.T) {
	const runs = 3*segSize + 1

	h := New()
	var msg interface{} = "hi"
	for i := 0; i < runs; i++ {
		var m interface{} = msg
		h.Enqueue(&m)
	}
	for i := 0; i < runs; i++ {
		p := h.Dequeue()
		if !reflect.DeepEqual(*p, msg) {
			t.Errorf("expected %v, got %v", msg, *p)
		}
	}
}

func TestConcurrentQueueAddFirst(t *testing.T) {
	const runs = 3*segSize + 1
	var wg sync.WaitGroup
	h := New()
	var msg interface{} = "hi"
	t.Log("GOMAXPROCS=", runtime.GOMAXPROCS(-1))
	t.Logf("Spawning %v adding goroutines", runs)
	for i := 0; i < runs; i++ {
		var m interface{} = msg
		wg.Add(1)
		go func() {
			hn := h.NewHandle()
			hn.Enqueue(&m)
			wg.Done()
		}()
	}
	t.Logf("Spawning %v getting goroutines", runs)
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func() {
			hn := h.NewHandle()
			p := hn.Dequeue()
			if !reflect.DeepEqual(*p, msg) {
				t.Errorf("expected %v, got %v", msg, *p)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestConcurrentQueueTakeFirst(t *testing.T) {
	const runs = 2*segSize + 1

	var wg sync.WaitGroup
	h := New()
	var msg interface{} = "hi"

	t.Logf("Spawning %v getting goroutines", runs)
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func() {
			hn := h.NewHandle()
			p := hn.Dequeue()
			if !reflect.DeepEqual(*p, msg) {
				t.Errorf("expected %v, got %v", msg, *p)
			}
			wg.Done()
		}()
	}

	t.Logf("Spawning %v adding goroutines", runs)
	for i := 0; i < runs; i++ {
		var m interface{} = msg
		wg.Add(1)
		go func() {
			hn := h.NewHandle()
			hn.Enqueue(&m)
			wg.Done()
		}()
	}
	wg.Wait()
}
