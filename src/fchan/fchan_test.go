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
	"sync"
	"testing"
)

const perThread = 256

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
	const runs = 2*segSize + 1 // 4*segSize + 1

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

func minN(b *testing.B) int {
	if b.N < 2 {
		return 2
	}
	return b.N
}
