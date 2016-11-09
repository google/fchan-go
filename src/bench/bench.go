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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"../fchan"
)

// chanRec is a wrapper for a channel-like object used in the benchmarking code
// to avoid code duplication
type chanRec struct {
	NewHandle func() interface{}
	Enqueue   func(ch interface{}, e fchan.Elt)
	Dequeue   func(ch interface{}) fchan.Elt
}

func wrapBounded(bound uint64) *chanRec {
	ch := fchan.NewBounded(bound)
	return &chanRec{
		NewHandle: func() interface{} { return ch.NewHandle() },
		Enqueue: func(ch interface{}, e fchan.Elt) {
			ch.(*fchan.BoundedChan).Enqueue(e)
		},
		Dequeue: func(ch interface{}) fchan.Elt {
			return ch.(*fchan.BoundedChan).Dequeue()
		},
	}
}

func wrapUnbounded() *chanRec {
	ch := fchan.New()
	return &chanRec{
		NewHandle: func() interface{} { return ch.NewHandle() },
		Enqueue: func(ch interface{}, e fchan.Elt) {
			ch.(*fchan.UnboundedChan).Enqueue(e)
		},
		Dequeue: func(ch interface{}) fchan.Elt {
			return ch.(*fchan.UnboundedChan).Dequeue()
		},
	}
}

func wrapChan(chanSize int) *chanRec {
	ch := make(chan fchan.Elt, chanSize)
	return &chanRec{
		NewHandle: func() interface{} { return nil },
		Enqueue:   func(_ interface{}, e fchan.Elt) { ch <- e },
		Dequeue:   func(_ interface{}) fchan.Elt { return <-ch },
	}
}

func benchHelp(N int, chanBase *chanRec, nProcs int) time.Duration {
	const nIters = 1
	var totalTime int64
	for iter := 0; iter < nIters; iter++ {
		var waitSetup, waitBench sync.WaitGroup
		nProcsPer := nProcs / 2
		pt := N / nProcsPer
		waitSetup.Add(2*nProcsPer + 1)
		for i := 0; i < nProcsPer; i++ {
			waitBench.Add(2)
			go func() {
				ch := chanBase.NewHandle()
				var (
					m   interface{} = 1
					msg fchan.Elt   = &m
				)
				waitSetup.Done()
				waitSetup.Wait()
				for j := 0; j < pt; j++ {
					chanBase.Enqueue(ch, msg)
				}
				waitBench.Done()
			}()
			go func() {
				ch := chanBase.NewHandle()
				waitSetup.Done()
				waitSetup.Wait()
				for j := 0; j < pt; j++ {
					chanBase.Dequeue(ch)
				}
				waitBench.Done()
			}()
		}
		time.Sleep(time.Millisecond * 5)
		waitSetup.Done()
		waitSetup.Wait()
		start := time.Now().UnixNano()
		waitBench.Wait()
		end := time.Now().UnixNano()
		runtime.GC()
		time.Sleep(time.Second)
		totalTime += end - start
	}
	return time.Duration(totalTime/nIters) * time.Nanosecond
}

func render(N, numCPUs int, gmp bool, desc string, t time.Duration) {
	extra := ""
	if gmp {
		extra = "GMP"
	}
	fmt.Printf("%s%s-%d\t%d\t%v\n", desc, extra, numCPUs, N, t)
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")

func main() {
	const (
		more     = 5000
		nOps     = 10000000
		gmpScale = 1
	)
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	for _, pack := range []struct {
		desc string
		f    func() *chanRec
	}{
		{"Chan10M", func() *chanRec { return wrapChan(nOps) }},
		{"Chan1K", func() *chanRec { return wrapChan(1024) }},
		{"Chan0", func() *chanRec { return wrapChan(0) }},
		{"Bounded1K", func() *chanRec { return wrapBounded(1024) }},
		{"Bounded0", func() *chanRec { return wrapBounded(0) }},
		{"Unbounded", wrapUnbounded},
	} {
		for _, nprocs := range []int{2, 4, 8, 12, 16, 24, 28, 32} {
			runtime.GOMAXPROCS(nprocs)
			dur := benchHelp(nOps, pack.f(), more)
			render(nOps, nprocs, false, pack.desc, dur)
			dur = benchHelp(nOps, pack.f(), nprocs*gmpScale)
			render(nOps, nprocs, true, pack.desc, dur)
		}
	}
}
