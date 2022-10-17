# `walmap`

[![MIT License](https://img.shields.io/github/license/octu0/walmap)](https://github.com/octu0/walmap/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/octu0/walmap?status.svg)](https://godoc.org/github.com/octu0/walmap)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/walmap)](https://goreportcard.com/report/github.com/octu0/walmap)
[![Releases](https://img.shields.io/github/v/release/octu0/walmap)](https://github.com/octu0/walmap/releases)

`walmap` is a fast serializable concurrent Map implementation.  
Map interface is the same as [octu0/cmap](https://github.com/octu0/cmap).
Key/Value is stored using Bitcask-like WAL.

## Example

```go
package main

import (
	"bytes"
	"github.com/octu0/walmap"
)

func main() {
	m := walmap.New()
	m.Set("foo", "bar")
	m.Set("hello", "world")

	if v, ok := m.Get("foo"); ok {
		println(v.(string))
	}

	m.Remove("hello")

	if 0 < m.ReclaimableSpace() {
		// Clean deleted values from memory
		if err := m.Compact(); err != nil {
			panic(err)
		}
	}

	// Snapshot / Restore
	out := bytes.NewBuffer(nil)
	if err := m.Snapshot(out); err != nil {
		panic(err)
	}
	m2, err := walmap.Restore(bytes.NewReader(out.Bytes()))
	if err != nil {
		panic(err)
	}

	if v, ok := m2.Get("foo"); ok {
		println(v.(string))
	}
}
```

## Benchmark

5x to 9x faster than implementing Snapshot/Restore using [octu0/cmap](https://github.com/octu0/cmap)

```
goos: darwin
goarch: amd64
pkg: github.com/octu0/walmap
cpu: Intel(R) Core(TM) i5-8210Y CPU @ 1.60GHz
BenchmarkSnapshot
BenchmarkSnapshot/cmap/snapshot/10_000
BenchmarkSnapshot/cmap/snapshot/10_000-4         	     142	   8771184 ns/op
BenchmarkSnapshot/cmap/snapshot/500_000
BenchmarkSnapshot/cmap/snapshot/500_000-4        	       2	 566466707 ns/op
BenchmarkSnapshot/cmap/restore/10_000
BenchmarkSnapshot/cmap/restore/10_000-4          	      99	  10632182 ns/op
BenchmarkSnapshot/cmap/restore/500_000
BenchmarkSnapshot/cmap/restore/500_000-4         	       3	 429974238 ns/op
BenchmarkSnapshot/walmap/snapshot/10_000
BenchmarkSnapshot/walmap/snapshot/10_000-4       	     758	   1342532 ns/op
BenchmarkSnapshot/walmap/snapshot/500_000
BenchmarkSnapshot/walmap/snapshot/500_000-4      	      19	  57805311 ns/op
BenchmarkSnapshot/walmap/restore/10_000
BenchmarkSnapshot/walmap/restore/10_000-4        	      57	  19237020 ns/op
BenchmarkSnapshot/walmap/restore/500_000
BenchmarkSnapshot/walmap/restore/500_000-4       	       6	 200340997 ns/op
PASS
```
