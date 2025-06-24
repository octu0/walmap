package walmap

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/octu0/cmap"
)

type testSet interface {
	Set(string, interface{})
}

type testKV struct {
	Key   string
	Value interface{}
}

func BenchmarkSnapshot(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	prepare := func(c testSet, size int) {
		for i := 0; i < size; i += 1 {
			key := strconv.Itoa(i)
			c.Set(key, key)
		}
	}
	snapshot := func(w io.Writer, c *cmap.CMap) error {
		enc := gob.NewEncoder(w)
		for _, k := range c.Keys() {
			v, ok := c.Get(k)
			if ok != true {
				continue
			}
			if err := enc.Encode(testKV{k, v}); err != nil {
				return err
			}
		}
		return nil
	}
	restore := func(r io.Reader, c *cmap.CMap) error {
		dec := gob.NewDecoder(r)
		for {
			kv := testKV{}
			if err := dec.Decode(&kv); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			c.Set(kv.Key, kv.Value)
		}
		return nil
	}

	b.Run("cmap/snapshot/10_000", func(tb *testing.B) {
		c := cmap.New()
		prepare(c, 10_000)
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			buf := bytes.NewBuffer(nil)
			if err := snapshot(buf, c); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
		}
	})
	b.Run("cmap/snapshot/500_000", func(tb *testing.B) {
		c := cmap.New()
		prepare(c, 500_000)
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			buf := bytes.NewBuffer(nil)
			if err := snapshot(buf, c); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
		}
	})
	b.Run("cmap/restore/10_000", func(tb *testing.B) {
		old := cmap.New()
		prepare(old, 10_000)
		buf := bytes.NewBuffer(nil)
		if err := snapshot(buf, old); err != nil {
			tb.Fatalf("no error: %+v", err)
		}
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			r := bytes.NewReader(buf.Bytes())
			c := cmap.New()
			if err := restore(r, c); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
			if c.Len() != 10_000 {
				tb.Errorf("restore failed: %d", c.Len())
			}
		}
	})
	b.Run("cmap/restore/500_000", func(tb *testing.B) {
		old := cmap.New()
		prepare(old, 500_000)
		buf := bytes.NewBuffer(nil)
		if err := snapshot(buf, old); err != nil {
			tb.Fatalf("no error: %+v", err)
		}
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			r := bytes.NewReader(buf.Bytes())
			c := cmap.New()
			if err := restore(r, c); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
			if c.Len() != 500_000 {
				tb.Errorf("restore failed: %d", c.Len())
			}
		}
	})
	b.Run("walmap/snapshot/10_000", func(tb *testing.B) {
		w := New()
		prepare(w, 10_000)
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			buf := bytes.NewBuffer(nil)
			if err := w.Snapshot(buf); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
		}
	})
	b.Run("walmap/snapshot/500_000", func(tb *testing.B) {
		w := New()
		prepare(w, 500_000)
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			buf := bytes.NewBuffer(nil)
			if err := w.Snapshot(buf); err != nil {
				tb.Fatalf("no error: %+v", err)
			}
		}
	})
	b.Run("walmap/restore/10_000", func(tb *testing.B) {
		old := New()
		prepare(old, 10_000)
		buf := bytes.NewBuffer(nil)
		if err := old.Snapshot(buf); err != nil {
			tb.Fatalf("no error: %+v", err)
		}
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			r := bytes.NewReader(buf.Bytes())
			w, err := Restore(r)
			if err != nil {
				tb.Fatalf("no error: %+v", err)
			}
			if w.Len() != 10_000 {
				tb.Errorf("restore failed: %d", w.Len())
			}
		}
	})
	b.Run("walmap/restore/500_000", func(tb *testing.B) {
		old := New()
		prepare(old, 500_000)
		buf := bytes.NewBuffer(nil)
		if err := old.Snapshot(buf); err != nil {
			tb.Fatalf("no error: %+v", err)
		}
		tb.ResetTimer()
		for i := 0; i < tb.N; i += 1 {
			r := bytes.NewReader(buf.Bytes())
			w, err := Restore(r)
			if err != nil {
				tb.Fatalf("no error: %+v", err)
			}
			if w.Len() != 500_000 {
				tb.Errorf("restore failed: %d", w.Len())
			}
		}
	})
}

func TestSetGet(t *testing.T) {
	m := New()
	m.Set("foo", "test1")

	if v, ok := m.Get("foo"); ok != true {
		t.Errorf("exists")
	} else {
		if v.(string) != "test1" {
			t.Errorf("actual: %v", v)
		}
	}

	m.Set("foo", "test2")
	if v, ok := m.Get("foo"); ok != true {
		t.Errorf("exists")
	} else {
		if v.(string) != "test2" {
			t.Errorf("actual: %v", v)
		}
	}
}
