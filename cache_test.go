package walmap

import (
	"bytes"
	"encoding/gob"
	"testing"
)

type testStruct struct{ Id int }

func init() {
	gob.Register(testStruct{})
}

func TestCacheSetGet(t *testing.T) {
	t.Run("nil", func(tt *testing.T) {
		c := newWalCache(newDefaultOption())
		c.Set("test", nil)

		v, ok := c.Get("test")
		if ok != true {
			tt.Errorf("key exists")
		}
		if v != nil {
			tt.Errorf("nil value")
		}
	})
	t.Run("scala", func(tt *testing.T) {
		c := newWalCache(newDefaultOption())
		c.Set("test1", "value1")
		c.Set("test2", 123456)

		v1, ok1 := c.Get("test1")
		if ok1 != true {
			tt.Errorf("key exists")
		}
		if s, ok := v1.(string); ok != true {
			tt.Errorf("type: %T", v1)
		} else {
			if s != "value1" {
				tt.Errorf("actual: %s", s)
			}
		}
		v2, ok2 := c.Get("test2")
		if ok2 != true {
			tt.Errorf("key exists")
		}
		if s, ok := v2.(int); ok != true {
			tt.Errorf("type: %T", v1)
		} else {
			if s != 123456 {
				tt.Errorf("actual: %d", s)
			}
		}
	})
	t.Run("struct", func(tt *testing.T) {
		c := newWalCache(newDefaultOption())
		c.Set("test1", testStruct{123})
		c.Set("test2", testStruct{456})

		v1, ok1 := c.Get("test1")
		if ok1 != true {
			tt.Errorf("key exists")
		}
		if s, ok := v1.(testStruct); ok != true {
			tt.Errorf("type: %T", v1)
		} else {
			if s.Id != 123 {
				tt.Errorf("actual: %d", s.Id)
			}
		}
		v2, ok2 := c.Get("test2")
		if ok2 != true {
			tt.Errorf("key exists")
		}
		if s, ok := v2.(testStruct); ok != true {
			tt.Errorf("type: %T", v2)
		} else {
			if s.Id != 456 {
				tt.Errorf("actual: %d", s.Id)
			}
		}
	})
}

func TestCacheSnapshotRestore(t *testing.T) {
	check := func(t *testing.T, c *walCache) {
		v1, ok1 := c.Get("test1")
		if ok1 != true {
			t.Errorf("exists key")
		}
		if s, ok := v1.(string); ok != true {
			t.Errorf("type %T", v1)
		} else {
			if s != "value1" {
				t.Errorf("actual: %s", s)
			}
		}
		v2, ok2 := c.Get("test2")
		if ok2 != true {
			t.Errorf("exists key")
		}
		if s, ok := v2.(string); ok != true {
			t.Errorf("type %T", v2)
		} else {
			if s != "value2" {
				t.Errorf("actual: %s", s)
			}
		}
		v3, ok3 := c.Get("test3")
		if ok3 != true {
			t.Errorf("exists key")
		}
		if s, ok := v3.(string); ok != true {
			t.Errorf("type %T", v3)
		} else {
			if s != "value3" {
				t.Errorf("actual: %s", s)
			}
		}
		v4, ok4 := c.Get("test4")
		if ok4 != true {
			t.Errorf("exists key")
		}
		if s, ok := v4.(string); ok != true {
			t.Errorf("type %T", v4)
		} else {
			if s != "value4" {
				t.Errorf("actual: %s", s)
			}
		}
		v5, ok5 := c.Get("test5")
		if ok5 != true {
			t.Errorf("exists key")
		}
		if s, ok := v5.(string); ok != true {
			t.Errorf("type %T", v5)
		} else {
			if s != "value5" {
				t.Errorf("actual: %s", s)
			}
		}
	}

	c1 := newWalCache(newDefaultOption())
	c1.Set("test1", "value1")
	c1.Set("test2", "value2")
	c1.Set("test3", "value3")
	c1.Set("test4", "value4")
	c1.Set("test5", "value5")

	t.Logf("org")
	check(t, c1)

	out := bytes.NewBuffer(nil)
	if err := c1.Snapshot(out); err != nil {
		t.Fatalf("no error: %+v", err)
	}

	c2, err := restoreWalCache(bytes.NewReader(out.Bytes()), newDefaultOption())
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}

	t.Logf("restred")
	check(t, c2)
}
