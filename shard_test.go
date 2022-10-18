package walmap

import (
	"bytes"
	"testing"
)

func TestShardsSnapshotRestore(t *testing.T) {
	check := func(t *testing.T, s *shards) {
		v1, ok1 := s.GetShard("test1").Get("test1")
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
		v2, ok2 := s.GetShard("test2").Get("test2")
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
		v3, ok3 := s.GetShard("test3").Get("test3")
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
		v4, ok4 := s.GetShard("test4").Get("test4")
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
		v5, ok5 := s.GetShard("test5").Get("test5")
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

	s1 := newShards(newDefaultOption())
	s1.GetShard("test1").Set("test1", "value1")
	s1.GetShard("test2").Set("test2", "value2")
	s1.GetShard("test3").Set("test3", "value3")
	s1.GetShard("test4").Set("test4", "value4")
	s1.GetShard("test5").Set("test5", "value5")

	t.Logf("org")
	check(t, s1)

	out := bytes.NewBuffer(nil)
	if err := s1.Snapshot(out); err != nil {
		t.Fatalf("no error: %+v", err)
	}

	s2, err := restoreShards(bytes.NewReader(out.Bytes()), newDefaultOption())
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}

	t.Logf("restored")
	check(t, s2)
}
