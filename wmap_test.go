package walmap

import (
	"testing"
)

func TestSet(t *testing.T) {
	m := NewWMap()
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
