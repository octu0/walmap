package walmap

import (
	"bytes"
	"testing"
)

func TestLogReadWrite(t *testing.T) {
	log := NewLog(10, 10)
	if err := log.Write("hello", []byte("world")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("hello2", []byte("world2")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("test", []byte("test123456")); err != nil {
		t.Errorf("no error: %+v", err)
	}

	data1, ok1, err := log.Read("hello")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	data2, ok2, err := log.Read("hello2")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	data3, ok3, err := log.Read("test")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	_, ok4, err := log.Read("___not___found___")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}

	if ok1 != true {
		t.Errorf("exists! 1")
	}
	if ok2 != true {
		t.Errorf("exists! 2")
	}
	if ok3 != true {
		t.Errorf("exists! 3")
	}
	if ok4 {
		t.Errorf("not found key")
	}
	if bytes.Equal(data1, []byte("world")) != true {
		t.Errorf("actual: %s", data1)
	}
	if bytes.Equal(data2, []byte("world2")) != true {
		t.Errorf("actual: %s", data2)
	}
	if bytes.Equal(data3, []byte("test123456")) != true {
		t.Errorf("actual: %s", data3)
	}
}

func TestLogDelete(t *testing.T) {
	log := NewLog(10, 10)
	if err := log.Write("hello", []byte("hello")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("world", []byte("world")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("test", []byte("test")); err != nil {
		t.Errorf("no error: %+v", err)
	}

	oldData, deleted, err := log.Delete("world")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if deleted != true {
		t.Errorf("deleted")
	}
	if bytes.Equal(oldData, []byte("world")) != true {
		t.Errorf("actual: %v", oldData)
	}
	notFoundData, notfoundDeleted, err := log.Delete("world")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if notfoundDeleted {
		t.Errorf("not found = no delete")
	}
	if bytes.Equal(notFoundData, []byte("")) != true {
		t.Errorf("empty")
	}

	data1, ok1, err := log.Read("hello")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	data3, ok3, err := log.Read("test")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if ok1 != true {
		t.Errorf("exists key!")
	}
	if ok3 != true {
		t.Errorf("exists key!")
	}
	if bytes.Equal(data1, []byte("hello")) != true {
		t.Errorf("actual: %v", data1)
	}
	if bytes.Equal(data3, []byte("test")) != true {
		t.Errorf("actual: %v", data3)
	}

	data2, ok2, err := log.Read("world")
	if err != nil {
		t.Errorf("actual: %v", data3)
	}
	if ok2 {
		t.Errorf("delted key")
	}
	if bytes.Equal(data2, []byte("")) != true {
		t.Errorf("empty")
	}
}

func TestLogCompact(t *testing.T) {
	log := NewLog(10, 10)
	if err := log.Write("hello", []byte("hello")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("keyA", []byte("valueA")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := log.Write("test", []byte("test")); err != nil {
		t.Errorf("no error: %+v", err)
	}

	if _, _, err := log.Delete("keyA"); err != nil {
		t.Errorf("no error: %+v", err)
	}

	if s := log.ReclaimableSpace(); s != 26 { // 26 = 8(keysize) + 8(datasize) + 4(len("keyA")) + 6(len("valueA"))
		t.Errorf("actual: %d", s)
	}

	prevBufSize := log.buf.Len()

	if err := log.Compact(); err != nil {
		t.Errorf("no error: %+v", err)
	}

	currBufSize := log.buf.Len()

	if s := log.ReclaimableSpace(); 0 != s {
		t.Errorf("actual: %d", s)
	}

	t.Logf("compact() datasize prev = %d byte, curr = %d byte", prevBufSize, currBufSize)

	if prevBufSize <= currBufSize {
		t.Errorf("no delete space")
	}

	data1, ok1, err := log.Read("hello")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	data3, ok3, err := log.Read("test")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if ok1 != true {
		t.Errorf("no delete key")
	}
	if ok3 != true {
		t.Errorf("no delete key")
	}
	if bytes.Equal(data1, []byte("hello")) != true {
		t.Errorf("actual: %v", data1)
	}
	if bytes.Equal(data3, []byte("test")) != true {
		t.Errorf("actual: %v", data3)
	}

	data2, ok2, err := log.Read("world")
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if ok2 {
		t.Errorf("deleted")
	}
	if bytes.Equal(data2, []byte("")) != true {
		t.Errorf("actual: %v", data2)
	}
}
