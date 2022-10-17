package codec

import (
	"bytes"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	index1 := Index(0)
	index2, err := Encode(buf, index1, "hello", []byte("world"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	index3, err := Encode(buf, index2, "hello2", []byte("world2"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if _, err := Encode(buf, index3, "test", []byte("test")); err != nil {
		t.Errorf("no error: %+v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	key1, data1, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if key1 != "hello" {
		t.Errorf("key actual:%s", key1)
	}
	if bytes.Equal(data1, []byte("world")) != true {
		t.Errorf("data actual:%v", data1)
	}

	key2, data2, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if key2 != "hello2" {
		t.Errorf("key actual:%s", key2)
	}
	if bytes.Equal(data2, []byte("world2")) != true {
		t.Errorf("data actual:%v", data2)
	}

	key3, data3, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if key3 != "test" {
		t.Errorf("key actual:%s", key3)
	}
	if bytes.Equal(data3, []byte("test")) != true {
		t.Errorf("data actual:%v", data3)
	}
}

func TestDecodeIndex(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	index1 := Index(0)
	index2, err := Encode(buf, index1, "hello", []byte("world"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if _, err := Encode(buf, index2, "foo", []byte("bar")); err != nil {
		t.Errorf("no error: %+v", err)
	}

	{
		r := bytes.NewReader(buf.Bytes()[index2:])
		key, data, err := Decode(r)
		if err != nil {
			t.Errorf("no error: %+v", err)
		}
		if key != "foo" {
			t.Errorf("key actual:%s", key)
		}
		if bytes.Equal(data, []byte("bar")) != true {
			t.Errorf("data actual:%v", data)
		}
	}
	{
		r := bytes.NewReader(buf.Bytes()[index1:])
		key, data, err := Decode(r)
		if err != nil {
			t.Errorf("no error: %+v", err)
		}
		if key != "hello" {
			t.Errorf("key actual:%s", key)
		}
		if bytes.Equal(data, []byte("world")) != true {
			t.Errorf("data actual:%v", data)
		}
	}
}
