package codec

import (
	"bytes"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	index1 := Index(0)
	index2, err := Encode(buf, index1, []byte("hello"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	index3, err := Encode(buf, index2, []byte("world"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	index4, err := Encode(buf, index3, []byte("test"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	h1, data1, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if h1.Prev != index1 {
		t.Errorf("prev actual:%d", h1.Prev)
	}
	if h1.Next != index2 {
		t.Errorf("next actual:%d", h1.Next)
	}
	if bytes.Equal(data1, []byte("hello")) != true {
		t.Errorf("data actual:%v", data1)
	}

	h2, data2, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if h2.Prev != index2 {
		t.Errorf("prev actual:%d", h2.Prev)
	}
	if h2.Next != index3 {
		t.Errorf("next actual:%d", h2.Next)
	}
	if bytes.Equal(data2, []byte("world")) != true {
		t.Errorf("data actual:%v", data2)
	}

	h3, data3, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if h3.Prev != index3 {
		t.Errorf("prev actual:%d", h3.Prev)
	}
	if h3.Next != index4 {
		t.Errorf("next actual:%d", h3.Next)
	}
	if bytes.Equal(data3, []byte("test")) != true {
		t.Errorf("data actual:%v", data3)
	}
}

func TestDecodeIndex(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	index1 := Index(0)
	index2, err := Encode(buf, index1, []byte("hello"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	index3, err := Encode(buf, index2, []byte("world"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}

	r := bytes.NewReader(buf.Bytes()[index2:])
	header, data, err := Decode(r)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if header.Prev != index2 {
		t.Errorf("prev actual:%d", header.Prev)
	}
	if header.Next != index3 {
		t.Errorf("next actual:%d", header.Next)
	}
	if bytes.Equal(data, []byte("world")) != true {
		t.Errorf("data actual:%v", data)
	}
}

func TestRewriteHeader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	index1 := Index(0)
	index2, err := Encode(buf, index1, []byte("hello"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	index3, err := Encode(buf, index2, []byte("world"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}

	if err := RewriteHeader(buf.Bytes()[index2:], Header{Prev: index3, Next: index1, Size: 2}); err != nil {
		t.Errorf("no error: %+v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	h1, data1, err := Decode(r)
	if h1.Prev != index1 {
		t.Errorf("prev actual:%d", h1.Prev)
	}
	if h1.Next != index2 {
		t.Errorf("next actual:%d", h1.Next)
	}
	if bytes.Equal(data1, []byte("hello")) != true {
		t.Errorf("data actual:%v", data1)
	}
}
