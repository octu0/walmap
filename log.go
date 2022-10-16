package walmap

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/octu0/walmap/codec"
)

var (
	ErrCompactRunning = errors.New("compat already in progress")
)

type Log struct {
	mutex       *sync.RWMutex
	buf         *bytes.Buffer
	compacting  bool
	lastIndex   codec.Index
	reclaimable uint64
}

func (l *Log) Write(data []byte) (codec.Index, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	index := l.lastIndex
	nextIndex, err := codec.Encode(l.buf, index, data)
	if err != nil {
		return 0, err
	}
	l.lastIndex = nextIndex
	return index, nil
}

func (l *Log) Read(index codec.Index) ([]byte, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	buf := l.buf.Bytes()
	_, data, err := codec.Decode(bytes.NewReader(buf[index:]))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (l *Log) Delete(index codec.Index) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	buf := l.buf.Bytes()
	header, err := codec.DecodeHeader(bytes.NewReader(buf[index:]))
	if err != nil {
		return err
	}
	prevIndex := header.Prev
	prevHeader, err := codec.DecodeHeader(bytes.NewReader(buf[prevIndex:]))
	if err != nil {
		return err
	}
	prevHeader.Next = header.Next
	if err := codec.RewriteHeader(buf[prevIndex:prevIndex+codec.Index(codec.HeaderSize)], prevHeader); err != nil {
		return err
	}
	l.reclaimable += header.Size + codec.HeaderSize
	return nil
}

func (l *Log) ReclaimableSpace() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.reclaimable
}

func (l *Log) compactRunning() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.compacting
}

func (l *Log) makeCopy() (*bytes.Buffer, codec.Index, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	newLog, err := RestoreLog(bytes.NewReader(l.buf.Bytes()), l.buf.Len(), noopEachFunc)
	if err != nil {
		return nil, 0, err
	}
	return newLog.buf, newLog.lastIndex, nil
}

func (l *Log) Compact() error {
	if l.compactRunning() {
		return ErrCompactRunning
	}

	l.mutex.Lock()
	l.compacting = true
	l.mutex.Unlock()
	defer func() {
		l.mutex.Lock()
		l.compacting = false
		l.mutex.Unlock()
	}()

	newBuf, newLastIndex, err := l.makeCopy()
	if err != nil {
		return err
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.buf = newBuf
	l.lastIndex = newLastIndex
	l.reclaimable = 0
	return nil
}

func (l *Log) Bytes() []byte {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.buf.Bytes()
}

type eachFunc func(codec.Index, []byte) error

func noopEachFunc(codec.Index, []byte) error {
	return nil
}

func RestoreLog(r io.Reader, initialSize int, each eachFunc) (*Log, error) {
	lastIndex := codec.Index(0)
	newBuf := bytes.NewBuffer(make([]byte, 0, initialSize))
	for {
		header, data, err := codec.Decode(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if err := each(header.Prev, data); err != nil {
			return nil, err
		}
		next, err := codec.Encode(newBuf, lastIndex, data)
		if err != nil {
			return nil, err
		}
		lastIndex = next
	}

	return &Log{
		buf:       newBuf,
		lastIndex: lastIndex,
	}, nil
}

func NewLog(size int) *Log {
	return &Log{
		mutex:       new(sync.RWMutex),
		buf:         bytes.NewBuffer(make([]byte, 0, size)),
		compacting:  false,
		lastIndex:   codec.Index(0),
		reclaimable: uint64(0),
	}
}
