package persistance

import (
	"encoding/binary"
	"fmt"
	"os"
)

type SimpleWAL interface {
	Append(entry []byte) error
	Commit() error
	Restore() (EntryIterator, error)
	Close() error
}

type simpleWAL struct {
	walPath string
	walFile *os.File
}

func newSimpleWAL(walPath string) (SimpleWAL, error) {
	wf, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &simpleWAL{
		walPath: walPath,
		walFile: wf,
	}, nil
}

func (l *simpleWAL) Append(entry []byte) error {
	if l.walFile == nil {
		return fmt.Errorf("wal not opened")
	}
	if err := binary.Write(l.walFile, binary.LittleEndian, uint32(len(entry))); err != nil {
		return err
	}
	_, err := l.walFile.Write(entry)
	return err
}

func (l *simpleWAL) Commit() error {
	if l.walFile == nil {
		return fmt.Errorf("wal not opened")
	}
	return l.walFile.Sync()
}

// Returns an iterator over the WAL entries
func (l *simpleWAL) Restore() (EntryIterator, error) {
	return newWALIterator([]string{l.walPath}), nil
}

func (l *simpleWAL) getLastEntry() ([]byte, error) {
	it, err := l.Restore()
	if err != nil {
		return nil, err
	}
	lastEntry, err := it.Last()
	if err != nil {
		return nil, err
	}
	return lastEntry, nil
}

func (l *simpleWAL) Close() error {
	if l.walFile != nil {
		return l.walFile.Close()
	}
	return nil
}
