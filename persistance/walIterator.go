package persistance

import (
	"encoding/binary"
	"io"
	"os"
)

type EntryIterator interface {
	Next() ([]byte, error) // returns entry or io.EOF
	Last() ([]byte, error)
	Close() error
}

type walIterator struct {
	files []string // list of WAL paths in order
	idx   int      // index of current file
	f     *os.File // open file handle
}

func newWALIterator(files []string) EntryIterator {
	return &walIterator{
		files: files,
		idx:   0,
		f:     nil,
	}
}

func (it *walIterator) openNextFile() error {
	if it.f != nil {
		it.f.Close()
	}
	if it.idx >= len(it.files) {
		return io.EOF
	}
	f, err := os.Open(it.files[it.idx])
	if err != nil {
		return err
	}
	it.f = f
	it.idx++
	return nil
}

func (it *walIterator) Next() ([]byte, error) {
	if it.f == nil {
		if err := it.openNextFile(); err != nil {
			return nil, err
		}
	}

	for {
		var length uint32
		err := binary.Read(it.f, binary.LittleEndian, &length)
		if err != nil {
			// if we are at the end of file → go to next file
			if err == io.EOF {
				if err := it.openNextFile(); err != nil {
					return nil, err
				}
				continue // try reading again
			}
			return nil, err
		}

		buf := make([]byte, length)
		n, err := io.ReadFull(it.f, buf)
		if err != nil || uint32(n) < length {
			// torn entry → end of this WAL and no more valid entries
			if err := it.openNextFile(); err != nil {
				return nil, err
			}
			continue
		}
		return buf, nil
	}
}

func (it *walIterator) Close() error {
	if it.f != nil {
		return it.f.Close()
	}
	return nil
}

// Last returns the last valid entry from all WAL files and any error encountered.
// If a torn write or other error occurs, it returns the last successfully read entry
// along with the error. This allows callers to recover the last valid state while
// being aware that the WAL may have incomplete writes.
// Returns (nil, io.EOF) if no entries exist.
func (it *walIterator) Last() ([]byte, error) {
	var lastEntry []byte
	var lastErr error

	for {
		entry, err := it.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			// Store the error but keep the last valid entry
			lastErr = err
			break
		}
		lastEntry = entry
	}

	if lastEntry == nil {
		return nil, io.EOF
	}

	// Return last valid entry with any error that occurred after it
	return lastEntry, lastErr
}
