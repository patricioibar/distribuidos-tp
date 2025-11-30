package persistance

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type StateLog interface {
	Append(entry []byte) error
	Commit() error
	RotateWAL() error
	WriteSnapshot(state []byte) (string, error)
	Restore() ([]byte, EntryIterator, error)
	GetLogsIterator() (EntryIterator, error)
	Close() error
}

type stateLog struct {
	dir         string
	walDir      string
	snapshotDir string
	walTs       int64
	walFile     *os.File
}

const (
	walPrefix  = "wal-"
	walSuffix  = ".log"
	snapPrefix = "snapshot-"
	snapSuffix = ".snap"
)

func NewStateLog(dir string) (StateLog, error) {
	l := &stateLog{
		dir:         dir,
		walDir:      filepath.Join(dir, "wal"),
		snapshotDir: filepath.Join(dir, "snapshots"),
	}

	if err := os.MkdirAll(l.walDir, 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(l.snapshotDir, 0755); err != nil {
		return nil, err
	}

	err := l.openNewWAL()
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *stateLog) openNewWAL() error {

	if l.walFile != nil {
		l.walFile.Close()
	}

	l.walTs = time.Now().UnixNano()
	path := filepath.Join(l.walDir, fmt.Sprintf("%s%d%s", walPrefix, l.walTs, walSuffix))
	wf, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	l.walFile = wf
	return nil
}

// Append the entry to the WAL file, fmt: <lenght><entry>
func (l *stateLog) Append(entry []byte) error {
	if l.walFile == nil {
		return fmt.Errorf("wal not opened")
	}
	if err := binary.Write(l.walFile, binary.LittleEndian, uint32(len(entry))); err != nil {
		return err
	}
	_, err := l.walFile.Write(entry)
	return err
}

// Commt by running the fsync on the current WAL
func (l *stateLog) Commit() error {
	if l.walFile == nil {
		return fmt.Errorf("wal not opened")
	}
	return l.walFile.Sync()
}

// RotateWAL: commit & start a new timestamped WAL segment
func (l *stateLog) RotateWAL() error {
	if err := l.Commit(); err != nil {
		return err
	}
	return l.openNewWAL()
}

// Write a temporary snapshot file, then renameit to a permanent one, renaming makes the write "atomic", in the sense that it either all the content is written or none of it is.
func (l *stateLog) WriteSnapshot(state []byte) (string, error) {
	ts := time.Now().UnixNano()
	tmp := filepath.Join(l.snapshotDir, fmt.Sprintf("%s%d%s.tmp", snapPrefix, ts, snapSuffix))
	dst := filepath.Join(l.snapshotDir, fmt.Sprintf("%s%d%s", snapPrefix, ts, snapSuffix))

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(state); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}

	if err := os.Rename(tmp, dst); err != nil {
		return "", err
	}

	// best-effort directory fsync
	if d, err := os.Open(l.snapshotDir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	return filepath.Base(dst), nil
}

// Returns the latest snapshot data and an iterator over WAL entries after that snapshot
func (l *stateLog) Restore() ([]byte, EntryIterator, error) {
	// 1: Primero agarro el ultimo archivo de snapshot
	snaps, err := filepath.Glob(filepath.Join(l.snapshotDir, snapPrefix+"*"+snapSuffix))
	if err != nil {
		return nil, nil, err
	}

	var snapData []byte
	var snapTs int64 = 0
	if len(snaps) > 0 {
		sort.Strings(snaps)
		latest := snaps[len(snaps)-1]
		b, err := os.ReadFile(latest)
		if err != nil {
			return nil, nil, err
		}

		snapData = b

		base := filepath.Base(latest)
		ts, err := parseTsFromFilename(base, snapPrefix, snapSuffix)
		if err != nil {
			return nil, nil, err
		}

		snapTs = ts
	}

	// 2: despues agarro los archivos del wals que se hayan escrito despues del ultimo snapshot
	wals, err := filepath.Glob(filepath.Join(l.walDir, walPrefix+"*"+walSuffix))
	if err != nil {
		return nil, nil, err
	}
	type wf struct {
		path string
		ts   int64
	}

	var candidates []wf
	for _, p := range wals {
		base := filepath.Base(p)
		ts, err := parseTsFromFilename(base, walPrefix, walSuffix)
		if err != nil {
			continue
		}

		if ts > snapTs {
			candidates = append(candidates, wf{path: p, ts: ts})
		}
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ts < candidates[j].ts })

	// 3: Build list of WAL file paths for iterator
	var walPaths []string
	for _, c := range candidates {
		walPaths = append(walPaths, c.path)
	}

	// Create iterator for the WAL files
	iter := newWALIterator(walPaths)

	return snapData, iter, nil
}

func (l *stateLog) GetLogsIterator() (EntryIterator, error) {
	WALPaths, err := filepath.Glob(filepath.Join(l.walDir, walPrefix+"*"+walSuffix))
	if err != nil {
		return nil, err
	}
	if WALPaths == nil {
		return nil, fmt.Errorf("no WAL files found")
	}
	sort.Strings(WALPaths)
	return newWALIterator(WALPaths), nil
}

// Closes the currently opended WAL file
func (l *stateLog) Close() error {
	if l.walFile != nil {
		return l.walFile.Close()
	}
	return nil
}

// parseTsFromFilename extracts integer timestamp from filenames like "prefix-<ts>.suffix"
func parseTsFromFilename(name, prefix, suffix string) (int64, error) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, fmt.Errorf("invalid name format: %s", name)
	}
	trim := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	ts, err := strconv.ParseInt(trim, 10, 64)
	if err != nil {
		return 0, err
	}
	return ts, nil
}
