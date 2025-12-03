package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// Each record is prefixed with an 8-byte integer indicating its length.
const LEN_WIDTH = 8

// CommitLog represents an append-only log file on disk
type CommitLog struct {
	mu   sync.RWMutex
	file *os.File
	size int64
}

// NewCommitLog creates or opens a commit log file.
func NewCommitLog(path string) (*CommitLog, error) {
	permissions := os.FileMode(0666)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, permissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open commit log file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return &CommitLog{
		file: f,
		size: fi.Size(),
	}, nil
}

// Append writes a new record to the end of the log.
func (c *CommitLog) Append(data []byte) (offset int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pos := c.size

	// Write the length of the data as an 8-byte header
	lenBuf := make([]byte, LEN_WIDTH)
	binary.BigEndian.PutUint64(lenBuf, uint64((len(data))))
	if _, err := c.file.Write(lenBuf); err != nil {
		return 0, fmt.Errorf("failed to write record length: %w", err)
	}

	// Write the actual data
	if _, err := c.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write record data: %w", err)
	}

	// Update the in-memory size of the log
	c.size += int64(LEN_WIDTH + len(data))

	return pos, nil
}

// Read retrieves a record from a specific offset in the log.
func (c *CommitLog) Read(offset int64) ([]byte, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= c.size {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	// Read the length prefix
	lenBuf := make([]byte, LEN_WIDTH)
	if _, err := c.file.ReadAt(lenBuf, offset); err != nil {
		return nil, 0, fmt.Errorf("failed to read record length: %w", err)
	}

	recordLen := binary.BigEndian.Uint64(lenBuf)

	// Read the record data
	data := make([]byte, recordLen)
	if _, err := c.file.ReadAt(data, offset+LEN_WIDTH); err != nil {
		return nil, 0, fmt.Errorf("failed to read record data: %w", err)
	}

	nextOffset := offset + int64(LEN_WIDTH) + int64(recordLen)

	return data, nextOffset, nil
}

// Close gracefully closes the log file.
func (c *CommitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.file.Close()
}

// Name returns the file name of the log.
func (c *CommitLog) Name() string {
	return c.file.Name()
}
