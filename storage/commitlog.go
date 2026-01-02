package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// commitLogState holds the durable state for a commit log.
type commitLogState struct {
	LastAppliedIndex     uint64           `json:"last_applied_index"`
	ProducerLastSequence map[uint64]int64 `json:"producer_last_sequence"`
}

const (
	// Each record is prefixed with an 8-byte integer indicating its length.
	lenWidth = 8
	// Initial mmap size (1 MB)
	initialMmapSize = 1 << 20
	// Maximum mmap size (1 GB)
	maxMmapSize = 1 << 30
)

// CommitLog represents an append-only log file on disk
type CommitLog struct {
	mu        sync.RWMutex
	file      *os.File
	size      int64
	mmapData  []byte
	mmapSize  int64
	state     commitLogState
	statePath string
}

// NewCommitLog creates or opens a commit log file.
func NewCommitLog(path string) (*CommitLog, error) {
	permissions := os.FileMode(0666)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, permissions)
	if err != nil {
		return nil, fmt.Errorf("failed to open commit log file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	commitLog := &CommitLog{
		file:      f,
		size:      fi.Size(),
		statePath: path + ".state",
	}

	// Initialize mmap
	if err := commitLog.initMmap(); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to initialize mmap: %w", err)
	}

	// Load the persisted state from disk.
	if err := commitLog.loadState(); err != nil {
		commitLog.Close()
		return nil, fmt.Errorf("failed to load commit log state: %w", err)
	}

	return commitLog, nil
}

// initMmap initializes the memory-mapped region for the file.
func (c *CommitLog) initMmap() error {
	// Determine the size to mmap
	mmapSize := int64(initialMmapSize)
	if c.size > 0 {
		// Round up to the nearest power of 2
		for mmapSize < c.size && mmapSize < maxMmapSize {
			mmapSize *= 2
		}
	}

	// Ensure the file is at least mmapSize
	if err := c.file.Truncate(mmapSize); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Memory map the file
	data, err := unix.Mmap(int(c.file.Fd()), 0, int(mmapSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %w", err)
	}

	c.mmapData = data
	c.mmapSize = mmapSize
	return nil
}

// remapIfNeeded grows the mmap region if necessary.
func (c *CommitLog) remapIfNeeded(requiredSize int64) error {
	if requiredSize <= c.mmapSize {
		return nil
	}

	// Unmap the current region
	if err := unix.Munmap(c.mmapData); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}

	// Calculate new size (double until it fits)
	newSize := c.mmapSize * 2
	for newSize < requiredSize && newSize < maxMmapSize {
		newSize *= 2
	}

	if newSize > maxMmapSize {
		newSize = maxMmapSize
	}

	if requiredSize > newSize {
		return fmt.Errorf("required size exceeds maximum mmap size")
	}

	// Grow the file
	if err := c.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Remap with new size
	data, err := unix.Mmap(int(c.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to remap file: %w", err)
	}

	c.mmapData = data
	c.mmapSize = newSize
	return nil
}

// loadState reads the .state file from disk.
func (c *CommitLog) loadState() error {
	data, err := os.ReadFile(c.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			// State file doesn't exist, start with a fresh state.
			c.state = commitLogState{
				LastAppliedIndex:     0,
				ProducerLastSequence: make(map[uint64]int64),
			}
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &c.state)
}

// persistState writes the current state to the .state file.
func (c *CommitLog) persistState() error {
	data, err := json.Marshal(c.state)
	if err != nil {
		return err
	}

	return os.WriteFile(c.statePath, data, 0644)
}

// AppendIdempotent appends a record to the log if it hasn't been seen before.
func (c *CommitLog) AppendIdempotent(producerID uint64, sequenceNumber int64, data []byte) (offset int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lastSeq, ok := c.state.ProducerLastSequence[producerID]
	if ok && sequenceNumber <= lastSeq {
		// Duplicate or out-of-order message; skip appending.
		return -1, nil
	}

	pos := c.size
	recordSize := int64(lenWidth + len(data))
	newSize := c.size + recordSize

	// Ensure mmap region is large enough
	if err := c.remapIfNeeded(newSize); err != nil {
		return 0, fmt.Errorf("failed to remap: %w", err)
	}

	// Write the length of the data as an 8-byte header directly to mmap
	binary.BigEndian.PutUint64(c.mmapData[pos:pos+lenWidth], uint64(len(data)))

	// Write the actual data to mmap
	copy(c.mmapData[pos+lenWidth:pos+recordSize], data)

	// Update the in-memory size of the log
	c.size = newSize

	return pos, nil
}

// Append writes a new record to the end of the log.
func (c *CommitLog) Append(data []byte) (offset int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pos := c.size
	recordSize := int64(lenWidth + len(data))
	newSize := c.size + recordSize

	// Ensure mmap region is large enough
	if err := c.remapIfNeeded(newSize); err != nil {
		return 0, fmt.Errorf("failed to remap: %w", err)
	}

	// Write the length of the data as an 8-byte header directly to mmap
	binary.BigEndian.PutUint64(c.mmapData[pos:pos+lenWidth], uint64(len(data)))

	// Write the actual data to mmap
	copy(c.mmapData[pos+lenWidth:pos+recordSize], data)

	// Update the in-memory size of the log
	c.size = newSize

	return pos, nil
}

// Read retrieves a record from a specific offset in the log.
func (c *CommitLog) Read(offset int64) ([]byte, int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if offset >= c.size {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	// Read the length prefix from mmap
	recordLen := binary.BigEndian.Uint64(c.mmapData[offset : offset+lenWidth])

	// Read the record data from mmap
	data := make([]byte, recordLen)
	copy(data, c.mmapData[offset+lenWidth:offset+lenWidth+int64(recordLen)])

	nextOffset := offset + int64(lenWidth) + int64(recordLen)

	return data, nextOffset, nil
}

// Close gracefully closes the log file.
func (c *CommitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Sync mmap to disk before unmapping
	if c.mmapData != nil {
		if err := unix.Msync(c.mmapData, unix.MS_SYNC); err != nil {
			return fmt.Errorf("failed to sync mmap: %w", err)
		}

		// Unmap the memory
		if err := unix.Munmap(c.mmapData); err != nil {
			return fmt.Errorf("failed to unmap: %w", err)
		}
		c.mmapData = nil
	}

	// Truncate the file to actual size
	if err := c.file.Truncate(c.size); err != nil {
		return fmt.Errorf("failed to truncate file to size: %w", err)
	}

	return c.file.Close()
}

func (c *CommitLog) GetLastAppliedIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.LastAppliedIndex
}

func (c *CommitLog) SetCommitLogState(index uint64, producerID uint64, sequenceNumber int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.LastAppliedIndex = index
	c.state.ProducerLastSequence[producerID] = sequenceNumber
	return c.persistState()
}
