// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package wireclient

import (
	"io"
	"sync"
	"sync/atomic"
)

// combiner writes frames from many goroutines without a writer goroutine:
// whoever finds the connection free writes, and takes along every frame
// that others queued meanwhile, in one write. When the connection is idle
// a response goes out at once, with no hand-over to another goroutine
// (which costs a wake-up); when it is busy, responses gather.
// maxQueued is how many frames may wait for a busy connection.
const maxQueued = 1024

type combiner struct {
	w      io.Writer
	onFail func(error)

	writing sync.Mutex // held while writing
	qmu     sync.Mutex
	queue   [][]byte
	buf     []byte // reused to join frames
	failed  atomic.Bool
}

func (c *combiner) send(frame []byte) {
	if c.failed.Load() {
		return
	}
	c.qmu.Lock()
	c.queue = append(c.queue, frame)
	full := len(c.queue) >= maxQueued
	c.qmu.Unlock()
	// If another goroutine is writing, it will see the frame: it checks
	// the queue again after letting go of writing. A full queue waits for
	// the writer instead, so a client that stops reading slows its own
	// requests down rather than filling the memory.
	if full {
		c.writing.Lock()
	} else if !c.writing.TryLock() {
		return
	}
	for {
		c.qmu.Lock()
		frames := c.queue
		c.queue = nil
		c.qmu.Unlock()
		if len(frames) > 0 && !c.failed.Load() {
			if err := c.write(frames); err != nil && c.failed.CompareAndSwap(false, true) {
				c.onFail(err)
			}
		}
		c.writing.Unlock()
		c.qmu.Lock()
		empty := len(c.queue) == 0
		c.qmu.Unlock()
		if empty || !c.writing.TryLock() {
			return
		}
	}
}

// write sends frames in one write. Call with writing held.
func (c *combiner) write(frames [][]byte) error {
	if len(frames) == 1 {
		_, err := c.w.Write(frames[0])
		return err
	}
	b := c.buf[:0]
	for _, f := range frames {
		b = append(b, f...)
	}
	if cap(b) <= 1<<20 {
		c.buf = b // keep a moderate buffer for the next time
	}
	_, err := c.w.Write(b)
	return err
}
