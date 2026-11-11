/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	clockQueueSize = 256
	clockScanBatch = 64
)

type clockEntry struct {
	keys []BlockHash
	slot *clockSlot
}

type clockSlot struct {
	key  BlockHash
	used atomic.Bool
	dead atomic.Bool
	// Policy links belong exclusively to the maintenance worker.
	prev, next *clockSlot
}

func (slot *clockSlot) touch() {
	if !slot.used.Load() {
		slot.used.Store(true)
	}
}

type clockCommand struct {
	slot    *clockSlot
	remove  bool
	barrier chan struct{}
}

type clockState struct {
	entries  sync.Map // BlockHash -> *clockEntry
	capacity int64
	// Reservations cover live, retired and queued slots, including unpublished inserts.
	reserved atomic.Int64
	live     atomic.Int64
	waiters  atomic.Int64
	commands chan clockCommand
	reclaim  chan struct{}
	space    chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// clockEngineKeys publishes mapping changes synchronously. Its worker owns
// policy links; readers only access immutable values and a per-slot usage bit.
type clockEngineKeys struct {
	state   *clockState
	cleanup runtime.Cleanup
}

func newClockEngineKeys(size int) (*clockEngineKeys, error) {
	if size <= 0 {
		return nil, fmt.Errorf("engine key capacity must be positive: %d", size)
	}
	state := &clockState{
		capacity: int64(size), commands: make(chan clockCommand, min(size, clockQueueSize)),
		reclaim: make(chan struct{}, 1), space: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	s := &clockEngineKeys{state: state}
	// The worker and cleanup argument do not reference s. KeepAlive in each
	// operation prevents collection while that operation uses state.
	s.cleanup = runtime.AddCleanup(s, func(state *clockState) { state.close() }, state)
	go state.run()
	return s, nil
}

func (s *clockEngineKeys) Close() {
	s.cleanup.Stop()
	s.state.close()
}

func (s *clockState) close() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *clockEngineKeys) Get(key BlockHash) ([]BlockHash, bool) {
	defer runtime.KeepAlive(s)
	value, ok := s.state.entries.Load(key)
	if !ok {
		return nil, false
	}
	entry := value.(*clockEntry)
	entry.slot.touch()
	return entry.keys, true
}

func (s *clockEngineKeys) Peek(key BlockHash) ([]BlockHash, bool) {
	defer runtime.KeepAlive(s)
	value, ok := s.state.entries.Load(key)
	if !ok {
		return nil, false
	}
	return value.(*clockEntry).keys, true
}

func (s *clockEngineKeys) Contains(key BlockHash) bool { _, ok := s.Peek(key); return ok }
func (s *clockEngineKeys) Len() int                    { defer runtime.KeepAlive(s); return int(s.state.live.Load()) }

func (s *clockEngineKeys) Put(key BlockHash, keys []BlockHash) {
	defer runtime.KeepAlive(s)
	state := s.state
	var owned []BlockHash
	for {
		if value, ok := state.entries.Load(key); ok {
			old := value.(*clockEntry)
			if slices.Equal(old.keys, keys) {
				old.slot.touch()
				return
			}
			if owned == nil {
				owned = slices.Clone(keys)
			}
			if state.entries.CompareAndSwap(key, old, &clockEntry{keys: owned, slot: old.slot}) {
				old.slot.touch()
				return
			}
			continue
		}
		state.reserve()
		if owned == nil {
			owned = slices.Clone(keys)
		}
		slot := &clockSlot{key: key}
		slot.used.Store(true)
		entry := &clockEntry{keys: owned, slot: slot}
		// Count before publication: Remove can run as soon as the entry is visible.
		state.live.Add(1)
		if _, loaded := state.entries.LoadOrStore(key, entry); loaded {
			state.live.Add(-1)
			state.release()
			continue
		}
		state.enqueue(clockCommand{slot: slot})
		return
	}
}

func (s *clockEngineKeys) Remove(key BlockHash) bool {
	defer runtime.KeepAlive(s)
	state := s.state
	for {
		value, ok := state.entries.Load(key)
		if !ok {
			return false
		}
		entry := value.(*clockEntry)
		if state.entries.CompareAndDelete(key, entry) {
			state.live.Add(-1)
			entry.slot.dead.Store(true)
			state.enqueue(clockCommand{slot: entry.slot, remove: true})
			return true
		}
	}
}

// wait drains policy commands submitted before the barrier. Producers must be
// quiescent when a caller needs a fully settled snapshot.
func (s *clockEngineKeys) wait() {
	defer runtime.KeepAlive(s)
	barrier := make(chan struct{})
	s.state.enqueue(clockCommand{barrier: barrier})
	select {
	case <-barrier:
	case <-s.state.stop:
	}
}

func (s *clockState) enqueue(command clockCommand) {
	select {
	case s.commands <- command:
	case <-s.stop:
		panic("engine key store is closed")
	}
}

func (s *clockState) reserve() {
	for {
		count := s.reserved.Load()
		if count < s.capacity {
			if s.reserved.CompareAndSwap(count, count+1) {
				if count+1 < s.capacity {
					s.notifySpace()
				}
				return
			}
			continue
		}
		s.waiters.Add(1)
		s.kick()
		select {
		case <-s.space:
		case <-s.stop:
			panic("engine key store is closed")
		}
		s.waiters.Add(-1)
	}
}

func (s *clockState) notifySpace() {
	select {
	case s.space <- struct{}{}:
	default:
	}
}
func (s *clockState) kick() {
	select {
	case s.reclaim <- struct{}{}:
	default:
	}
}
func (s *clockState) release() { s.reserved.Add(-1); s.notifySpace() }

func (s *clockState) run() {
	defer close(s.done)
	root := &clockSlot{}
	root.next, root.prev = root, root
	cursor := root
	unlink := func(slot *clockSlot) {
		if cursor == slot {
			cursor = slot.prev
		}
		slot.prev.next, slot.next.prev = slot.next, slot.prev
		slot.prev, slot.next = nil, nil
		s.release()
	}
	for {
		select {
		case <-s.stop:
			return
		case command := <-s.commands:
			if command.barrier != nil {
				close(command.barrier)
				continue
			}
			slot := command.slot
			if command.remove {
				if slot.prev != nil {
					unlink(slot)
				}
			} else if slot.dead.Load() {
				s.release()
			} else {
				slot.prev, slot.next = root.prev, root
				root.prev.next, root.prev = slot, slot
			}
			if s.waiters.Load() > 0 && s.reserved.Load() >= s.capacity {
				s.kick()
			}
		case <-s.reclaim:
			for range clockScanBatch {
				if s.waiters.Load() == 0 || s.reserved.Load() < s.capacity || root.next == root {
					break
				}
				cursor = cursor.next
				if cursor == root {
					cursor = cursor.next
				}
				slot := cursor
				value, ok := s.entries.Load(slot.key)
				if !ok || value.(*clockEntry).slot != slot {
					unlink(slot)
					continue
				}
				if slot.used.Swap(false) {
					continue
				}
				// Compare the complete immutable value, including replacement identity.
				if s.entries.CompareAndDelete(slot.key, value) {
					s.live.Add(-1)
					unlink(slot)
				}
			}
			if s.waiters.Load() > 0 && s.reserved.Load() >= s.capacity && root.next != root {
				s.kick()
			}
		}
	}
}
