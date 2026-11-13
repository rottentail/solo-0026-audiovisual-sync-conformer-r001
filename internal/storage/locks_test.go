package storage

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKeyLocksSerializesSameKey verifies the mutual exclusion contract:
// actions for the same identity never overlap, regardless of contention.
func TestKeyLocksSerializesSameKey(t *testing.T) {
	locks := newKeyLocks()
	var active int32
	var overlaps int32
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = locks.do("take:shared", func() error {
				if atomic.AddInt32(&active, 1) != 1 {
					atomic.AddInt32(&overlaps, 1)
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&active, -1)
				return nil
			})
		}()
	}
	wait.Wait()
	if overlaps != 0 {
		t.Fatalf("same identity ran %d overlapping actions", overlaps)
	}
}

// TestKeyLocksRetiresIdleEntries verifies that finished identities do not
// leave lock objects behind after a large number of workflows.
func TestKeyLocksRetiresIdleEntries(t *testing.T) {
	locks := newKeyLocks()
	for i := 0; i < 2000; i++ {
		if err := locks.do("analysis:work-"+strconv.Itoa(i), func() error { return nil }); err != nil {
			t.Fatalf("run action: %v", err)
		}
	}
	if retained := locks.count(); retained != 0 {
		t.Fatalf("%d idle lock entries were retained", retained)
	}
}

// TestKeyLocksKeepsEntryForWaiter verifies that a pending waiter holds a
// reference, so cleanup cannot replace the entry with a second mutex while
// that waiter is still waiting for the same identity.
func TestKeyLocksKeepsEntryForWaiter(t *testing.T) {
	locks := newKeyLocks()
	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = locks.do("conform:shared", func() error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	<-holderStarted

	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		_ = locks.do("conform:shared", func() error { return nil })
	}()

	// The waiter must hold a reference before it blocks. Wait until it has
	// registered instead of assuming a fixed scheduling latency.
	var entry *keyLock
	var refs int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, refs = locks.inspect("conform:shared")
		if entry != nil && refs >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if entry == nil || refs < 2 {
		t.Fatalf("waiting identity entry is %v with %d references, want both parties retained", entry, refs)
	}

	close(holderRelease)
	<-holderDone
	<-waiterDone

	if retained := locks.count(); retained != 0 {
		t.Fatalf("%d idle lock entries were retained after all parties released", retained)
	}
}

// count and inspect are test-only views of the lock table.
func (k *keyLocks) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.locks)
}

func (k *keyLocks) inspect(key string) (*keyLock, int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	entry := k.locks[key]
	if entry == nil {
		return nil, 0
	}
	return entry, entry.refs
}
