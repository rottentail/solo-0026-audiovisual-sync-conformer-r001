package storage

import "sync"

// keyLocks owns the lifecycle of per-identity mutual exclusion.
//
// Callers borrow an identity lock through do instead of retaining a mutex.
// Every entry is reference counted: a caller owns one reference from the
// moment it acquires the entry until it releases it, including the time it
// spends waiting on the entry's mutex. The last release removes the entry, so
// identities that finish their workflow do not leave lock objects behind when
// the repository has processed many takes, analyses, or conforms.
//
// Cleanup only removes an entry at zero references. A concurrent caller that
// is already waiting for the same identity has taken its reference before it
// blocks, so the entry cannot be replaced by a second, unrelated mutex while
// that waiter is pending.
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyLocks() *keyLocks {
	return &keyLocks{locks: make(map[string]*keyLock)}
}

// do runs action while holding the exclusive lock for key and returns the
// action's error. The lock entry is allocated on demand and retired when the
// last holder or waiter has released it.
func (k *keyLocks) do(key string, action func() error) error {
	entry := k.acquire(key)
	entry.mu.Lock()
	defer func() {
		entry.mu.Unlock()
		k.release(key)
	}()
	return action()
}

// acquire returns the entry for key and takes one reference for the caller.
// The reference is held until release, so an entry that still has pending
// waiters is never dropped from the table.
func (k *keyLocks) acquire(key string) *keyLock {
	k.mu.Lock()
	defer k.mu.Unlock()
	entry := k.locks[key]
	if entry == nil {
		entry = &keyLock{}
		k.locks[key] = entry
	}
	entry.refs++
	return entry
}

// release drops the caller's reference and removes the entry once nobody
// holds or waits for it anymore. It must be called after the entry's mutex is
// unlocked so a waiter that is about to make progress still keeps the entry
// alive through its own reference.
func (k *keyLocks) release(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	entry := k.locks[key]
	if entry == nil {
		return
	}
	entry.refs--
	if entry.refs == 0 {
		delete(k.locks, key)
	}
}
