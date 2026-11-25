/*
 * Copyright 2021 Layotto Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package in_memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"mosn.io/layotto/components/lock"
)

// fakeClock is a manually advanced clock. It is safe for concurrent use
// because the concurrency test reads it from several goroutines.
type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newTestLock builds an InMemoryLock driven by clock.
func newTestLock(clock *fakeClock) *InMemoryLock {
	return newInMemoryLockWithTimeSource(clock.Now)
}

func TestNew(t *testing.T) {
	s := NewInMemoryLock()
	assert.NotNil(t, s)
	assert.NotNil(t, s.now)
}

func TestInit(t *testing.T) {
	s := NewInMemoryLock()
	assert.NotNil(t, s)

	err := s.Init(lock.Metadata{})
	assert.NoError(t, err)
}

func TestFeatures(t *testing.T) {
	s := NewInMemoryLock()
	assert.NotNil(t, s)

	f := s.Features()
	assert.NotNil(t, f)
	assert.Equal(t, 0, len(f))
}

func TestTryLock(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)
	assert.NotNil(t, s)

	req := &lock.TryLockRequest{
		ResourceId: "key111",
		LockOwner:  "own",
		Expire:     3,
	}

	var err error
	var resp *lock.TryLockResponse
	resp, err = s.TryLock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, resp.Success)

	resp, err = s.TryLock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.False(t, resp.Success)

	req = &lock.TryLockRequest{
		ResourceId: "key112",
		LockOwner:  "own",
		Expire:     1,
	}

	resp, err = s.TryLock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, resp.Success)

	resp, err = s.TryLock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.False(t, resp.Success)

	// Once the lease deadline is reached the lock can be taken again.
	clock.Advance(time.Second)

	resp, err = s.TryLock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, resp.Success)
}

func TestUnLock(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)
	assert.NotNil(t, s)

	req := &lock.UnlockRequest{
		ResourceId: "key111",
		LockOwner:  "own",
	}

	var err error
	var resp *lock.UnlockResponse
	resp, err = s.Unlock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, lock.LOCK_UNEXIST, resp.Status)

	lockReq := &lock.TryLockRequest{
		ResourceId: "key111",
		LockOwner:  "own",
		Expire:     10,
	}

	var lockResp *lock.TryLockResponse
	lockResp, err = s.TryLock(context.TODO(), lockReq)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, lockResp.Success)

	resp, err = s.Unlock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, lock.SUCCESS, resp.Status)

	lockResp, err = s.TryLock(context.TODO(), lockReq)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, lockResp.Success)

	req.LockOwner = "1"

	resp, err = s.Unlock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, lock.LOCK_BELONG_TO_OTHERS, resp.Status)

	req.ResourceId = "11"
	lockReq.ResourceId = "11"
	req.LockOwner = "own1"
	lockReq.LockOwner = "own1"
	lockResp, err = s.TryLock(context.TODO(), lockReq)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.True(t, lockResp.Success)

	resp, err = s.Unlock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, lock.SUCCESS, resp.Status)

	resp, err = s.Unlock(context.TODO(), req)
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, lock.LOCK_UNEXIST, resp.Status)
}

func TestUnlockAfterExpiryReturnsLockUnexist(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	lockReq := &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3}
	resp, err := s.TryLock(context.TODO(), lockReq)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Still inside the lease: unlocking succeeds.
	clock.Advance(2 * time.Second)
	unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.SUCCESS, unlockResp.Status)

	// Re-acquire, then let the lease run out. The old holder must no longer be
	// able to unlock, because another owner may already hold the lock.
	resp, err = s.TryLock(context.TODO(), lockReq)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	clock.Advance(4 * time.Second)
	unlockResp, err = s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, unlockResp.Status)

	// Same instant: another owner can take the lock.
	other := &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-b", Expire: 3}
	resp, err = s.TryLock(context.TODO(), other)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestLockKeepAliveSuccess(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	clock.Advance(2 * time.Second)
	aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 5})
	assert.NoError(t, err)
	assert.NotNil(t, aliveResp)
	assert.Equal(t, "res", aliveResp.ResourceId)
	assert.Equal(t, lock.SUCCESS, aliveResp.Status)

	// The deadline is now t0+2s+5s = t0+7s.
	// At t0+3s (the original deadline) the lease is still held: renew succeeded.
	clock.Advance(time.Second)
	unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.SUCCESS, unlockResp.Status)

	// Re-acquire at t0+3s and renew at t0+4s for 5s: deadline is t0+9s.
	resp, err = s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	clock.Advance(time.Second)
	aliveResp, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 5})
	assert.NoError(t, err)
	assert.Equal(t, lock.SUCCESS, aliveResp.Status)

	// One second before the renewed deadline the lease is still valid. Probing
	// with a different owner does not touch the lease: a live lease reports
	// LOCK_BELONG_TO_OTHERS, an expired one reports LOCK_UNEXIST.
	clock.Advance(4 * time.Second)
	aliveResp, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-b", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_BELONG_TO_OTHERS, aliveResp.Status)

	// Renewal is computed from the current time, not stacked on the old
	// deadline: at the 5s mark after the renewal, the lease has expired.
	clock.Advance(time.Second)
	unlockResp, err = s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, unlockResp.Status)

	// Another owner can take it at the same instant.
	other := &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-b", Expire: 3}
	resp, err = s.TryLock(context.TODO(), other)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestLockKeepAliveStacksFromCurrentTime(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 10})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Renew five times, one second apart, each with Expire=2. If renewals were
	// stacked on the previous deadline the lease would keep growing; recomputing
	// from now means it is always 2s from the last renewal.
	for i := 0; i < 5; i++ {
		clock.Advance(time.Second)
		aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 2})
		assert.NoError(t, err)
		assert.Equal(t, lock.SUCCESS, aliveResp.Status)
	}

	// Last renewal at t0+5s, deadline t0+7s.
	clock.Advance(1500 * time.Millisecond)
	unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.SUCCESS, unlockResp.Status)

	resp, err = s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 2})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	clock.Advance(500 * time.Millisecond)
	_, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 2})
	assert.NoError(t, err)
	clock.Advance(2 * time.Second)
	unlockResp, err = s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, unlockResp.Status)
}

func TestLockKeepAliveUnexist(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	// Never locked.
	aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.NotNil(t, aliveResp)
	assert.Equal(t, lock.LOCK_UNEXIST, aliveResp.Status)

	// Locked then explicitly unlocked: the lease no longer exists.
	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
	assert.NoError(t, err)
	assert.Equal(t, lock.SUCCESS, unlockResp.Status)

	aliveResp, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, aliveResp.Status)

	// Locked and then expired: renewal at the expiry instant is rejected too.
	resp, err = s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	clock.Advance(3 * time.Second)
	aliveResp, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, aliveResp.Status)
}

func TestLockKeepAliveBelongToOthers(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-b", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_BELONG_TO_OTHERS, aliveResp.Status)
}

// TestLockKeepAliveAfterExpiryAndTakeover covers the required scenario: at the
// expiry instant renewal returns LOCK_UNEXIST, another owner takes the lock, and
// afterwards the original owner's renewal reports LOCK_BELONG_TO_OTHERS.
func TestLockKeepAliveAfterExpiryAndTakeover(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	// Expire is 3 seconds: expire at t0+3s.
	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// The 4th second.
	clock.Advance(4 * time.Second)

	aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_UNEXIST, aliveResp.Status)

	// The same instant: another owner can take the lock.
	other, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-b", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, other.Success)

	// The original owner no longer holds the lease.
	aliveResp, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, lock.LOCK_BELONG_TO_OTHERS, aliveResp.Status)
}

func TestLockKeepAliveInvalidExpire(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 3})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	for _, expire := range []int32{0, -1} {
		aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: expire})
		assert.Error(t, err)
		assert.NotNil(t, aliveResp)
		assert.Equal(t, lock.INTERNAL_ERROR, aliveResp.Status)
	}

	aliveResp, err := s.LockKeepAlive(context.TODO(), nil)
	assert.Error(t, err)
	assert.NotNil(t, aliveResp)
	assert.Equal(t, lock.INTERNAL_ERROR, aliveResp.Status)
}

// TestLockKeepAliveNilRequestDoesNotPanic guards the nil request path separately
// from the component/wire contract, which never passes nil.
func TestLockKeepAliveNilRequest(t *testing.T) {
	s := NewInMemoryLock()
	assert.NotPanics(t, func() {
		_, _ = s.LockKeepAlive(context.TODO(), nil)
	})
}

// TestExpiredEntriesArePurged verifies that expired and unlocked entries do not
// accumulate in lockMap, without any background goroutine.
func TestExpiredEntriesArePurged(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	for i, id := range []string{"a", "b", "c"} {
		_, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: id, LockOwner: "owner", Expire: int32(i + 1)})
		assert.NoError(t, err)
	}
	// "a" expires first: purge it via TryLock on the same key.
	clock.Advance(time.Second)
	_, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "a", LockOwner: "owner", Expire: 10})
	assert.NoError(t, err)
	// "a" is still live (re-acquired just now); "b" expires at t0+2s.
	clock.Advance(time.Second)
	_, err = s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "b", LockOwner: "owner"})
	assert.NoError(t, err)
	// Unlock removes "b" immediately.
	_, ok := s.data.locks["b"]
	assert.False(t, ok)

	// "a" and "c" are still present; after they expire, LockKeepAlive purges them.
	clock.Advance(10 * time.Second)
	_, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "a", LockOwner: "owner", Expire: 3})
	assert.NoError(t, err)
	_, err = s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "c", LockOwner: "owner", Expire: 3})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(s.data.locks))
}

// TestConcurrentTryLockKeepAliveUnlock drives two owners on the same resource
// through a synchronizing barrier (no sleeps): both line up, then race TryLock,
// renewal and Unlock. Whatever the interleaving, a losing TryLock never grants
// renewal rights and the store ends up consistent and empty.
func TestConcurrentTryLockKeepAliveUnlock(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	const iterations = 200
	for i := 0; i < iterations; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup

		for _, owner := range []string{"owner-a", "owner-b"} {
			owner := owner
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // barrier: both goroutines are ready

				lockResp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: owner, Expire: 60})
				assert.NoError(t, err)
				if lockResp.Success {
					aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: owner, Expire: 60})
					assert.NoError(t, err)
					assert.Equal(t, lock.SUCCESS, aliveResp.Status)

					unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: owner})
					assert.NoError(t, err)
					assert.Equal(t, lock.SUCCESS, unlockResp.Status)
				} else {
					// Losing the race must never grant renewal rights. By the
					// time renewal runs the winner may still hold the lease
					// (LOCK_BELONG_TO_OTHERS) or may already have released it
					// (LOCK_UNEXIST), but it can never be SUCCESS.
					aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: owner, Expire: 60})
					assert.NoError(t, err)
					assert.Contains(t, []lock.LockStatus{lock.LOCK_BELONG_TO_OTHERS, lock.LOCK_UNEXIST}, aliveResp.Status)
				}
			}()
		}
		close(start)
		wg.Wait()

		// Every acquirer unlocked before its goroutine returned, so nothing may
		// be left behind.
		assert.Equal(t, 0, len(s.data.locks))
	}
}

// TestConcurrentKeepAliveAndUnlockBarrier checks that renewal and unlock on the
// same owner are serialized: whichever runs first, the other observes a state
// consistent with that order and the lease never survives the unlock.
func TestConcurrentKeepAliveAndUnlockBarrier(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	s := newTestLock(clock)

	for i := 0; i < 200; i++ {
		resp, err := s.TryLock(context.TODO(), &lock.TryLockRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 60})
		assert.NoError(t, err)
		assert.True(t, resp.Success)

		var aliveStatus, unlockStatus int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			aliveResp, err := s.LockKeepAlive(context.TODO(), &lock.LockKeepAliveRequest{ResourceId: "res", LockOwner: "owner-a", Expire: 60})
			assert.NoError(t, err)
			atomic.StoreInt32(&aliveStatus, int32(aliveResp.Status))
		}()
		go func() {
			defer wg.Done()
			<-start
			unlockResp, err := s.Unlock(context.TODO(), &lock.UnlockRequest{ResourceId: "res", LockOwner: "owner-a"})
			assert.NoError(t, err)
			atomic.StoreInt32(&unlockStatus, int32(unlockResp.Status))
		}()
		close(start)
		wg.Wait()

		// Unlock always succeeds here (only owner-a ever holds the lock), while
		// renewal either renews before the unlock or sees LOCK_UNEXIST after it.
		assert.Equal(t, int32(lock.SUCCESS), atomic.LoadInt32(&unlockStatus))
		assert.Contains(t, []int32{int32(lock.SUCCESS), int32(lock.LOCK_UNEXIST)}, atomic.LoadInt32(&aliveStatus))
		// Either order leaves the entry removed.
		assert.Equal(t, 0, len(s.data.locks))
	}
}
