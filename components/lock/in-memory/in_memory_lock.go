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
	"fmt"
	"sync"
	"time"

	"mosn.io/layotto/components/lock"
	"mosn.io/layotto/components/pkg/actuators"
)

const (
	componentName = "lock-memory"
)

var (
	once               sync.Once
	readinessIndicator *actuators.HealthIndicator
	livenessIndicator  *actuators.HealthIndicator
)

func init() {
	readinessIndicator = actuators.NewHealthIndicator()
	livenessIndicator = actuators.NewHealthIndicator()
}

type InMemoryLock struct {
	features []lock.Feature
	data     *lockMap
	// now is the time source used for every lease calculation. It is
	// time.Now in production and can be replaced in tests so that lease
	// expiry is deterministic. It is never nil after construction.
	now func() time.Time
}

// memoryLock is a lock holder
type memoryLock struct {
	owner      string
	expireTime time.Time
}

type lockMap struct {
	sync.Mutex
	locks map[string]*memoryLock
}

func NewInMemoryLock() *InMemoryLock {
	return newInMemoryLockWithTimeSource(time.Now)
}

// newInMemoryLockWithTimeSource builds a lock store driven by the given time
// source. NewInMemoryLock passes time.Now; tests pass a fake clock so that
// lease expiry can be exercised without sleeping. Keeping this constructor
// unexported leaves NewInMemoryLock's signature unchanged.
func newInMemoryLockWithTimeSource(now func() time.Time) *InMemoryLock {
	once.Do(func() {
		indicators := &actuators.ComponentsIndicator{ReadinessIndicator: readinessIndicator, LivenessIndicator: livenessIndicator}
		actuators.SetComponentsIndicator(componentName, indicators)
	})
	if now == nil {
		now = time.Now
	}
	return &InMemoryLock{
		features: make([]lock.Feature, 0),
		data: &lockMap{
			locks: make(map[string]*memoryLock),
		},
		now: now,
	}
}

// isExpired reports whether the lease of item has run out at now. A lease is
// expired once the clock reaches its deadline, so the deadline itself already
// belongs to the expired side.
func isExpired(item *memoryLock, now time.Time) bool {
	return !item.expireTime.After(now)
}

// purgeStaleLockLocked removes the entry for resourceId when it no longer
// protects anything: either its lease has expired, or it has no owner because
// the holder unlocked it. Such entries are indistinguishable from an absent
// lock, so dropping them on access keeps lockMap from growing forever without
// a background goroutine. Callers must hold s.data.
func (s *InMemoryLock) purgeStaleLockLocked(resourceId string, now time.Time) {
	item, ok := s.data.locks[resourceId]
	if !ok {
		return
	}
	if item.owner == "" || isExpired(item, now) {
		delete(s.data.locks, resourceId)
	}
}

func (s *InMemoryLock) Init(_ lock.Metadata) error {
	readinessIndicator.SetStarted()
	livenessIndicator.SetStarted()
	return nil
}

// LockKeepAlive try to renewal lease
//
// The lease is recomputed from the current time instead of being extended from
// the previous deadline: the new deadline is now+Expire. Extending the old
// deadline would let a client that calls LockKeepAlive repeatedly push the
// lease arbitrarily far into the future, which defeats the purpose of a
// bounded lease and delays failure detection of a dead holder.
func (s *InMemoryLock) LockKeepAlive(ctx context.Context, request *lock.LockKeepAliveRequest) (*lock.LockKeepAliveResponse, error) {
	if request == nil {
		return &lock.LockKeepAliveResponse{
			Status: lock.INTERNAL_ERROR,
		}, fmt.Errorf("[inMemoryLock]: nil LockKeepAlive request")
	}
	if request.Expire <= 0 {
		// Invalid arguments are reported as an error, consistent with the
		// validation TryLock performs in pkg/grpc/default_api/api_lock.go.
		// INTERNAL_ERROR is set as well so that a caller which ignores the
		// error still does not read the response as a successful renewal.
		return &lock.LockKeepAliveResponse{
			Status: lock.INTERNAL_ERROR,
		}, fmt.Errorf("[inMemoryLock]: expire must be positive.ResourceId: %s", request.ResourceId)
	}

	s.data.Lock()
	defer s.data.Unlock()
	now := s.now()
	// 1. An expired lease no longer protects the resource, so drop it before
	// looking it up. The caller then observes the same LOCK_UNEXIST it would
	// get for a resource that was never locked.
	s.purgeStaleLockLocked(request.ResourceId, now)
	item, ok := s.data.locks[request.ResourceId]
	if !ok {
		return &lock.LockKeepAliveResponse{
			ResourceId: request.ResourceId,
			Status:     lock.LOCK_UNEXIST,
		}, nil
	}
	// 2. Only the current holder may renew the lease.
	if item.owner != request.LockOwner {
		return &lock.LockKeepAliveResponse{
			ResourceId: request.ResourceId,
			Status:     lock.LOCK_BELONG_TO_OTHERS,
		}, nil
	}
	// 3. Renew from now.
	item.expireTime = now.Add(time.Second * time.Duration(request.Expire))
	return &lock.LockKeepAliveResponse{
		ResourceId: request.ResourceId,
		Status:     lock.SUCCESS,
	}, nil
}

func (s *InMemoryLock) Features() []lock.Feature {
	return s.features
}

// Try to add a lock. Currently this is a non-reentrant lock
func (s *InMemoryLock) TryLock(ctx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	s.data.Lock()
	defer s.data.Unlock()
	now := s.now()

	// 1. Drop the entry when its lease has expired: it must not block a new
	// holder, and it must not stay in lockMap forever.
	s.purgeStaleLockLocked(req.ResourceId, now)

	// 2. Check if it has been locked by others.
	// Currently this is a non-reentrant lock
	if _, ok := s.data.locks[req.ResourceId]; ok {
		//lock failed
		return &lock.TryLockResponse{
			Success: false,
		}, nil
	}

	// 3. Update owner information
	s.data.locks[req.ResourceId] = &memoryLock{
		owner:      req.LockOwner,
		expireTime: now.Add(time.Second * time.Duration(req.Expire)),
	}

	return &lock.TryLockResponse{
		Success: true,
	}, nil
}

func (s *InMemoryLock) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	s.data.Lock()
	defer s.data.Unlock()
	now := s.now()

	// 1. Drop the entry if it is gone or expired. An expired lease can already
	// be taken by another owner, so the old holder must not be told SUCCESS.
	s.purgeStaleLockLocked(req.ResourceId, now)

	// 2. Find the memoryLock for this resourceId
	item, ok := s.data.locks[req.ResourceId]
	if !ok {
		return &lock.UnlockResponse{
			Status: lock.LOCK_UNEXIST,
		}, nil
	}
	// 3. check the owner information
	if item.owner != req.LockOwner {
		return &lock.UnlockResponse{
			Status: lock.LOCK_BELONG_TO_OTHERS,
		}, nil
	}
	// 4. release the lock and remove the entry
	delete(s.data.locks, req.ResourceId)
	return &lock.UnlockResponse{
		Status: lock.SUCCESS,
	}, nil
}
