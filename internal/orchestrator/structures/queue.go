// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package structures provides generic data structures for the orchestrator,
// including a per-consumer queue and a ring buffer for FIE streaming.
package structures

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrConsumerNotRegistered is returned when no consumer has the given id.
	ErrConsumerNotRegistered = errors.New("consumer not registered")
	// ErrConsumerBufferFull is returned by TryPush when the consumer's buffer
	// has no room.
	ErrConsumerBufferFull = errors.New("consumer buffer full")
	// ErrConsumerClosed is returned when the consumer is closed while an
	// operation is waiting on it.
	ErrConsumerClosed = errors.New("consumer closed")
)

// Pop and Discard must be called from the same goroutine. Close is safe to
// call from any goroutine.
type consumer[T any] struct {
	id        string
	ch        chan *T
	done      chan struct{}
	queue     *Queue[T]
	closeOnce sync.Once
}

// Pop returns the next element, blocking until one is available, the context
// is canceled, or the consumer is closed.
func (qc *consumer[T]) Pop(ctx context.Context) (*T, error) {
	// Drain any buffered item first before blocking.
	select {
	case item := <-qc.ch:
		return item, nil
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-qc.done:
		return nil, ErrConsumerClosed
	case item := <-qc.ch:
		return item, nil
	}
}

// Close unregisters the consumer and unblocks any Push waiting on it. Calling
// Close multiple times, from any goroutine, is a no-op.
func (qc *consumer[T]) Close() {
	qc.closeOnce.Do(func() {
		qc.queue.mu.Lock()
		defer qc.queue.mu.Unlock()

		if c, ok := qc.queue.consumers[qc.id]; ok && c == qc {
			delete(qc.queue.consumers, qc.id)
		}
		close(qc.done)
	})
}

// Discard removes everything still buffered and returns how many elements
// were removed. It is meant to be called after Close, to account for elements
// that were pushed but never popped.
func (qc *consumer[T]) Discard() int {
	n := 0
	for {
		select {
		case <-qc.ch:
			n++
		default:
			return n
		}
	}
}

// Queue delivers elements to individual named consumers.
// Each consumer has its own buffered channel and receives only elements
// pushed directly to it by ID.
type Queue[T any] struct {
	mu         sync.Mutex
	consumers  map[string]*consumer[T]
	bufferSize int
}

// NewQueue creates a new Queue with the given per-consumer buffer size.
func NewQueue[T any](bufferSize int) (*Queue[T], error) {
	if bufferSize < 0 {
		return nil, fmt.Errorf("buffer size cannot be negative: got %d", bufferSize)
	}

	return &Queue[T]{
		consumers:  make(map[string]*consumer[T]),
		bufferSize: bufferSize,
	}, nil
}

// Returns an error if a consumer with the given id already exists.
func (q *Queue[T]) NewConsumer(id string) (*consumer[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.consumers[id]; ok {
		return nil, fmt.Errorf("consumer %q already exists", id)
	}

	c := &consumer[T]{
		id:    id,
		ch:    make(chan *T, q.bufferSize),
		done:  make(chan struct{}),
		queue: q,
	}
	q.consumers[id] = c
	return c, nil
}

// TryPush attempts to send an element to a specific consumer without blocking.
// Returns ErrConsumerNotRegistered if the consumer is not registered and
// ErrConsumerBufferFull if its buffer is full.
func (q *Queue[T]) TryPush(id string, item *T) error {
	// The send happens under the mutex so that it cannot interleave with
	// Close: an element is never accepted for a consumer that is already gone.
	q.mu.Lock()
	defer q.mu.Unlock()

	consumer, ok := q.consumers[id]
	if !ok {
		return ErrConsumerNotRegistered
	}
	select {
	case consumer.ch <- item:
		return nil
	default:
		return ErrConsumerBufferFull
	}
}

// Push sends an element to a specific consumer, waiting for room if its buffer
// is full. It returns ErrConsumerNotRegistered immediately if the consumer is
// not registered, ErrConsumerClosed if the consumer is closed while waiting,
// and the context error if ctx ends first.
func (q *Queue[T]) Push(ctx context.Context, id string, item *T) error {
	q.mu.Lock()
	consumer, ok := q.consumers[id]
	if !ok {
		q.mu.Unlock()
		return ErrConsumerNotRegistered
	}
	select {
	case consumer.ch <- item:
		q.mu.Unlock()
		return nil
	default:
	}
	q.mu.Unlock()

	select {
	case consumer.ch <- item:
		return nil
	case <-consumer.done:
		return ErrConsumerClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
