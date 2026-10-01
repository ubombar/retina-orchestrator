// Tests provide 100% coverage of queue.go.
package structures

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- NewQueue ---

func TestNewQueue_Valid(t *testing.T) {
	t.Parallel()
	q, err := NewQueue[int](10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q == nil {
		t.Fatal("expected non-nil Queue")
	}
}

func TestNewQueue_ZeroBufferSize(t *testing.T) {
	t.Parallel()
	// Zero buffer size creates an unbuffered channel — TryPush always fails immediately.
	q, err := NewQueue[int](0)
	if err != nil {
		t.Fatalf("expected not to get an error")
	}
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	if err := q.TryPush("a", &v); err == nil {
		t.Fatal("expected TryPush to fail on unbuffered channel")
	}
}

func TestNewQueue_NegativeBufferSize(t *testing.T) {
	t.Parallel()
	_, err := NewQueue[int](-1)
	if err == nil {
		t.Fatal("expected error for negative buffer size")
	}
}

// --- NewConsumer ---

func TestNewConsumer_Valid(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cons == nil {
		t.Fatal("expected non-nil consumer")
	}
	cons.Close()
}

func TestNewConsumer_DuplicateID(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	_, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("unexpected error on first consumer: %v", err)
	}
	_, err = q.NewConsumer("a")
	if err == nil {
		t.Fatal("expected error on duplicate id")
	}
}

func TestNewConsumer_DuplicateID_AfterClose(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	_, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("expected to reuse id after close, got error: %v", err)
	}
}

func TestNewConsumer_MultipleDistinctIDs(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	c1, err1 := q.NewConsumer("a")
	c2, err2 := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if c1 == c2 {
		t.Fatal("expected distinct consumers")
	}
}

// --- TryPush / Pop ---

func TestQueueTryPushPop_SingleConsumer(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 42
	if err := q.TryPush("a", &v); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}

	got, err := cons.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected pop error: %v", err)
	}
	if *got != 42 {
		t.Fatalf("expected 42, got %d", *got)
	}
}

func TestQueueTryPushPop_Order(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](8)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	for i := range 5 {
		v := i
		if err := q.TryPush("a", &v); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	for i := range 5 {
		got, err := cons.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected pop error: %v", err)
		}
		if *got != i {
			t.Fatalf("expected %d, got %d", i, *got)
		}
	}
}

func TestQueueTryPushPop_MultipleConsumers_Independent(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	c1, _ := q.NewConsumer("a")
	c2, _ := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	v1, v2 := 1, 2
	_ = q.TryPush("a", &v1)
	_ = q.TryPush("b", &v2)

	got1, err := c1.Pop(context.Background())
	if err != nil || *got1 != 1 {
		t.Fatalf("c1: expected 1, got %v, err %v", got1, err)
	}

	got2, err := c2.Pop(context.Background())
	if err != nil || *got2 != 2 {
		t.Fatalf("c2: expected 2, got %v, err %v", got2, err)
	}
}

func TestQueuePop_BlocksUntilTryPush(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 7
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = q.TryPush("a", &v)
	}()

	got, err := cons.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != 7 {
		t.Fatalf("expected 7, got %d", *got)
	}
}

func TestTryPush_BufferFull(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](2)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	_ = q.TryPush("a", &v)
	_ = q.TryPush("a", &v)

	if err := q.TryPush("a", &v); err == nil {
		t.Fatal("expected error when pushing to full buffer")
	}
}

func TestTryPush_DoesNotDeliverToOtherConsumers(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	c1, _ := q.NewConsumer("a")
	c2, _ := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	v := 42
	_ = q.TryPush("a", &v)

	got, err := c1.Pop(context.Background())
	if err != nil || *got != 42 {
		t.Fatalf("c1: expected 42, got %v, err %v", got, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = c2.Pop(ctx)
	if err == nil {
		t.Fatal("c2: expected timeout, got an element")
	}
}

// --- TryPush to unregistered / closed consumer ---

func TestTryPush_UnknownID(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)

	v := 1
	if err := q.TryPush("nonexistent", &v); err == nil {
		t.Fatal("expected error when pushing to unknown id")
	}
}

func TestTryPush_AfterClose(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	v := 1
	if err := q.TryPush("a", &v); err == nil {
		t.Fatal("expected error pushing to closed consumer")
	}
}

// --- Context cancellation ---

func TestQueuePop_ContextCancelledBeforeCall(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestQueuePop_ContextCancelledWhileBlocking(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
}

func TestQueuePop_ContextTimeout(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// --- Close ---

func TestQueueClose_Idempotent(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")

	cons.Close()
	cons.Close()
}

func TestQueueClose_PopAfterClose(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	_, err := cons.Pop(context.Background())
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestClose_UnregistersFromQueue(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	q.mu.Lock()
	_, stillRegistered := q.consumers["a"]
	q.mu.Unlock()

	if stillRegistered {
		t.Fatal("expected consumer to be removed from queue after Close")
	}
}

func TestClose_BufferedItemsStillReadableAfterClose(t *testing.T) {
	t.Parallel()
	// Buffered items pushed before Close are still readable after Close.
	// Once the buffer is drained, Pop returns an error.
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")

	v := 1
	_ = q.TryPush("a", &v)
	cons.Close()

	got, err := cons.Pop(context.Background())
	if err != nil {
		t.Fatalf("expected buffered item to be readable after close, got error: %v", err)
	}
	if *got != 1 {
		t.Fatalf("expected 1, got %d", *got)
	}

	_, err = cons.Pop(context.Background())
	if err == nil {
		t.Fatal("expected error after buffer drained and consumer closed")
	}
}

// --- Concurrency ---

func TestQueueConcurrent_TryPushPop(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](128)
	cons, _ := q.NewConsumer("a")
	const n = 100

	var wg sync.WaitGroup
	results := make([]int, 0, n)
	var mu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cons.Close()
		for range n {
			v, err := cons.Pop(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			results = append(results, *v)
			mu.Unlock()
		}
	}()

	for i := range n {
		v := i
		if err := q.TryPush("a", &v); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	wg.Wait()

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestConcurrent_MultipleConsumers(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](128)
	const n = 100
	ids := []string{"a", "b", "c"}

	consumers := make([]*consumer[int], len(ids))
	for i, id := range ids {
		cons, err := q.NewConsumer(id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consumers[i] = cons
	}

	var wg sync.WaitGroup
	for i, cons := range consumers {
		id := ids[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cons.Close()
			for range n {
				_, err := cons.Pop(context.Background())
				if err != nil {
					t.Errorf("[%s] unexpected error: %v", id, err)
					return
				}
			}
		}()
	}

	for i := range n {
		v := i
		for _, id := range ids {
			if err := q.TryPush(id, &v); err != nil {
				t.Fatalf("unexpected push error: %v", err)
			}
		}
	}

	wg.Wait()
}

func TestConcurrent_CloseAndTryPush(t *testing.T) {
	t.Parallel()
	// Race between Close and TryPush — TryPush should return an error, not panic.
	for range 100 {
		q, _ := NewQueue[int](4)
		cons, _ := q.NewConsumer("a")

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			v := 1
			_ = q.TryPush("a", &v)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			cons.Close()
		}()

		wg.Wait()
	}
}

func TestConcurrent_CloseAndPop(t *testing.T) {
	t.Parallel()
	// Race between Close and Pop — Pop should return an error, not panic.
	for range 100 {
		q, _ := NewQueue[int](4)
		cons, _ := q.NewConsumer("a")

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cons.Pop(context.Background())
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			cons.Close()
		}()

		wg.Wait()
	}
}

func TestConcurrent_ManyConsumers(t *testing.T) {
	t.Parallel()
	const numConsumers = 100
	const n = 50
	q, _ := NewQueue[int](n) // buffer sized to n so TryPush never fails
	consumers := make([]*consumer[int], numConsumers)
	for i := range numConsumers {
		cons, err := q.NewConsumer(fmt.Sprintf("consumer-%d", i))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consumers[i] = cons
	}

	var wg sync.WaitGroup
	for i, cons := range consumers {
		id := fmt.Sprintf("consumer-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cons.Close()
			for range n {
				_, err := cons.Pop(context.Background())
				if err != nil {
					t.Errorf("[%s] unexpected error: %v", id, err)
					return
				}
			}
		}()
	}

	for i := range n {
		v := i
		for j := range numConsumers {
			id := fmt.Sprintf("consumer-%d", j)
			if err := q.TryPush(id, &v); err != nil {
				t.Fatalf("unexpected push error: %v", err)
			}
		}
	}

	wg.Wait()
}

// --- Push ---

func TestPush_NotRegistered(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	v := 1
	if err := q.Push(context.Background(), "missing", &v); !errors.Is(err, ErrConsumerNotRegistered) {
		t.Fatalf("expected ErrConsumerNotRegistered, got %v", err)
	}
}

func TestPush_RoomAvailable(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 7
	if err := q.Push(context.Background(), "a", &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := cons.Pop(context.Background())
	if err != nil || *got != 7 {
		t.Fatalf("expected 7, got %v (err %v)", got, err)
	}
}

func TestPush_BlocksUntilPop(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	first, second := 1, 2
	if err := q.TryPush("a", &first); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := q.TryPush("a", &second); !errors.Is(err, ErrConsumerBufferFull) {
		t.Fatalf("expected ErrConsumerBufferFull, got %v", err)
	}

	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(context.Background(), "a", &second) }()

	select {
	case err := <-pushed:
		t.Fatalf("Push returned on a full buffer: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if got, _ := cons.Pop(context.Background()); *got != 1 {
		t.Fatalf("expected 1, got %d", *got)
	}
	select {
	case err := <-pushed:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push did not return after room was made")
	}
	if got, _ := cons.Pop(context.Background()); *got != 2 {
		t.Fatalf("expected 2, got %d", *got)
	}
}

func TestPush_UnblocksOnClose(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")

	v := 1
	_ = q.TryPush("a", &v)

	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(context.Background(), "a", &v) }()
	time.Sleep(20 * time.Millisecond)

	// Close from a goroutine other than the one that owns the consumer.
	go cons.Close()

	select {
	case err := <-pushed:
		if !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("expected ErrConsumerClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push did not return after Close")
	}
	if err := q.Push(context.Background(), "a", &v); !errors.Is(err, ErrConsumerNotRegistered) {
		t.Fatalf("expected ErrConsumerNotRegistered after Close, got %v", err)
	}
}

func TestPush_ContextTimeout(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	_ = q.TryPush("a", &v)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := q.Push(ctx, "a", &v); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestConsumer_ConcurrentClose(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(cons.Close)
	}
	wg.Wait()

	// The id is free again and the old consumer's Close must not remove the
	// new registration.
	replacement, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer replacement.Close()
	cons.Close()
	v := 1
	if err := q.TryPush("a", &v); err != nil {
		t.Fatalf("replacement consumer was unregistered: %v", err)
	}
}

func TestConsumer_Discard(t *testing.T) {
	t.Parallel()
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")

	v := 1
	for range 3 {
		_ = q.TryPush("a", &v)
	}
	cons.Close()
	if n := cons.Discard(); n != 3 {
		t.Fatalf("expected 3 discarded, got %d", n)
	}
	if n := cons.Discard(); n != 0 {
		t.Fatalf("expected 0 discarded, got %d", n)
	}
}
