package orchestrator

type scheduledEntry struct {
	at   int64
	pdid uint64
}
type schedulerHeap []scheduledEntry

func (heap *schedulerHeap) push(entry scheduledEntry) {
	*heap = append(*heap, entry)
	items := *heap
	for index := len(items) - 1; index > 0; {
		parent := (index - 1) / 2
		if items[parent].at <= items[index].at {
			break
		}
		items[parent], items[index] = items[index], items[parent]
		index = parent
	}
}
func (heap schedulerHeap) reschedule(at int64) { heap[0].at = at; heap.siftDown() }
func (heap *schedulerHeap) removeRoot() {
	items := *heap
	last := len(items) - 1
	items[0] = items[last]
	*heap = items[:last]
	heap.siftDown()
}
func (heap schedulerHeap) siftDown() {
	for index := 0; ; {
		least := index
		if left := 2*index + 1; left < len(heap) && heap[left].at < heap[least].at {
			least = left
		}
		if right := 2*index + 2; right < len(heap) && heap[right].at < heap[least].at {
			least = right
		}
		if least == index {
			return
		}
		heap[index], heap[least] = heap[least], heap[index]
		index = least
	}
}
