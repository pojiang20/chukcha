package heap

import "container/heap"

// T的类型是any
type Ordered[T any] interface {
	Less(v T) bool
}

// T的类型是Ordered[T]
type underlying[T Ordered[T]] []T
type Min[T Ordered[T]] struct {
	h *underlying[T]
}

func NewMin[T Ordered[T]]() Min[T] {
	h := &underlying[T]{}
	heap.Init(h)
	return Min[T]{h: h}
}

func (h *Min[T]) Push(x T) {
	heap.Push(h.h, x)
}

func (h *Min[T]) Pop() T {
	return heap.Pop(h.h).(T)
}

func (h *Min[T]) Len() int {
	return len(*h.h)
}

func (h underlying[T]) Len() int           { return len(h) }
func (h underlying[T]) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h underlying[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *underlying[T]) Push(x any) {
	*h = append(*h, x.(T))
}

// TODO 为什么要这么多步骤，而不是直接使用指针
func (h *underlying[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	//*h[len(*h)-1]
	return x
}
