package redis

import "sync/atomic"

type CyclicCounter struct {
	ubound int32

	value int32
}

func newCyclicCounter(count int32) *CyclicCounter {
	return &CyclicCounter{
		ubound: count,
		value:  0,
	}
}

func (w *CyclicCounter) spin() (refreshed bool) {
	if w.ubound == 0 {
		return false
	}
	if w.ubound == 1 {
		return true
	}

	atomic.AddInt32(&w.value, 1)
	return atomic.CompareAndSwapInt32(&w.value, w.ubound, 0)
}

func (w *CyclicCounter) reset() {
	atomic.StoreInt32(&w.value, 0)
}
