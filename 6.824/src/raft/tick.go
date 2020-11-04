package raft

import "time"
import "sync/atomic"
import "math/rand"

type atomicValue struct {
	value int32
}

func (at *atomicValue) get() int {
	return int(atomic.LoadInt32(&at.value))
}

func (at *atomicValue) set(v int) {
	atomic.StoreInt32(&at.value, int32(v))
}

func (at *atomicValue) add() {
	atomic.AddInt32(&at.value, 1)
}

type ticker struct {
	now atomicValue
	stopCh chan struct{}
	startCh chan struct{}
	stoped bool
}

func (t *ticker) beginTick(baseTimeout, bias int, handler func (), before bool) {
	for {
		timeout := baseTimeout
		
		if bias > 0 {
			timeout += rand.Intn(bias)
		}
		
		for t.now.set(0); t.now.get() < timeout; t.now.add() {
			select {
				case <- t.stopCh:
					<- t.startCh
					if before {
						handler()
					}
				default: // non-block
			}
			
			time.Sleep(time.Millisecond)
		}
		
		handler()
	}
}

func newTick(timeout int, bias int, handler func (), before bool) *ticker {
	t := &ticker{}
	t.stopCh = make(chan struct{}, 1)
	t.startCh = make(chan struct{})
	t.reset()
	t.stop()
	go t.beginTick(timeout, bias, handler, before)
	return t
}

func (t *ticker) stop() {
	if !t.stoped {
		t.stopCh <- struct{}{}
		t.stoped = true
	}
}

func (t *ticker) start() {
	if t.stoped {
		t.reset()
		t.startCh <- struct{}{}
		t.stoped = false
	}
}

func (t *ticker) reset() {
	t.now.set(0)
}
