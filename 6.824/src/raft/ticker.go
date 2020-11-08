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
	handler func()
	timeout int
	bias int
	stopCh chan struct{}
	startCh chan struct{}
	stoped bool
}

func (t *ticker) beginTick() {
	for {
		finalTimeout := t.timeout
		if t.bias > 0 {
			finalTimeout += rand.Intn(t.bias)
		}
		
		for t.now.set(0); t.now.get() < finalTimeout; t.now.add() {
			select {
				case <- t.stopCh:
					<- t.startCh
				default: // non-block
			}
			
			time.Sleep(time.Millisecond)
		}
		
		t.handler()
	}
}

func newTick(timeout int, bias int, handler func()) *ticker {
	t := &ticker{}
	t.stopCh = make(chan struct{}, 1)
	t.startCh = make(chan struct{})
	t.handler = handler
	t.timeout = timeout
	t.bias = bias
	t.stop()
	go t.beginTick()
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
		t.startCh <- struct{}{}
		t.stoped = false
	}
}

func (t *ticker) reset() {
	t.now.set(0)
}

func (t *ticker) trigger() {
	t.now.set(t.timeout + t.bias)
	t.start()
}