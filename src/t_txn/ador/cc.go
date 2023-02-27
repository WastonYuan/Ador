package ador

import (
	"sync"
	// "fmt"
)

type CC struct {
	Step int // all thread reach this step
	Signal int
	ThdCnt int
	Mutex *sync.RWMutex
}

func NewCC() (*CC) {
	return &CC{0, 0, 0, &sync.RWMutex{}}
}



func (cc *CC) Start(cnt int) {
	cc.Mutex.Lock()
	defer cc.Mutex.Unlock()
	cc.ThdCnt = cnt
	cc.Signal = cnt
}



func (cc *CC) IsGoOn(t *Thread) bool {
	cc.Mutex.Lock()
	defer cc.Mutex.Unlock()
	if cc.Step == t.statics.step {
		cc.Signal --
		t.statics.step = t.statics.step + 1
		if cc.Signal == 0 {
			cc.Signal = cc.ThdCnt
			cc.Step ++
		}
		// fmt.Println(cc.Step, t.statics.step, cc.Signal, cc.ThdCnt)
		return false
	}
	
	return true
}

// sync only after all thread start
func (cc *CC) Sync(t *Thread) {
	for cc.IsGoOn(t) {}
}


func (cc *CC) Exit(t *Thread) {
	cc.Mutex.Lock()
	defer cc.Mutex.Unlock()
	cc.ThdCnt --
	if cc.Step == t.statics.step {
		cc.Signal --
		if cc.Signal == 0 {
			cc.Signal = cc.ThdCnt
			cc.Step ++
		}
	}
}