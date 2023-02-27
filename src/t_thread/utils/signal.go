package utils

import "sync"
/*
This signal with no lock except update
*/
type Signal struct {
	size int
	count int
	lock *sync.Mutex
}

func NewSignal(size int) *Signal {
	return &Signal{size, 0, &sync.Mutex{}}
}

func (s *Signal) Add() {
	
	for true {
		if s.count < s.size {
			s.lock.Lock()
			if s.count < s.size {
				s.count ++
				s.lock.Unlock()
				return
			} else {
				s.lock.Unlock()
				continue
			}
			
		}
	}
}

func (s *Signal) Release() {
	
	for true {
		if s.count > 0 {
			s.lock.Lock()
			if s.count > 0 {
				s.count --
				s.lock.Unlock()
				return
			} else {
				s.lock.Unlock()
				continue
			}
		}
	}
}



