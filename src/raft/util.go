package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func getRandtime(l int64, r int64)time.Duration{
	ms := l + (rand.Int63() % (r-l+1))
	return time.Duration(ms) * time.Millisecond
}
