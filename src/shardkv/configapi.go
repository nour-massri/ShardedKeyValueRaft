package shardkv

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
)
func Make_config(n int, unreliable bool, maxraftstate int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// controller and its clerk
	cfg.ctl = cfg.StartCtrlerService()

	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)
	cfg.n = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*ShardKV, cfg.n)
		gg.saved = make([]*raft.Persister, cfg.n)
		gg.endnames = make([][]string, cfg.n)
		gg.mendnames = make([][]string, cfg.ctl.n)
		for i := 0; i < cfg.n; i++ {
			cfg.StartServer(gi, i)
		}
	}

	cfg.clerks = make(map[*Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid

	cfg.net.Reliable(!unreliable)

	return cfg
}

// tell the shardctrler that a group is joining.
func (cfg *config) Join(gi int) {
	cfg.joinm([]int{gi}, cfg.ctl)
}

func (cfg *config) MakeClient() *Clerk {
	return cfg.makeClient(cfg.ctl)
}