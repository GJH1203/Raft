package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	clientSeqNums map[int64]int64 // last sequence number processed for each client  
	pending map[int]chan Op       // pending requests indexed by Raft log index
}


type Op struct {
	Operation string // "Join", "Leave", "Move", "Query"
	
	// Join fields
	Servers map[int][]string
	
	// Leave fields
	GIDs []int
	
	// Move fields
	Shard int
	GID   int
	
	// Query fields
	Num int
	
	// Common fields for duplicate detection
	ClientId int64
	SeqNum   int64
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sm.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if already processed (duplicate detection)
	if seqNum, exists := sm.clientSeqNums[args.ClientId]; exists && seqNum >= args.SeqNum {
		sm.mu.Unlock()
		return
	}
	
	// Check if we're the leader
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	sm.pending[index] = ch
	sm.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		sm.mu.Lock()
		delete(sm.pending, index)
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			// Operation successfully applied
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		sm.mu.Unlock()
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if already processed (duplicate detection)
	if seqNum, exists := sm.clientSeqNums[args.ClientId]; exists && seqNum >= args.SeqNum {
		sm.mu.Unlock()
		return
	}
	
	// Check if we're the leader
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: "Leave",
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	sm.pending[index] = ch
	sm.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		sm.mu.Lock()
		delete(sm.pending, index)
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			// Operation successfully applied
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		sm.mu.Unlock()
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if already processed (duplicate detection)
	if seqNum, exists := sm.clientSeqNums[args.ClientId]; exists && seqNum >= args.SeqNum {
		sm.mu.Unlock()
		return
	}
	
	// Check if we're the leader
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: "Move",
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	sm.pending[index] = ch
	sm.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		sm.mu.Lock()
		delete(sm.pending, index)
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			// Operation successfully applied
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		sm.mu.Unlock()
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if we're the leader
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: "Query",
		Num:       args.Num,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	sm.pending[index] = ch
	sm.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		sm.mu.Lock()
		delete(sm.pending, index)
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			// Get the requested configuration
			configNum := args.Num
			if configNum == -1 || configNum >= len(sm.configs) {
				configNum = len(sm.configs) - 1
			}
			if configNum >= 0 && configNum < len(sm.configs) {
				reply.Config = sm.configs[configNum]
			}
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		sm.mu.Unlock()
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		sm.mu.Unlock()
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.clientSeqNums = make(map[int64]int64)
	sm.pending = make(map[int]chan Op)

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Start background goroutine to process applied messages
	go sm.processAppliedMessages()

	return sm
}

// Background goroutine to process messages from Raft
func (sm *ShardMaster) processAppliedMessages() {
	for msg := range sm.applyCh {
		if msg.UseSnapshot {
			// Handle snapshots if needed
		} else {
			// Handle regular log entries
			op := msg.Command.(Op)
			sm.mu.Lock()
			
			// Check if this operation was already processed (duplicate detection)
			if seqNum, exists := sm.clientSeqNums[op.ClientId]; !exists || seqNum < op.SeqNum {
				// Apply the operation
				switch op.Operation {
				case "Join":
					sm.applyJoin(op.Servers)
				case "Leave":
					sm.applyLeave(op.GIDs)
				case "Move":
					sm.applyMove(op.Shard, op.GID)
				case "Query":
					// Query doesn't change state, just need to ensure linearizability
				}
				sm.clientSeqNums[op.ClientId] = op.SeqNum
			}
			
			// Notify waiting RPC handler if any
			if ch, exists := sm.pending[msg.Index]; exists {
				select {
				case ch <- op:
				default:
				}
			}
			
			sm.mu.Unlock()
		}
	}
}

// Apply Join operation - add new groups and rebalance shards
func (sm *ShardMaster) applyJoin(servers map[int][]string) {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	
	// Copy existing groups
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	
	// Add new groups
	for gid, servers := range servers {
		newConfig.Groups[gid] = servers
	}
	
	// Rebalance shards
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

// Apply Leave operation - remove groups and redistribute their shards
func (sm *ShardMaster) applyLeave(gids []int) {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	
	// Copy groups except the ones leaving
	leavingGids := make(map[int]bool)
	for _, gid := range gids {
		leavingGids[gid] = true
	}
	
	for gid, servers := range lastConfig.Groups {
		if !leavingGids[gid] {
			newConfig.Groups[gid] = servers
		}
	}
	
	// Rebalance shards
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

// Apply Move operation - move a specific shard to a specific group
func (sm *ShardMaster) applyMove(shard int, gid int) {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	
	// Copy groups
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	
	// Move the shard
	newConfig.Shards[shard] = gid
	sm.configs = append(sm.configs, newConfig)
}

// Rebalance shards among groups to distribute load evenly
func (sm *ShardMaster) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		// No groups, assign all shards to group 0 (invalid)
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	
	// Get all group IDs
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	
	// Sort for deterministic behavior
	for i := 0; i < len(gids); i++ {
		for j := i + 1; j < len(gids); j++ {
			if gids[i] > gids[j] {
				gids[i], gids[j] = gids[j], gids[i]
			}
		}
	}
	
	// Calculate target shards per group
	nGroups := len(gids)
	shardsPerGroup := NShards / nGroups
	extraShards := NShards % nGroups
	
	// Count current shards per group
	shardCounts := make(map[int]int)
	for _, gid := range gids {
		shardCounts[gid] = 0
	}
	
	for _, gid := range config.Shards {
		if gid != 0 {
			shardCounts[gid]++
		}
	}
	
	// Find shards that need to be reassigned
	toReassign := make([]int, 0)
	
	for shard, gid := range config.Shards {
		// If shard is assigned to a group that doesn't exist, reassign
		if gid == 0 || config.Groups[gid] == nil {
			toReassign = append(toReassign, shard)
			config.Shards[shard] = 0
		} else {
			// Calculate target for this group
			target := shardsPerGroup
			if extraShards > 0 && gid <= gids[extraShards-1] {
				target++
			}
			
			// If group has too many shards, reassign excess
			if shardCounts[gid] > target {
				toReassign = append(toReassign, shard)
				config.Shards[shard] = 0
				shardCounts[gid]--
			}
		}
	}
	
	// Reassign shards to groups that need them
	for _, shard := range toReassign {
		for _, gid := range gids {
			target := shardsPerGroup
			if extraShards > 0 && gid <= gids[extraShards-1] {
				target++
			}
			
			if shardCounts[gid] < target {
				config.Shards[shard] = gid
				shardCounts[gid]++
				break
			}
		}
	}
}
