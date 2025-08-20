package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"
import "bytes"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "Get", "Put", "Append", "Reconfigure", "MigrateShard"
	
	// Client request fields
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64
	
	// Reconfiguration fields
	Config    shardmaster.Config
	
	// Migration fields
	Shard        int
	Data         map[string]string
	ClientSeqNums map[int64]int64
	ClientResults map[int64]map[int64]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk
	config        shardmaster.Config
	lastConfig    shardmaster.Config
	
	// Key-value store per shard
	kvStore       map[string]string
	
	// Duplicate detection
	clientSeqNums map[int64]int64
	clientResults map[int64]map[int64]string
	
	// Request tracking
	pending       map[int]chan Op
	
	// Shard ownership tracking
	ownedShards   map[int]bool
	
	// Migration tracking
	pendingShards map[int]bool  // shards we're waiting to receive
	migrationData map[int]map[string]string  // data for shards we need to send
	
	// Snapshot state
	lastAppliedIndex int
	persister        *raft.Persister
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if we're responsible for this shard
	shard := key2shard(args.Key)
	if !kv.ownedShards[shard] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	// Check if already processed (duplicate detection)
	if seqNum, exists := kv.clientSeqNums[args.ClientId]; exists && seqNum >= args.SeqNum {
		if clientResults, ok := kv.clientResults[args.ClientId]; ok {
			if result, ok := clientResults[args.SeqNum]; ok {
				reply.Value = result
				kv.mu.Unlock()
				return
			}
		}
	}
	
	// Check if we're the leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	kv.pending[index] = ch
	kv.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		kv.mu.Lock()
		delete(kv.pending, index)
		
		// Check if we still own this shard
		if !kv.ownedShards[shard] {
			reply.Err = ErrWrongGroup
		} else if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			if clientResults, ok := kv.clientResults[args.ClientId]; ok {
				if result, ok := clientResults[args.SeqNum]; ok {
					reply.Value = result
				}
			}
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		kv.mu.Unlock()
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
	// Check if we're responsible for this shard
	shard := key2shard(args.Key)
	if !kv.ownedShards[shard] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	// Check if already processed (duplicate detection)
	if seqNum, exists := kv.clientSeqNums[args.ClientId]; exists && seqNum >= args.SeqNum {
		kv.mu.Unlock()
		return
	}
	
	// Check if we're the leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	kv.pending[index] = ch
	kv.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		kv.mu.Lock()
		delete(kv.pending, index)
		
		// Check if we still own this shard
		if !kv.ownedShards[shard] {
			reply.Err = ErrWrongGroup
		} else if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
			// Operation successfully applied
		} else {
			reply.WrongLeader = true // Leader changed, client should retry
		}
		kv.mu.Unlock()
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.pending, index)
		reply.WrongLeader = true // Timeout, let client retry
		kv.mu.Unlock()
	}
}

// MigrateShard RPC handler - receives shard data from another group
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	
	// Check if we're expecting this shard
	if !kv.pendingShards[args.Shard] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	// Check if we're the leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	// Create a migration operation for Raft
	op := Op{
		Operation:     "MigrateShard",
		Shard:         args.Shard,
		Data:          args.Data,
		ClientSeqNums: args.ClientSeqNums,
		ClientResults: args.ClientResults,
	}
	
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	// Wait for the operation to be applied
	ch := make(chan Op, 1)
	kv.pending[index] = ch
	kv.mu.Unlock()
	
	select {
	case appliedOp := <-ch:
		kv.mu.Lock()
		delete(kv.pending, index)
		if appliedOp.Operation == "MigrateShard" && appliedOp.Shard == args.Shard {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.pending, index)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister

	// Your initialization code here.
	kv.kvStore = make(map[string]string)
	kv.clientSeqNums = make(map[int64]int64)
	kv.clientResults = make(map[int64]map[int64]string)
	kv.pending = make(map[int]chan Op)
	kv.ownedShards = make(map[int]bool)
	kv.pendingShards = make(map[int]bool)
	kv.migrationData = make(map[int]map[string]string)
	kv.lastAppliedIndex = 0

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Initialize with current configuration from shardmaster
	kv.config = kv.mck.Query(-1)
	kv.lastConfig = shardmaster.Config{Num: 0, Groups: make(map[int][]string)}
	
	// Initialize shard ownership based on current config
	kv.updateShardOwnership()
	
	// Restore from snapshot if exists
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Start background goroutines
	go kv.processAppliedMessages()
	go kv.configMonitor()

	return kv
}

// Background goroutine to process messages from Raft
func (kv *ShardKV) processAppliedMessages() {
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			// Handle snapshots
			kv.mu.Lock()
			kv.decodeSnapshot(msg.Snapshot)
			kv.lastAppliedIndex = msg.Index
			kv.mu.Unlock()
		} else {
			// Handle regular log entries
			op := msg.Command.(Op)
			kv.mu.Lock()
			
			// Skip if this is an old message from before a snapshot
			if msg.Index <= kv.lastAppliedIndex {
				kv.mu.Unlock()
				continue
			}
			
			// Apply the operation
			switch op.Operation {
			case "Get":
				kv.applyGet(op)
			case "Put", "Append":
				kv.applyPutAppend(op)
			case "Reconfigure":
				kv.applyReconfigure(op)
			case "MigrateShard":
				kv.applyMigrateShard(op)
			}
			
			kv.lastAppliedIndex = msg.Index
			
			// Check if we need to create a snapshot
			kv.checkSnapshot(msg.Index)
			
			// Notify waiting RPC handler if any
			if ch, exists := kv.pending[msg.Index]; exists {
				select {
				case ch <- op:
				default:
				}
			}
			
			kv.mu.Unlock()
		}
	}
}

// Background goroutine to monitor configuration changes
func (kv *ShardKV) configMonitor() {
	for {
		time.Sleep(100 * time.Millisecond)
		
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		currentConfigNum := kv.config.Num
		kv.mu.Unlock()
		
		if isLeader {
			// Query for next configuration
			newConfig := kv.mck.Query(currentConfigNum + 1)
			if newConfig.Num == currentConfigNum + 1 {
				// New configuration detected, propose it
				op := Op{
					Operation: "Reconfigure",
					Config:    newConfig,
				}
				kv.rf.Start(op)
			} else {
				// Also check if we've missed any configurations
				// This happens when a server restarts and needs to catch up
				latestConfig := kv.mck.Query(-1)
				if latestConfig.Num > currentConfigNum {
					// We've missed some configurations, apply them one by one
					nextConfig := kv.mck.Query(currentConfigNum + 1)
					if nextConfig.Num == currentConfigNum + 1 {
						op := Op{
							Operation: "Reconfigure",
							Config:    nextConfig,
						}
						kv.rf.Start(op)
					}
				}
			}
		}
	}
}

// Apply Get operation
func (kv *ShardKV) applyGet(op Op) {
	// Check if this operation was already processed (duplicate detection)
	if seqNum, exists := kv.clientSeqNums[op.ClientId]; !exists || seqNum < op.SeqNum {
		// Get the value
		value := kv.kvStore[op.Key]
		
		// Store result for duplicate detection
		if kv.clientResults[op.ClientId] == nil {
			kv.clientResults[op.ClientId] = make(map[int64]string)
		}
		kv.clientResults[op.ClientId][op.SeqNum] = value
		kv.clientSeqNums[op.ClientId] = op.SeqNum
	}
}

// Apply Put/Append operation
func (kv *ShardKV) applyPutAppend(op Op) {
	// Check if this operation was already processed (duplicate detection)
	if seqNum, exists := kv.clientSeqNums[op.ClientId]; !exists || seqNum < op.SeqNum {
		// Apply the operation
		switch op.Operation {
		case "Put":
			kv.kvStore[op.Key] = op.Value
		case "Append":
			kv.kvStore[op.Key] += op.Value
		}
		kv.clientSeqNums[op.ClientId] = op.SeqNum
	}
}

// Apply reconfiguration
func (kv *ShardKV) applyReconfigure(op Op) {
	newConfig := op.Config
	if newConfig.Num == kv.config.Num + 1 {
		kv.lastConfig = kv.config
		kv.config = newConfig
		kv.updateShardOwnership()
		kv.startShardMigration()
	}
}

// Apply shard migration
func (kv *ShardKV) applyMigrateShard(op Op) {
	if kv.pendingShards[op.Shard] {
		// Merge the data
		for key, value := range op.Data {
			kv.kvStore[key] = value
		}
		
		// Merge client sequence numbers
		for clientId, seqNum := range op.ClientSeqNums {
			if kv.clientSeqNums[clientId] < seqNum {
				kv.clientSeqNums[clientId] = seqNum
			}
		}
		
		// Merge client results
		for clientId, results := range op.ClientResults {
			if kv.clientResults[clientId] == nil {
				kv.clientResults[clientId] = make(map[int64]string)
			}
			for seqNum, result := range results {
				kv.clientResults[clientId][seqNum] = result
			}
		}
		
		// Mark shard as owned and no longer pending
		kv.ownedShards[op.Shard] = true
		delete(kv.pendingShards, op.Shard)
	}
}

// Update shard ownership based on new configuration
func (kv *ShardKV) updateShardOwnership() {
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.config.Shards[shard] == kv.gid {
			// We should own this shard
			if !kv.ownedShards[shard] {
				// For the first configuration or when we're joining for the first time
				if kv.lastConfig.Num == 0 || kv.lastConfig.Shards[shard] == 0 {
					// No migration needed, just start owning the shard
					kv.ownedShards[shard] = true
				} else {
					// We don't own it yet, mark as pending migration
					kv.pendingShards[shard] = true
				}
			}
		} else {
			// We should not own this shard
			if kv.ownedShards[shard] {
				// We currently own it, prepare migration data
				kv.prepareMigrationData(shard)
				delete(kv.ownedShards, shard)
			}
		}
	}
}

// Prepare migration data for a shard
func (kv *ShardKV) prepareMigrationData(shard int) {
	data := make(map[string]string)
	for key, value := range kv.kvStore {
		if key2shard(key) == shard {
			data[key] = value
		}
	}
	kv.migrationData[shard] = data
}

// Start shard migration process
func (kv *ShardKV) startShardMigration() {
	for shard, data := range kv.migrationData {
		newGid := kv.config.Shards[shard]
		if servers, ok := kv.config.Groups[newGid]; ok {
			// Send migration data to new owner
			go kv.sendMigrationData(shard, newGid, servers, data)
		}
	}
	// Clear migration data after starting all migrations
	kv.migrationData = make(map[int]map[string]string)
}

// Send migration data to another group
func (kv *ShardKV) sendMigrationData(shard int, gid int, servers []string, data map[string]string) {
	kv.mu.Lock()
	
	// Create copies to avoid race conditions during RPC encoding
	clientSeqNums := make(map[int64]int64)
	for k, v := range kv.clientSeqNums {
		clientSeqNums[k] = v
	}
	
	clientResults := make(map[int64]map[int64]string)
	for k, v := range kv.clientResults {
		clientResults[k] = make(map[int64]string)
		for k2, v2 := range v {
			clientResults[k][k2] = v2
		}
	}
	
	configNum := kv.config.Num
	kv.mu.Unlock()
	
	args := MigrateShardArgs{
		Shard:         shard,
		ConfigNum:     configNum,
		Data:          data,
		ClientSeqNums: clientSeqNums,
		ClientResults: clientResults,
	}
	
	// Try each server in the target group
	for _, server := range servers {
		srv := kv.make_end(server)
		var reply MigrateShardReply
		ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
		if ok && reply.Err == OK {
			// Migration successful, clean up data
			kv.mu.Lock()
			delete(kv.migrationData, shard)
			// Remove keys for this shard from our store
			for key := range data {
				delete(kv.kvStore, key)
			}
			kv.mu.Unlock()
			return
		}
	}
	
	// Don't retry here to avoid goroutine explosion
	// The migration will be retried when the next configuration change happens
}

// Snapshot functions
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientSeqNums)
	e.Encode(kv.clientResults)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	e.Encode(kv.ownedShards)
	e.Encode(kv.pendingShards)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	
	var kvStore map[string]string
	var clientSeqNums map[int64]int64
	var clientResults map[int64]map[int64]string
	var lastAppliedIndex int
	var config shardmaster.Config
	var lastConfig shardmaster.Config
	var ownedShards map[int]bool
	var pendingShards map[int]bool
	
	if d.Decode(&kvStore) != nil ||
		d.Decode(&clientSeqNums) != nil ||
		d.Decode(&clientResults) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&ownedShards) != nil ||
		d.Decode(&pendingShards) != nil {
		return
	}
	
	kv.kvStore = kvStore
	kv.clientSeqNums = clientSeqNums
	kv.clientResults = clientResults
	kv.lastAppliedIndex = lastAppliedIndex
	kv.config = config
	kv.lastConfig = lastConfig
	kv.ownedShards = ownedShards
	kv.pendingShards = pendingShards
	
	// Clear migration data as it should not persist across snapshots
	kv.migrationData = make(map[int]map[string]string)
}

func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		snapshot := kv.encodeSnapshot()
		kv.rf.SaveSnapshot(snapshot, index)
	}
}
