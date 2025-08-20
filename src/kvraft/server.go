package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "Get", "Put", or "Append"
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore        map[string]string                    // key-value store
	clientSeqNums  map[int64]int64                      // last sequence number processed for each client
	clientResults  map[int64]map[int64]string           // results for each client request [clientId][seqNum] -> result
	pending        map[int]chan Op                      // pending requests indexed by Raft log index
	
	// Snapshot state
	lastAppliedIndex int // last index applied to this state machine
	persister        *raft.Persister
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
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
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
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

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	
	reply.WrongLeader = false
	reply.Err = OK
	
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
		if appliedOp.ClientId == args.ClientId && appliedOp.SeqNum == args.SeqNum {
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

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// Encode current state into a snapshot
func (kv *RaftKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientSeqNums)
	e.Encode(kv.clientResults)
	e.Encode(kv.lastAppliedIndex)
	return w.Bytes()
}

// Decode snapshot and restore state
func (kv *RaftKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	
	var kvStore map[string]string
	var clientSeqNums map[int64]int64
	var clientResults map[int64]map[int64]string
	var lastAppliedIndex int
	
	if d.Decode(&kvStore) != nil ||
		d.Decode(&clientSeqNums) != nil ||
		d.Decode(&clientResults) != nil ||
		d.Decode(&lastAppliedIndex) != nil {
		log.Fatal("Failed to decode snapshot")
		return
	}
	
	kv.kvStore = kvStore
	kv.clientSeqNums = clientSeqNums
	kv.clientResults = clientResults
	kv.lastAppliedIndex = lastAppliedIndex
}

// Check if snapshot is needed and create one
func (kv *RaftKV) checkSnapshot(index int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		snapshot := kv.encodeSnapshot()
		kv.rf.SaveSnapshot(snapshot, index)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// Your initialization code here.
	kv.kvStore = make(map[string]string)
	kv.clientResults = make(map[int64]map[int64]string)
	kv.clientSeqNums = make(map[int64]int64)
	kv.pending = make(map[int]chan Op)
	kv.lastAppliedIndex = 0

	// Restore from snapshot if exists
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Start background goroutine to process applied messages
	go kv.processAppliedMessages()

	return kv
}

// Background goroutine to process messages from Raft
func (kv *RaftKV) processAppliedMessages() {
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			// Handle snapshots (Part B)
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
			
			// Check if this operation was already processed (duplicate detection)
			if seqNum, exists := kv.clientSeqNums[op.ClientId]; !exists || seqNum < op.SeqNum {
				// Apply the operation
				switch op.Operation {
				case "Put":
					kv.kvStore[op.Key] = op.Value
				case "Append":
					kv.kvStore[op.Key] += op.Value
				case "Get":
					if kv.clientResults[op.ClientId] == nil {
						kv.clientResults[op.ClientId] = make(map[int64]string)
					}
					kv.clientResults[op.ClientId][op.SeqNum] = kv.kvStore[op.Key]
				}
				kv.clientSeqNums[op.ClientId] = op.SeqNum
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
