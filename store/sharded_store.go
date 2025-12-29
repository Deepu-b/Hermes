package store

import (
	"hash/fnv"
	"sync"
	"time"
)

/*
shardedStore partitions keys across multiple independent shards
to reduce lock contention under concurrent access.
*/
type shardedStore struct {
	numShards int
	shards    []shard
}

/*
Each shard owns its own store and mutex.
Operations on different shards can proceed concurrently.
*/
type shard struct {
	mu    sync.RWMutex
	store *store
}

/*
NewShardedStore creates a sharded store with the given number
of shards. Each shard maintains its own isolated state.
*/
func NewShardedStore(numShards int) DataStore {
	shards := make([]shard, numShards)
	for i := range numShards {
		shards[i] = shard{
			store: &store{
				data: make(map[string]Entry),
			},
		}
	}
	return &shardedStore{
		numShards: numShards,
		shards:    shards,
	}
}

/*
Read locks only the shard responsible for the given key.
Reads acquire an exclusive lock due to lazy expiration.
*/
func (s *shardedStore) Read(key string) (Entry, bool) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.store.Read(key)
}

/*
Write applies write semantics within the owning shard.
*/
func (s *shardedStore) Write(key string, value Entry, mode PutMode) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.store.Write(key, value, mode)
}

/*
Expire updates TTL metadata within the owning shard.
*/
func (s *shardedStore) Expire(key string, ttl time.Duration) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.store.Expire(key, ttl)
}

/*
getShard deterministically maps a key to its shard.
*/
func (s *shardedStore) getShard(key string) *shard {
	shardIndex := getShardIndex(key, s.numShards)
	return &s.shards[shardIndex]
}

/*
hashString computes a stable hash for shard selection.

It uses Fowler-Noll-Vo-1a algorithm where starting from 
a pre-defined offset, each byte b is xor-ed and multiplied 
by pre-defined prime resulting in deterministic hash
*/
func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/*
getShardIndex returns the shard index for a given key.
*/
func getShardIndex(key string, numShards int) int {
	return int(hashString(key) % uint32(numShards))
}
