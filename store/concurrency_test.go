package store

import (
	"sync"
	"testing"
	"time"
)

/*
storeFactory abstracts store construction so the same
tests can be executed against different concurrency models.
*/
type storeFactory func() DataStore

func runConcurrencyTests(t *testing.T, name string, newStore storeFactory) {
	t.Run(name, func(t *testing.T) {
		t.Run("ConcurrentWritesSameKey", func(t *testing.T) {
			testConcurrentWritesSameKey(t, newStore)
		})

		t.Run("ConcurrentReadsAndWrites", func(t *testing.T) {
			testConcurrentReadsAndWrites(t, newStore)
		})

		t.Run("ConcurrentExpireAndRead", func(t *testing.T) {
			testConcurrentExpireAndRead(t, newStore)
		})

		t.Run("testExpiredKeyCanBeRecreatedExplicitly", func(t *testing.T) {
			testExpiredKeyCanBeRecreatedExplicitly(t, newStore)
		})
	})
}

func TestConcurrencyModels(t *testing.T) {
	runConcurrencyTests(t, "LockedStore", func() DataStore {
		return NewLockedStore()
	})

	runConcurrencyTests(t, "EventLoopStore", func() DataStore {
		return NewEventloopStore(128)
	})

	runConcurrencyTests(t, "ShardedStore", func() DataStore {
		return NewShardedStore(8)
	})
}

/*
Multiple goroutines writing the same key concurrently.
Final value must be one of the written values and no corruption
or panic should occur.
*/
func testConcurrentWritesSameKey(t *testing.T, newStore storeFactory) {
	s := newStore()

	const writers = 50
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = s.Write("key", Entry{Value: []byte{byte(i)}}, PutOverwrite)
		}()
	}

	wg.Wait()

	val, ok := s.Read("key")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if len(val.Value) != 1 {
		t.Fatalf("unexpected value corruption")
	}
}

/*
Readers and writers operate concurrently.
Reads must never observe partial or invalid state.
*/
func testConcurrentReadsAndWrites(t *testing.T, newStore storeFactory) {
	s := newStore()

	_ = s.Write("key", Entry{Value: []byte("init")}, PutOverwrite)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = s.Read("key")
		}()

		go func() {
			defer wg.Done()
			_ = s.Write("key", Entry{Value: []byte("updated")}, PutOverwrite)
		}()
	}

	wg.Wait()

	_, ok := s.Read("key")
	if !ok {
		t.Fatalf("expected key to exist after concurrent access")
	}
}

/*
Expire and Read racing concurrently.
Expired keys must never be observable.
*/
func testConcurrentExpireAndRead(t *testing.T, newStore storeFactory) {
	s := newStore()

	_ = s.Write("key", Entry{Value: []byte("value")}, PutOverwrite)
	_ = s.Expire("key", GetUnixTimestamp(time.Now().Add(20*time.Millisecond)))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		_, ok := s.Read("key")
		if ok {
			t.Fatalf("expected key to be expired")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, _ = s.Read("key")
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
}

/*
Ensure that an expired key can be recreated explicitly
*/
func testExpiredKeyCanBeRecreatedExplicitly(t *testing.T, newStore storeFactory) {
	s := newStore()

	_ = s.Write("key", Entry{Value: []byte("value")}, PutOverwrite)
	_ = s.Expire("key", GetUnixTimestamp(time.Now().Add(10*time.Millisecond)))

	time.Sleep(20 * time.Millisecond)

	_, ok := s.Read("key")
	if ok {
		t.Fatalf("expired key should not be visible")
	}

	err := s.Write("key", Entry{Value: []byte("new")}, PutOverwrite)
	if err != nil {
		t.Fatalf("expected overwrite to recreate key")
	}

	val, ok := s.Read("key")
	if !ok || string(val.Value) != "new" {
		t.Fatalf("expected recreated key")
	}
}
