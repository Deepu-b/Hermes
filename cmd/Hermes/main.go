package main

import (
	"hermes/server"
	"hermes/store"
	"hermes/wal"
)

func main() {
	s := store.NewShardedStore(16)
	w, err := wal.NewWAL("log.log")
	if err != nil {
		panic(err)
	}

	newStore, err := store.NewWalStore(s, w)
	if err != nil {
		panic(err)
	}

	server := server.NewServer(":8080", newStore)
	server.Start()	// check by nc localhost 8080
}
