package main

import (
	"hermes/server"
	"hermes/store"
)

func main() {
	store := store.NewShardedStore(16)

	s := server.NewServer(":8080", store)
	s.Start()
}
