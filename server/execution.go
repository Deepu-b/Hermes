package server

import (
	"hermes/protocol"
	"hermes/store"
	"strconv"
	"time"
)

/*
executeCommand maps a validated protocol command to datastore operations.
Note: It contains no networking logic and no concurrency concerns.
*/
func (s *Server) executeCommand(cmd protocol.Command, dataStore store.DataStore) Response {
	switch cmd.Name {
	case protocol.CommandGet:
		key := cmd.Args[0]
		entry, ok := dataStore.Read(key)

		if !ok {
			return Response{
				Kind: ResponseNil,
			}
		}
		return Response{
			Kind:  ResponseValue,
			Value: string(entry.Value),
		}

	case protocol.CommandSet:
		key := cmd.Args[0]
		val := cmd.Args[1]

		err := dataStore.Write(
			key,
			store.Entry{
				Value: []byte(val),
			},
			store.PutOverwrite,
		)
		if err != nil {
			return Response{
				Kind:  ResponseClientError,
				Value: err.Error(),
			}
		}
		return Response{
			Kind: ResponseOK,
		}

	case protocol.CommandExpire:
		key := cmd.Args[0]
		ttlSec, err := strconv.Atoi(cmd.Args[1])

		if err != nil {
			return Response{
				Kind:  ResponseClientError,
				Value: "invalid ttl",
			}
		}

		ok := dataStore.Expire(key, time.Duration(ttlSec)*time.Second)
		if !ok {
			return Response{
				Kind: ResponseNil,
			}
		}
		return Response{
			Kind: ResponseOK,
		}

	default:
		return Response{
			Kind: ResponseServerError,
		}
	}
}
