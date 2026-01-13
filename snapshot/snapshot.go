package snapshot

/*
Snapshot package provides a minimal, protocol-agnostic snapshot mechanism.

Key principles:
- No dependency on store or wal packages (prevents cyclic imports)
- Snapshot is a derived optimization, never the source of truth
- Format is intentionally simple and self-describing

Why binary (not JSON):
- Faster to read/write
- Smaller on disk
- No schema drift risk during refactors
*/

import (
	"encoding/binary"
	"io"
)

/*
Item is a minimal DTO representing a single persisted entry.

Design choice:
- This struct intentionally does NOT depend on store.Entry
- Acts as a stable persistence boundary even if store internals evolve
*/
type Item struct {
	Key       string
	Value     []byte
	ExpiresAt int64
}

/*
Streamer defines a push-based iterator over snapshot items.

Why push-based (instead of pull / iterator object):
- Avoids exposing store internals
- Allows snapshot logic to remain stateless
- Works uniformly across locked, sharded, and event-loop stores
*/
type Streamer func(yield func(Item) bool)

/*
Write serializes a stream of items into a compact binary snapshot.

Binary Format (Little Endian): [KeyLen:int32][KeyBytes][ValLen:int32][ValueBytes][Expire:int64]

- Binary over JSON → smaller, faster, deterministic
- Length-prefixed fields → safe parsing without delimiters
- One-pass streaming → no need to buffer entire dataset in memory
*/
func Write(w io.Writer, stream Streamer) error {
	var writeErr error

	// Helper to centralize binary.Write error handling
	write := func(v any) {
		if writeErr != nil {
			return
		}
		writeErr = binary.Write(w, binary.LittleEndian, v)
	}

	// Stream items one-by-one to avoid memory amplification
	stream(func(item Item) bool {
		write(int32(len(item.Key)))
		if writeErr == nil {
			_, writeErr = w.Write([]byte(item.Key))
		}

		write(int32(len(item.Value)))
		if writeErr == nil {
			_, writeErr = w.Write(item.Value)
		}

		write(int64(item.ExpiresAt))

		// Stop streaming on first failure
		return writeErr == nil
	})
	return writeErr
}

/*
Load reconstructs state from a snapshot file.

Corruption policy:
- EOF is treated as successful termination
- Any other error aborts loading
- Partial snapshots are rejected rather than partially applied

This strictness prevents silently loading inconsistent state.
*/
func Load(r io.Reader, set func(Item)) error {
	for {
		var keyLen int32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				return nil // End of file, success
			}
			return err
		}
		if keyLen < 0 {
			return io.ErrUnexpectedEOF
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return err
		}

		var valLen int32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return err
		}
		if valLen < 0 {
			return io.ErrUnexpectedEOF
		}

		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return err
		}

		var expire int64
		if err := binary.Read(r, binary.LittleEndian, &expire); err != nil {
			return err
		}

		// Delegate application logic to caller
		set(Item{
			Key:       string(keyBytes),
			Value:     valBytes,
			ExpiresAt: expire,
		})
	}
}
