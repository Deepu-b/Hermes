package server

import (
	"hermes/protocol"
	"hermes/store"
	"testing"
)

func TestExecuteCommand_GET_MissingKey(t *testing.T) {
	s := &Server{}
	ds := store.NewStore()

	cmd := protocol.Command{
		Name: protocol.CommandGet,
		Args: []string{"missing"},
	}

	resp := s.executeCommand(cmd, ds)

	if resp.Kind != ResponseNil {
		t.Fatalf("expected ResponseNil, got %v", resp.Kind)
	}
}

func TestExecuteCommand_SET_Then_GET(t *testing.T) {
	s := &Server{}
	ds := store.NewStore()

	setCmd := protocol.Command{
		Name: protocol.CommandSet,
		Args: []string{"a", "1"},
	}

	getCmd := protocol.Command{
		Name: protocol.CommandGet,
		Args: []string{"a"},
	}

	resp := s.executeCommand(setCmd, ds)
	if resp.Kind != ResponseOK {
		t.Fatalf("expected ResponseOK, got %v", resp.Kind)
	}

	resp = s.executeCommand(getCmd, ds)
	if resp.Kind != ResponseValue || resp.Value != "1" {
		t.Fatalf("expected value '1', got %+v", resp)
	}
}

func TestExecuteCommand_EXPIRE_InvalidTTL(t *testing.T) {
	s := &Server{}
	ds := store.NewStore()

	cmd := protocol.Command{
		Name: protocol.CommandExpire,
		Args: []string{"a", "notanint"},
	}

	resp := s.executeCommand(cmd, ds)

	if resp.Kind != ResponseClientError {
		t.Fatalf("expected ResponseClientError, got %v", resp.Kind)
	}
}

func TestExecuteCommand_EXPIRE_MissingKey(t *testing.T) {
	s := &Server{}
	ds := store.NewStore()

	cmd := protocol.Command{
		Name: protocol.CommandExpire,
		Args: []string{"missing", "10"},
	}

	resp := s.executeCommand(cmd, ds)

	if resp.Kind != ResponseNil {
		t.Fatalf("expected ResponseNil, got %v", resp.Kind)
	}
}

func TestExecuteCommand_UnknownCommand(t *testing.T) {
	s := &Server{}
	ds := store.NewStore()

	cmd := protocol.Command{
		Name: "UNKNOWN",
		Args: []string{},
	}

	resp := s.executeCommand(cmd, ds)

	if resp.Kind != ResponseServerError {
		t.Fatalf("expected ResponseServerError, got %v", resp.Kind)
	}
}
