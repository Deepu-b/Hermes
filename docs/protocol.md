# Protocol & Command Handling

This document describes how client requests are handled in the system, from
network input to datastore execution and response output.

The design separates **networking**, **protocol rules**, and **execution logic**
to keep the system easy to reason about and extend.

---

## Request Lifecycle

Each client request follows a fixed pipeline:

TCP Read
→ Framing (line-based)
→ Parse
→ Validate
→ Execute
→ Format Response
→ TCP Write

Each step has a single responsibility and clear boundaries.

---

## Server Responsibilities

The server layer is responsible for:

- Listening for TCP connections
- Managing connection lifecycles
- Enforcing read/write timeouts
- Enforcing maximum input size
- Orchestrating request flow

The server does **not** implement command semantics or datastore logic.

---

## Protocol Parsing

The protocol layer is responsible for:

- Parsing raw text input into commands
- Validating command names
- Validating argument count
- Validating argument types

If a command is successfully parsed, it is guaranteed to be syntactically valid.
Execution logic does not re-check syntax.

---

## Command Representation

A command represents a validated client intent:

- Immutable after parsing
- Free of networking concerns
- Independent of datastore implementation

Commands carry only structured data required for execution.

---

## Command Execution

The execution layer is responsible for:

- Mapping commands to datastore operations
- Enforcing command semantics
- Translating datastore outcomes into structured results

Execution assumes all inputs are already validated.

---

## Response Formatting

Responses are represented using structured response types.

The response layer is responsible for:

- Converting execution results into wire-format output
- Centralizing response formatting rules
- Ensuring consistent client-visible behavior

No response formatting occurs inside execution logic.

---

## Error Handling

Errors are classified by origin:

- Protocol errors indicate invalid client requests
- Execution errors indicate semantic failures or internal issues

This separation keeps error handling predictable and consistent.

---

## Command Registry

Supported commands are defined in a centralized registry that specifies:

- Command name
- Expected argument count
- Expected argument types

Adding a new command requires:
- defining its specification
- implementing execution logic
- adding tests

No server changes are required.

---

## Protocol Guarantees

Current guarantees:

- Commands are case-insensitive
- Arguments are whitespace-delimited
- One command per line
- Text-based protocol

Explicit non-goals:

- Binary-safe payloads
- Pipelining
- Transactions

These are intentionally deferred.

---

## Protocol Evolution

The protocol layer is isolated so that parsing and formatting can evolve
(e.g. RESP-style framing) without affecting execution or storage layers.

---
