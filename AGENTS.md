# AGENTS.md - MarketStore Development Guide

MarketStore is a database server optimized for financial time-series data, written in Go 1.18.
Module path: `github.com/alpacahq/marketstore/v4`

## Build Commands

```bash
make build              # Build the marketstore binary
make install            # Install the binary to $GOPATH/bin
make plugins            # Build all contrib plugins as .so shared objects
make fmt                # Run go fmt ./...
make generate           # Run go:generate directives
make generate-sql       # Regenerate ANTLR SQL parser
```

## Test Commands

```bash
# All unit tests
make unit-test                    # go test -coverprofile=coverage.txt -covermode=atomic ./...

# Single package tests
go test ./executor/...            # Test one package
go test ./catalog/... -run TestName  # Single test by name
go test -v ./frontend/... -run TestQuery  # Verbose single test

# Integration tests (require Docker)
make integration-test-jsonrpc     # Python pytest via Docker, JSON-RPC API
make integration-test-grpc        # Python pytest via Docker, gRPC API
make integration-test-contrib     # Tests contrib/ice plugin

# Full suite (build + unit + integration)
make test

# Replication tests (Docker)
make replication-test

# Coverage for a single package
scripts/coverage.sh ./executor/...
```

## Linting

No linter is configured. The only code quality check is `make fmt` (`go fmt ./...`).
Use `go vet ./...` to catch common issues. Some contrib plugins run `go vet` in their Makefiles.

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `cmd/` | CLI commands (Cobra): start, connect, create, estimate, tool |
| `executor/` | Core database engine: WAL, file I/O, caching, sorting, read/write |
| `frontend/` | gRPC + JSON-RPC server, query/write handlers, streaming |
| `catalog/` | Database catalog management |
| `planner/` | Query planning |
| `sqlparser/` | ANTLR-based SQL parser |
| `utils/` | Config, I/O helpers, logging, RPC, pool, stats, timeframes |
| `models/` | Data models (Bar, Trade, Quote) |
| `uda/` | User-defined aggregates (avg, count, gap, max, min, adjust) |
| `plugins/` | Plugin interfaces (trigger + bgworker) |
| `contrib/` | 15 contributed plugins (feeders + triggers), built as Go .so |
| `proto/` | Protobuf definitions (marketstore.proto, replication.proto) |
| `replication/` | Master-replica replication via gRPC streaming |
| `internal/di/` | Dependency injection container |
| `tests/` | Integration tests (Python/pytest in Docker) |

Default ports: 5993 (JSON-RPC), 5995 (gRPC), 5996 (replication).

## Code Style Guidelines

### Imports

Use three groups separated by blank lines: (1) stdlib, (2) third-party, (3) internal.
Alphabetical within each group. Use aliases only for naming conflicts.

```go
import (
    "fmt"
    "os"
    "sync"

    "go.uber.org/zap"

    "github.com/alpacahq/marketstore/v4/executor/wal"
    "github.com/alpacahq/marketstore/v4/utils/log"
)
```

Common aliases: `goio "io"` or `stdio "io"` (conflicts with `utils/io`),
`pb "github.com/alpacahq/marketstore/v4/proto"`,
`utilsio "github.com/alpacahq/marketstore/v4/utils/io"`.

### Naming Conventions

- **Acronyms are UPPERCASE**: `WALFile`, `IOPlan`, `RPCServer`, `GRPCListenURL`, `TGID`, `CSM`
- **Exported types**: PascalCase (`Directory`, `DataService`, `Sender`)
- **Unexported types**: camelCase (`ioFilePlan`, `bufferMeta`, `aux`)
- **Unexported sentinel errors**: camelCase (`errNotQueryable`)
- **Exported sentinel errors**: PascalCase (`ErrRetryable`)
- **Receiver names**: Short, 1-2 chars (`d *Directory`, `wf *WALFileType`, `s *DataService`)
- **File names**: snake_case for multi-word (`grpc_client.go`, `wal_test.go`), lowercase single-word (`catalog.go`, `errors.go`)
- **Error types**: Collected in separate `errors.go` files per package

### Error Handling

**Preferred (modern)**: `fmt.Errorf` with `%w` for wrapping:
```go
return fmt.Errorf("read dir %s: %w", subPath, err)
```

**Legacy**: `github.com/pkg/errors` wrapping still exists in `replication/` and `contrib/`:
```go
return errors.Wrap(err, "failed to get wal message stream")
```

**Custom error types**: String-based types in `catalog/errors.go` and `executor/errors.go`:
```go
type WALCreateError string
func (msg WALCreateError) Error() string { ... }
```

Use `errors.As()` and `errors.Is()` for error matching. When adding new error handling
code, prefer `fmt.Errorf` with `%w` over `pkg/errors`.

### Logging

Use `go.uber.org/zap` via the `utils/log` wrapper. Import as `"github.com/alpacahq/marketstore/v4/utils/log"`.

**Printf-style** (dominant pattern):
```go
log.Info("using %v for configuration", configFilePath)
log.Error("failed to write bars for %s (%+v)", model.Key(), err)
```

**Structured zap style** (used directly in some places):
```go
log.Error("failed to close walfile", zap.Error(err))
```

The logger is a global singleton -- do not pass it through function parameters.

### Types and Interfaces

- Define interfaces at the **consumer** site, not the implementation site
- Keep interfaces small (1-3 methods)
- Place custom error types in a dedicated `errors.go` file per package

### Concurrency

- Embed `sync.RWMutex` in structs for thread safety; use `defer d.Unlock()` where possible
- Use `sync.Map` for concurrent map access
- Use buffered channels as work queues
- Use `context.Context` for cancellation and lifecycle
- Use `sync/atomic` for flag toggling
- Worker pool in `utils/pool/pool.go`

### Testing

- **Framework**: `testing` + `github.com/stretchr/testify/assert`
- **Test packages**: Prefer black-box testing with `_test` package suffix
- **Setup helpers**: Use `t.Helper()` and `t.TempDir()` for test directories
- **Assertions**: Use `assert.Nil(t, err)`, `assert.Equal(t, expected, actual)`, `assert.Len(t, slice, n)`
- **Mocking**: `github.com/golang/mock` for interface mocks
- **Parallel tests**: `t.Parallel()` used in some tests but not required everywhere
- **Table-driven tests**: Used in `replication/` but most tests use standalone `TestXxx` functions
- **Race detector**: Disabled globally (`contrib/stream/shelf/shelf_test.go` fails with `-race`)

### Comments and Documentation

- Write GoDoc comments on exported functions and types
- Use `/* */` block comments for longer inline explanations
- Use `// nolint:` directives with a reason comment: `// nolint:gosec // keep for compatibility`

### Plugins

Plugins are built as Go shared objects: `go build -buildmode=plugin -o name.so .`
Each plugin has a `main` package with a stub `main(){}` and a factory function
(`NewBgWorker` or `NewTrigger`). See `plugins/README.md` for interface details.

## CI/CD

CircleCI 2.1 with `cimg/go:1.18.2`. Workflow: build + plugins in parallel,
unit tests independently, integration tests independently. Deploy on tags only.
Coverage via Codecov (target: 70% on patches, threshold: 2% on project).
