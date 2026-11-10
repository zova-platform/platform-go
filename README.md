# platform-go

Shared infrastructure library for ZOVA Go services: config, logging,
OpenTelemetry, gRPC, health endpoints and graceful shutdown.

Rule: no business logic in this repo.

```go
import "github.com/zova-platform/platform-go/logx"
```

Install a tagged version with `go get github.com/zova-platform/platform-go@vX.Y.Z`.
