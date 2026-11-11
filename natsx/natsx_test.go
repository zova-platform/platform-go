package natsx_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/zova-platform/platform-go/natsx"
)

const natsImage = "nats:2.15-alpine"

var (
	natsURL string
	counter atomic.Int64
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := tcnats.Run(ctx, natsImage)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start nats container:", err)
		os.Exit(1)
	}
	natsURL, err = container.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "nats connection string:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(container)
	os.Exit(code)
}

type env struct {
	client  *natsx.Client
	stream  string
	subject string
}

func setup(t *testing.T) env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := natsx.Connect(natsx.Config{URL: natsURL, Name: "natsx-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	n := counter.Add(1)
	e := env{
		client:  client,
		stream:  fmt.Sprintf("TEST%d", n),
		subject: fmt.Sprintf("dispatch.t%d.requested", n),
	}
	if err := client.EnsureStream(ctx, natsx.StreamConfig{Name: e.stream, Subjects: []string{fmt.Sprintf("dispatch.t%d.>", n)}}); err != nil {
		t.Fatal(err)
	}
	return e
}
