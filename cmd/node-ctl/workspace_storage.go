package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
)

func workspaceStorageCmd(in io.Reader, out io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return workspacedisk.Serve(ctx, in, out)
}
