package simulator

import (
	"context"
	"testing"
	"time"
)

func TestRunProcessesTicksUntilContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	Run(ctx, []string{"AAPL", "MSFT"})
}
