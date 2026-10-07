package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/marketdata"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/pipeline"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/risk"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/store"
)

//bringing it altogether

//get bogus data
//check error
//stream the symbols

func Run(ctx context.Context, symbols []string) {
	fmt.Println("initialising the simulator")
	memoryStore := store.MemoryStore{}
	processor := pipeline.NewProcessor(&memoryStore, risk.Engine{})
	simulator, errSimCreation := marketdata.NewSimulator(map[string]float64{
		"AAPL":  336.13,
		"MSFT":  493.78,
		"GOOGL": 349.54,
		"AMZN":  253.71,
		"TSLA":  364.27,
	}, 1*time.Second, map[string]float64{
		"AAPL":  0.02,
		"MSFT":  0.02,
		"GOOGL": 0.02,
		"AMZN":  0.02,
		"TSLA":  0.04,
	})
	if errSimCreation != nil {
		slog.Warn("there was an error starting the simulator", "error", errSimCreation)
		return
	}

	fmt.Printf("[simulator] streaming symbols: %v\n", symbols)
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	ticks, streamErrors := simulator.Stream(runContext, symbols)
	processTick := func(ctx context.Context, tick domain.PriceTick) error {
		fmt.Printf("[simulator] processing tick: %s %s bid=%.4f ask=%.4f\n",
			tick.EventID, tick.Symbol, tick.Bid, tick.Ask)
		if err := processor.Process(ctx, tick); err != nil {
			return fmt.Errorf("process %s: %w", tick.EventID, err)
		}
		fmt.Printf("[simulator] processed tick: %s\n", tick.EventID)
		return nil
	}
	pool, errPoolCreation := pipeline.NewWorkerPool(4, processTick)
	if errPoolCreation != nil {
		slog.Error("there was an error starting the worker pool", "error", errPoolCreation)
		return
	}

	if err := pool.Run(runContext, ticks); err != nil {
		slog.Error("worker pool stopped", "error", err)
		return
	}
	for streamErr := range streamErrors {
		if streamErr != nil {
			slog.Error("market-data stream failed", "error", streamErr)
		}
	}
	fmt.Println("[simulator] stream stopped; all ticks processed")
}
