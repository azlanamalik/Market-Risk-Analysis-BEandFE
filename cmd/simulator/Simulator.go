package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/marketdata"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/pipeline"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/store"
)

//bringing it altogether

//get bogus data
//check error
//stream the symbols

func Run(ctx context.Context, symbols []string) {
	fmt.Println("initialising the simulator")
	memoryStore := store.MemoryStore{}
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
	ticks, _ := simulator.Stream(ctx, symbols)
	var waitGroup sync.WaitGroup

	for tick := range ticks {
		fmt.Printf("[simulator] received tick: %s %s bid=%.4f ask=%.4f\n", tick.EventID, tick.Symbol, tick.Bid, tick.Ask)
		waitGroup.Add(1)
		go func(tick domain.PriceTick) {
			defer waitGroup.Done()
			fmt.Printf("[simulator] processing tick: %s\n", tick.EventID)
			pipeline.Processor(ctx, tick, &memoryStore)
			fmt.Printf("[simulator] finished tick: %s\n", tick.EventID)
		}(tick)
	}

	fmt.Println("[simulator] stream stopped; waiting for processors")
	waitGroup.Wait()
	fmt.Println("[simulator] all ticks processed")
}
