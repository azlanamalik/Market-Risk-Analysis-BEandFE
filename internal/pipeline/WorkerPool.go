package pipeline

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
)

// TickProcessor is the synchronous unit of work executed by a worker.
type TickProcessor func(context.Context, domain.PriceTick) error

// RandomSource is the part of rand.Rand used by Simulator. Supplying a seeded
// rand.Rand makes generated ticks reproducible in tests.
type RandomSource interface {
	Float64() float64
}

// Simulator generates one tick at a time for one symbol. It is intentionally
// a bounded random walk, not a financial model: it has no market regime,
// volatility, order-book, or trading-calendar behavior. Its purpose is to
// provide a simple ongoing input for pipeline and risk-processing tests.
type Simulator struct {
	symbol      string
	price       float64
	step        float64
	random      RandomSource
	nextEventID uint64
}

// NewSimulator creates a synchronous simulator for symbol. Each call to Next
// moves the midpoint by at most step and returns a new bid/ask quote.
func NewSimulator(symbol string, initialPrice, step float64, random RandomSource) (*Simulator, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol must not be empty")
	}
	if !isPositiveFinite(initialPrice) {
		return nil, fmt.Errorf("initial price must be positive and finite")
	}
	if math.IsNaN(step) || math.IsInf(step, 0) || step < 0 {
		return nil, fmt.Errorf("step must be finite and non-negative")
	}
	if random == nil {
		random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	return &Simulator{
		symbol: symbol,
		price:  initialPrice,
		step:   step,
		random: random,
	}, nil
}

// Next advances the random walk and returns the next market-data event.
func (simulator *Simulator) Next() domain.PriceTick {
	change := (simulator.random.Float64()*2 - 1) * simulator.step
	midpoint := simulator.price + change
	if !isPositiveFinite(midpoint) {
		midpoint = math.SmallestNonzeroFloat64
	}
	simulator.price = midpoint

	halfSpread := math.Min(simulator.step/10, midpoint/2)
	simulator.nextEventID++

	return domain.PriceTick{
		EventID:    fmt.Sprintf("%s-%d", simulator.symbol, simulator.nextEventID),
		Symbol:     simulator.symbol,
		Bid:        midpoint - halfSpread,
		Ask:        midpoint + halfSpread,
		ObservedAt: time.Now().UTC(),
	}
}

func isPositiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// WorkerPool fans ticks out to a fixed number of workers. A worker processes
// one tick before requesting another. Processing stops at the first error.
type WorkerPool struct {
	workers int
	process TickProcessor
}

// NewWorkerPool creates a pool with exactly workers processing goroutines.
func NewWorkerPool(workers int, process TickProcessor) (*WorkerPool, error) {
	if workers <= 0 {
		return nil, fmt.Errorf("worker count must be positive: %d", workers)
	}
	if process == nil {
		return nil, fmt.Errorf("tick processor must not be nil")
	}

	return &WorkerPool{workers: workers, process: process}, nil
}

// Run processes ticks until input is closed, the context is cancelled, or a
// processor returns an error. The first processing error is returned wrapped
// after every worker has stopped.
func (pool *WorkerPool) Run(ctx context.Context, input <-chan domain.PriceTick) error {
	if ctx == nil {
		return fmt.Errorf("run worker pool: nil context")
	}

	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errorMu  sync.Mutex
		firstErr error
	)

	recordError := func(err error) {
		errorMu.Lock()
		defer errorMu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	wg.Add(pool.workers)
	for worker := 0; worker < pool.workers; worker++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerContext.Done():
					return
				case tick, ok := <-input:
					if !ok {
						return
					}
					if workerContext.Err() != nil {
						return
					}
					if err := pool.process(workerContext, tick); err != nil {
						recordError(fmt.Errorf("process tick %q: %w", tick.Symbol, err))
						return
					}
				}
			}
		}()
	}

	wg.Wait()

	errorMu.Lock()
	err := firstErr
	errorMu.Unlock()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("run worker pool: %w", err)
	}
	return nil
}
