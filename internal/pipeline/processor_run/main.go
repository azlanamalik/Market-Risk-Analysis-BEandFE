package main

import (
	"context"
	"fmt"
	"time"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/pipeline"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/risk"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/store"
)

func main() {
	memoryStore := store.MemoryStore{}
	processor := pipeline.NewProcessor(&memoryStore, risk.Engine{})
	ctx := context.Background()

	memoryStore.InsertPosition(ctx, domain.Position{
		PortfolioID:  "growth-portfolio",
		Symbol:       "AAPL",
		Quantity:     100,
		AveragePrice: 225.50,
	})
	memoryStore.InsertPosition(ctx, domain.Position{
		PortfolioID:  "income-portfolio",
		Symbol:       "AAPL",
		Quantity:     40,
		AveragePrice: 228.25,
	})

	tick := domain.PriceTick{
		EventID:    "quick-check-1",
		Symbol:     "AAPL",
		Bid:        229.90,
		Ask:        230.10,
		ObservedAt: time.Now(),
	}

	if err := processor.Process(ctx, tick); err != nil {
		fmt.Printf("processor failed: %v\n", err)
	}

	fmt.Println("processor completed")

	positions, _ := memoryStore.GetPositionsBySymbol(ctx, "AAPL")
	fmt.Println("stored positions:")
	for index, position := range positions {
		fmt.Printf("%d: %+v\n", index, position)
	}

	storedTick, _ := memoryStore.GetPriceTick(ctx, "AAPL")
	fmt.Printf("stored price tick: %+v\n", storedTick)

	fmt.Println("stored risks:")
	for _, portfolioID := range []string{"growth-portfolio", "income-portfolio"} {
		storedRisk, err := memoryStore.GetRisk(ctx, portfolioID, "AAPL")
		if err != nil {
			fmt.Printf("%s: %v\n", portfolioID, err)
			continue
		}
		fmt.Printf("%s: %+v\n", portfolioID, storedRisk)
	}
}
