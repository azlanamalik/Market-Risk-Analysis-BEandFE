package pipeline

import (
	"context"
	"fmt"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/store"
)

type RiskEngine interface {
	Calculate(domain.PriceTick, domain.Position) domain.PositionRisk
}

type Processor struct {
	store  store.RiskRepository
	engine RiskEngine
}

func NewProcessor(repository store.RiskRepository, engine RiskEngine) *Processor {
	return &Processor{store: repository, engine: engine}
}

func (processor *Processor) Process(ctx context.Context, tick domain.PriceTick) error {
	if !tick.Validator() {
		return fmt.Errorf("validate tick: invalid tick")
	}

	if err := processor.store.SaveTick(ctx, tick); err != nil {
		return fmt.Errorf("save tick: %w", err)
	}

	positions, err := processor.store.GetPositionsBySymbol(ctx, tick.Symbol)
	if err != nil {
		return fmt.Errorf("load positions: %w", err)
	}

	for _, position := range positions {
		result := processor.engine.Calculate(tick, position)
		if err := processor.store.SaveRisk(ctx, result); err != nil {
			return fmt.Errorf("save risk: %w", err)
		}
	}

	return nil
}
