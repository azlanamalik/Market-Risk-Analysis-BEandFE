package store

import (
	"context"

	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
)

type RiskRepository interface {
	SaveTick(ctx context.Context, tick domain.PriceTick) error
	GetPositionsBySymbol(ctx context.Context, symbol string) ([]domain.Position, error)
	SaveRisk(ctx context.Context, result domain.PositionRisk) error
}
