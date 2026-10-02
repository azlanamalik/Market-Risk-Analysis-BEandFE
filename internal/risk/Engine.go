package risk

import (
	"github.com/azlanamalik/Market-Risk-Analysis-backend-and-frontend-API-/internal/domain"
)

type Engine struct{}

func (Engine) Calculate(priceTick domain.PriceTick, position domain.Position) domain.PositionRisk {
	return Risk(priceTick, position)
}

func Risk(priceTick domain.PriceTick, position domain.Position) domain.PositionRisk {
	valid := priceTick.Validator()
	if !valid || priceTick.Symbol != position.Symbol {
		return domain.PositionRisk{
			PortfolioID:   "error loading",
			Symbol:        "error loading",
			MarketPrice:   1.0,
			MarketValue:   1.0,
			UnrealizedPnL: 1.0,
		} //dont terminate as thining on a scale of full network we are sending constant packages it is bad if we terminate program on a slight change of dat
	}

	marketPrice := priceTick.Midpoint()
	marketValue := position.Quantity * marketPrice
	unrealizedPnL := position.Quantity * (marketPrice - position.AveragePrice)
	return domain.PositionRisk{
		PortfolioID:   position.PortfolioID,
		Symbol:        position.Symbol,
		MarketPrice:   marketPrice,
		MarketValue:   marketValue,
		UnrealizedPnL: unrealizedPnL,
	}
}
