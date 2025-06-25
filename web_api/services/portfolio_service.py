"""
Service layer for portfolio-related operations.

This module provides functions to interact with the portfolio manager
and transform data between API and domain models.
"""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import HTTPException, status

from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.models import OptionType, ContractType, VanillaOption
from web_api.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    OptionPositionCreate,
    OptionPositionResponse,
    PnLDataPoint,
    PnLResponse
)

# Initialize portfolio manager with default data directory
portfolio_manager = PortfolioManager()

# In-memory storage for demo purposes
# In a production environment, this would be replaced with a proper database
portfolio_mapping: Dict[str, str] = {}  # Maps API portfolio IDs to internal portfolio IDs


def _to_option_type(option_type_str: str) -> OptionType:
    """Convert option type string to OptionType enum."""
    try:
        return OptionType[option_type_str.upper()]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid option type: {option_type_str}. Must be 'call' or 'put'."
        )


def _from_option_type(option_type: OptionType) -> str:
    """Convert OptionType enum to string."""
    return option_type.value.lower()


def _to_vanilla_option(option_data: dict) -> VanillaOption:
    """Convert API option data to VanillaOption."""
    return VanillaOption(
        symbol=option_data["symbol"],
        option_type=_to_option_type(option_data["option_type"]),
        strike=option_data["strike"],
        expiration=datetime.fromisoformat(option_data["expiration"]),
        quantity=option_data["quantity"],
        premium=option_data["premium"],
        underlying=option_data["underlying"]
    )


def _from_vanilla_option(option: VanillaOption) -> dict:
    """Convert VanillaOption to API response format."""
    return {
        "id": str(uuid.uuid4()),  # Generate a new ID for the API
        "symbol": option.symbol,
        "option_type": _from_option_type(option.option_type),
        "strike": option.strike,
        "expiration": option.expiration.isoformat(),
        "quantity": option.quantity,
        "premium": option.premium,
        "underlying": option.underlying,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }


async def create_portfolio(portfolio_data: PortfolioCreate) -> PortfolioResponse:
    """Create a new portfolio."""
    try:
        # Create a new portfolio in the portfolio manager
        portfolio_id = str(uuid.uuid4())
        portfolio = {
            "id": portfolio_id,
            "name": portfolio_data.name,
            "description": portfolio_data.description or "",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "options": [],
            "total_delta": 0.0,
            "total_gamma": 0.0,
            "total_theta": 0.0,
            "total_vega": 0.0,
            "total_value": 0.0
        }
        
        # In a real implementation, we would save this to the portfolio manager
        # For now, we'll just store it in memory
        portfolio_mapping[portfolio_id] = portfolio
        
        return portfolio
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create portfolio: {str(e)}"
        )


async def get_portfolio(portfolio_id: str) -> PortfolioResponse:
    """Get a portfolio by ID."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    return portfolio_mapping[portfolio_id]


async def list_portfolios() -> List[PortfolioResponse]:
    """List all portfolios."""
    return list(portfolio_mapping.values())


async def update_portfolio(
    portfolio_id: str,
    portfolio_data: PortfolioUpdate
) -> PortfolioResponse:
    """Update a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolio_mapping[portfolio_id]
    update_data = portfolio_data.dict(exclude_unset=True)
    
    for field, value in update_data.items():
        if field in portfolio and value is not None:
            portfolio[field] = value
    
    portfolio["updated_at"] = datetime.utcnow().isoformat()
    return portfolio


async def delete_portfolio(portfolio_id: str) -> None:
    """Delete a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    del portfolio_mapping[portfolio_id]


async def add_option(
    portfolio_id: str,
    option_data: OptionPositionCreate
) -> OptionPositionResponse:
    """Add an option to a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolio_mapping[portfolio_id]
    
    # Create a new option
    option = {
        "id": str(uuid.uuid4()),
        **option_data.dict(),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    # Add the option to the portfolio
    if "options" not in portfolio:
        portfolio["options"] = []
    portfolio["options"].append(option)
    portfolio["updated_at"] = datetime.utcnow().isoformat()
    
    return option


async def update_option(
    portfolio_id: str,
    option_id: str,
    option_data: OptionPositionCreate
) -> OptionPositionResponse:
    """Update an option in a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolio_mapping[portfolio_id]
    
    # Find and update the option
    for option in portfolio.get("options", []):
        if option["id"] == option_id:
            update_data = option_data.dict(exclude_unset=True)
            for field, value in update_data.items():
                option[field] = value
            option["updated_at"] = datetime.utcnow().isoformat()
            portfolio["updated_at"] = datetime.utcnow().isoformat()
            return option
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Option {option_id} not found in portfolio {portfolio_id}"
    )


async def remove_option(portfolio_id: str, option_id: str) -> None:
    """Remove an option from a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolio_mapping[portfolio_id]
    
    # Find and remove the option
    for i, option in enumerate(portfolio.get("options", [])):
        if option["id"] == option_id:
            portfolio["options"].pop(i)
            portfolio["updated_at"] = datetime.utcnow().isoformat()
            return
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Option {option_id} not found in portfolio {portfolio_id}"
    )


async def get_portfolio_pnl(portfolio_id: str) -> PnLResponse:
    """Get PnL data for a portfolio."""
    if portfolio_id not in portfolio_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    # In a real implementation, this would calculate actual PnL
    # For now, we'll return mock data
    now = datetime.utcnow()
    historical_data = [
        PnLDataPoint(
            timestamp=(now - timedelta(minutes=i)).isoformat(),
            pnl=i * 100.0,  # Mock PnL
            underlying_price=1000.0 + (i * 10.0)  # Mock underlying price
        )
        for i in range(10, 0, -1)
    ]
    
    return PnLResponse(
        portfolio_id=portfolio_id,
        current_pnl=1000.0,  # Mock current PnL
        pnl_24h=100.0,  # Mock 24h PnL
        data=historical_data
    )
