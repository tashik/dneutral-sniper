from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from uuid import UUID, uuid4


class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"


class OptionPositionCreate(BaseModel):
    """Schema for creating a new option position"""
    symbol: str = Field(..., description="Option symbol (e.g., BTC-25DEC23-40000-C)")
    option_type: OptionType = Field(..., description="Type of option (call/put)")
    strike: float = Field(..., gt=0, description="Strike price of the option")
    expiration: datetime = Field(..., description="Expiration date of the option")
    quantity: float = Field(..., gt=0, description="Number of contracts")
    premium: float = Field(..., description="Premium paid/received per contract")
    underlying: str = Field(..., description="Underlying asset (e.g., BTC-USD)")


class OptionPositionResponse(OptionPositionCreate):
    """Schema for returning option position data"""
    id: str = Field(..., description="Position ID")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    delta: Optional[float] = Field(None, description="Option delta")
    gamma: Optional[float] = Field(None, description="Option gamma")
    theta: Optional[float] = Field(None, description="Option theta")
    vega: Optional[float] = Field(None, description="Option vega")
    iv: Optional[float] = Field(None, description="Implied volatility")
    mark_price: Optional[float] = Field(None, description="Current mark price")


class PortfolioCreate(BaseModel):
    """Schema for creating a new portfolio"""
    name: str = Field(..., min_length=1, max_length=100, description="Portfolio name")
    description: Optional[str] = Field(None, max_length=500, description="Optional portfolio description")
    underlying: str = Field(..., description="Underlying asset symbol (e.g., BTC-USD)")


class PortfolioUpdate(PortfolioCreate):
    """Schema for updating a portfolio"""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="New portfolio name")
    underlying: Optional[str] = Field(None, description="Underlying asset symbol (e.g., BTC-USD)")


class PortfolioResponse(PortfolioCreate):
    """Schema for returning portfolio data"""
    id: str = Field(..., description="Portfolio ID")
    underlying: str = Field(..., description="Underlying asset symbol (e.g., BTC-USD)")
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    options: List[OptionPositionResponse] = Field(default_factory=list, description="List of options in the portfolio")
    total_delta: float = Field(0.0, description="Total portfolio delta")
    total_gamma: float = Field(0.0, description="Total portfolio gamma")
    total_theta: float = Field(0.0, description="Total portfolio theta")
    total_vega: float = Field(0.0, description="Total portfolio vega")
    total_value: float = Field(0.0, description="Total portfolio value in quote currency")

    @validator('options', pre=True)
    def set_empty_list(cls, v):
        return v or []
        
    @validator('created_at', 'updated_at', pre=True)
    def parse_datetime(cls, v, field):
        if v is None:
            return datetime.utcnow()
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                return datetime.utcnow()
        return v


class PortfolioListResponse(BaseModel):
    """Schema for returning a list of portfolios"""
    portfolios: List[PortfolioResponse]
    total: int


class PnLDataPoint(BaseModel):
    """Schema for PnL data points"""
    timestamp: datetime
    pnl: float
    underlying_price: float


class PnLResponse(BaseModel):
    """Schema for PnL history response"""
    portfolio_id: UUID
    data: List[PnLDataPoint]
    current_pnl: float
    pnl_24h: float


class ErrorResponse(BaseModel):
    """Standard error response"""
    detail: str
    code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
