"""
Common types and enums for the dNeutral Sniper application.
"""
from enum import Enum, auto
from datetime import datetime
from typing import Dict, Any, List, Optional, Union, Tuple


class PortfolioEventType(Enum):
    """Types of events that can occur in a portfolio."""
    POSITION_OPENED = auto()
    POSITION_CLOSED = auto()
    POSITION_ADJUSTED = auto()
    DEPOSIT = auto()
    WITHDRAWAL = auto()
    TRADE_EXECUTED = auto()
    HEDGE_EXECUTED = auto()
    REBALANCE = auto()
    DIVIDEND = auto()
    EXPIRATION = auto()
    OPTION_ADDED = auto()
    OPTION_REMOVED = auto()
    STATE_CHANGED = auto()
    ASSIGNMENT = auto()
    EXERCISE = auto()
    ADJUSTMENT = auto()
    FEE = auto()
    INTEREST = auto()
    CORPORATE_ACTION = auto()
    OTHER = auto()


class OptionType(Enum):
    """Option type (Call or Put)."""
    CALL = "call"
    PUT = "put"


class OptionStyle(Enum):
    """Option style (American or European)."""
    AMERICAN = "american"
    EUROPEAN = "european"


class ContractType(Enum):
    """Contract type (Futures, Option, etc.)."""
    FUTURE = "future"
    OPTION = "option"
    PERPETUAL = "perpetual"
    SPOT = "spot"
    INDEX = "index"


class OrderSide(Enum):
    """Order side (Buy or Sell)."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type (Limit, Market, etc.)."""
    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    MARKET_IF_TOUCHED = "market_if_touched"
    LIMIT_IF_TOUCHED = "limit_if_touched"
    MARKET_WITH_LEFTOVER_AS_LIMIT = "market_with_leftover_as_limit"


class PositionSide(Enum):
    """Position side (Long or Short)."""
    LONG = "long"
    SHORT = "short"


class PortfolioEvent:
    """Represents an event in a portfolio's history."""
    
    def __init__(
        self,
        event_type: PortfolioEventType,
        timestamp: datetime,
        data: Dict[str, Any],
        portfolio_id: Optional[str] = None,
        position_id: Optional[str] = None,
    ):
        self.event_type = event_type
        self.timestamp = timestamp
        self.data = data
        self.portfolio_id = portfolio_id
        self.position_id = position_id
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a dictionary."""
        return {
            "event_type": self.event_type.name,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "portfolio_id": self.portfolio_id,
            "position_id": self.position_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PortfolioEvent':
        """Create an event from a dictionary."""
        return cls(
            event_type=PortfolioEventType[data["event_type"]],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data["data"],
            portfolio_id=data.get("portfolio_id"),
            position_id=data.get("position_id"),
        )


class Greeks:
    """Represents the greeks for an option or portfolio."""
    
    def __init__(
        self,
        delta: float = 0.0,
        gamma: float = 0.0,
        theta: float = 0.0,
        vega: float = 0.0,
        rho: float = 0.0,
    ):
        self.delta = delta
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        self.rho = rho
    
    def to_dict(self) -> Dict[str, float]:
        """Convert the greeks to a dictionary."""
        return {
            "delta": self.delta,
            "gamma": self.gamma,
            "theta": self.theta,
            "vega": self.vega,
            "rho": self.rho,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> 'Greeks':
        """Create greeks from a dictionary."""
        return cls(**data)
    
    def __add__(self, other: 'Greeks') -> 'Greeks':
        """Add two Greeks objects together."""
        if not isinstance(other, Greeks):
            raise TypeError("Can only add Greeks to other Greeks")
        
        return Greeks(
            delta=self.delta + other.delta,
            gamma=self.gamma + other.gamma,
            theta=self.theta + other.theta,
            vega=self.vega + other.vega,
            rho=self.rho + other.rho,
        )
    
    def __mul__(self, scalar: float) -> 'Greeks':
        """Multiply Greeks by a scalar."""
        if not isinstance(scalar, (int, float)):
            raise TypeError("Can only multiply Greeks by a scalar")
        
        return Greeks(
            delta=self.delta * scalar,
            gamma=self.gamma * scalar,
            theta=self.theta * scalar,
            vega=self.vega * scalar,
            rho=self.rho * scalar,
        )
    
    def __repr__(self) -> str:
        """Return a string representation of the Greeks."""
        return (
            f"Greeks(delta={self.delta:.4f}, gamma={self.gamma:.4f}, "
            f"theta={self.theta:.4f}, vega={self.vega:.4f}, rho={self.rho:.4f})"
        )
