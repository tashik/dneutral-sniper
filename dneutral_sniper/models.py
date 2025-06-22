from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from typing import Optional


class OptionType(Enum):
    CALL = "call"
    PUT = "put"


class ContractType(Enum):
    INVERSE = "inverse"  # Settled in BTC, quoted in USD (e.g., BTC-28JUN24-60000-C)
    STANDARD = "standard"  # Settled in USD, quoted in USD (e.g., BTC-28JUN24-60000C)


@dataclass
class VanillaOption:
    instrument_name: str
    option_type: OptionType
    strike: float
    expiry: datetime
    quantity: float
    underlying: str
    contract_type: ContractType = ContractType.INVERSE  # Default to inverse for backward compatibility
    mark_price: Optional[float] = None  # Current mark price in USD
    iv: Optional[float] = None  # Implied volatility cache
    usd_value: Optional[float] = None  # Current USD value of the option position
    delta: Optional[float] = None  # Current delta of the option position
    _greeks_calculated: bool = field(default=False, init=False)  # Internal flag for greeks calculation state

    def __post_init__(self):
        """Validate that expiry is a datetime object and ensure it's timezone-aware."""
        if not isinstance(self.expiry, datetime):
            raise ValueError(f"expiry must be a datetime object, got {type(self.expiry).__name__}")

        # Ensure expiry is timezone-aware
        if self.expiry.tzinfo is None:
            self.expiry = self.expiry.replace(tzinfo=timezone.utc)
