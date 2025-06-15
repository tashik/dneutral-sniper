from dataclasses import dataclass
from enum import Enum
from datetime import datetime

class OptionType(Enum):
    CALL = "call"
    PUT = "put"

@dataclass
class VanillaOption:
    instrument_name: str
    option_type: OptionType
    strike: float
    expiry: datetime
    quantity: float
    underlying: str
    mark_price: float = None  # Used for runtime calculations/logging
    iv: float = None  # Implied volatility cache
