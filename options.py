from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum
import numpy as np
from datetime import datetime
import math

class OptionType(Enum):
    CALL = "call"
    PUT = "put"

@dataclass
class VanillaOption:
    instrument_name: str
    option_type: OptionType
    strike: float
    expiry: datetime
    quantity: float  # Positive for long, negative for short
    underlying: str = "BTC-PERPETUAL"

class BlackScholesModel:
    @staticmethod
    def calculate_d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate d1 parameter for Black-Scholes formula"""
        if T <= 0:
            return float('inf') if S > K else float('-inf')
        return (np.log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))

    @staticmethod
    def calculate_d2(d1: float, sigma: float, T: float) -> float:
        """Calculate d2 parameter for Black-Scholes formula"""
        return d1 - sigma * np.sqrt(T)

    @staticmethod
    def calculate_delta(option_type: OptionType, d1: float) -> float:
        """Calculate option delta"""
        if option_type == OptionType.CALL:
            return float(norm.cdf(d1))
        else:
            return float(norm.cdf(d1) - 1)

class Portfolio:
    def __init__(self):
        self.options: Dict[str, VanillaOption] = {}
        self._total_delta: Optional[float] = None

    def add_option(self, option: VanillaOption):
        """Add option to portfolio"""
        self.options[option.instrument_name] = option
        self._total_delta = None  # Reset cached delta

    def remove_option(self, instrument_name: str):
        """Remove option from portfolio"""
        if instrument_name in self.options:
            del self.options[instrument_name]
            self._total_delta = None  # Reset cached delta

    def get_option(self, instrument_name: str) -> Optional[VanillaOption]:
        """Get option by instrument name"""
        return self.options.get(instrument_name)

    def list_options(self) -> List[VanillaOption]:
        """Get list of all options in portfolio"""
        return list(self.options.values())

    def update_option_quantity(self, instrument_name: str, new_quantity: float):
        """Update option quantity"""
        if instrument_name in self.options:
            self.options[instrument_name].quantity = new_quantity
            self._total_delta = None  # Reset cached delta

class OptionModel:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.bs_model = BlackScholesModel()

    def calculate_portfolio_delta(self, current_price: float,
                                volatility: float,
                                risk_free_rate: float = 0.0) -> float:
        """Calculate total portfolio delta"""
        if self.portfolio._total_delta is not None:
            return self.portfolio._total_delta

        total_delta = 0.0
        current_time = datetime.now()

        for option in self.portfolio.list_options():
            # Calculate time to expiry in years
            T = (option.expiry - current_time).total_seconds() / (365 * 24 * 3600)

            # Skip expired options
            if T <= 0:
                continue

            # Calculate option delta
            d1 = self.bs_model.calculate_d1(
                S=current_price,
                K=option.strike,
                T=T,
                r=risk_free_rate,
                sigma=volatility
            )

            option_delta = self.bs_model.calculate_delta(option.option_type, d1)
            total_delta += option_delta * option.quantity

        self.portfolio._total_delta = total_delta
        return total_delta

    def calculate_required_hedge(self, current_price: float,
                               volatility: float,
                               target_delta: float = 0.0,
                               risk_free_rate: float = 0.0) -> float:
        """
        Calculate required hedge position to achieve target delta
        Returns the required position in the underlying (positive for long, negative for short)
        """
        portfolio_delta = self.calculate_portfolio_delta(
            current_price=current_price,
            volatility=volatility,
            risk_free_rate=risk_free_rate
        )

        # Required hedge is the negative of portfolio delta plus target delta
        required_hedge = target_delta - portfolio_delta
        return required_hedge
