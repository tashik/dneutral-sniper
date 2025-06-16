from dataclasses import dataclass
from typing import Optional, Dict, Tuple
import numpy as np
from datetime import datetime
from scipy.stats import norm
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.models import OptionType, VanillaOption, ContractType
import logging
from decimal import Decimal, ROUND_HALF_UP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class OptionModel:
    def __init__(self, portfolio: Portfolio, deribit_client=None):
        self.portfolio = portfolio
        self.bs_model = BlackScholesModel()
        self.deribit_client = deribit_client  # For mark price lookup

    async def _get_mark_price_and_iv(self, option: VanillaOption) -> Tuple[float, float]:
        """Get mark price and IV for an option, with fallback to cache or config."""
        mark_price = iv = None

        # First try to get from deribit client
        if self.deribit_client is not None:
            mark_price, iv = self.deribit_client.get_price_iv(option.instrument_name)
            # If cache miss, try async fetch
            if mark_price is None or iv is None:
                mark_price, iv = await self.deribit_client.get_instrument_mark_price_and_iv(option.instrument_name)

        # Fallback to option's cached values if available
        if mark_price is None and option.mark_price is not None:
            mark_price = option.mark_price
        if iv is None and option.iv is not None:
            iv = option.iv

        return mark_price or 0.0, iv or 0.0

    async def _calculate_option_delta(
        self,
        option: VanillaOption,
        current_price: float,
        volatility: float,
        risk_free_rate: float,
        time_to_expiry: float
    ) -> float:
        """Calculate delta for a single option.

        For inverse options (BTC-settled), the delta is (black_scholes_delta - mark_price).
        For standard options, it's the standard Black-Scholes delta.
        """
        if time_to_expiry <= 0:
            return 0.0

        # Get IV from Deribit
        mark_price, iv = await self._get_mark_price_and_iv(option)

        d1 = self.bs_model.calculate_d1(
            S=current_price,
            K=option.strike,
            T=time_to_expiry,
            r=risk_free_rate,
            sigma=iv or volatility  # Fallback IV if not available
        )
        # Calculate standard Black-Scholes delta
        bs_delta = self.bs_model.calculate_delta(option.option_type, d1)

        # For inverse options, adjust delta by subtracting mark price
        if option.contract_type == ContractType.INVERSE:
            delta = bs_delta - mark_price
        else:
            delta = bs_delta
        return delta

    def _calculate_option_usd_value(
        self,
        option: VanillaOption,
        current_price: float,
        mark_price: float,
        delta: float
    ) -> float:
        """Calculate USD value of an option position."""
        if option.contract_type == ContractType.INVERSE:
            # For inverse options, value is in BTC, so convert to USD
            return mark_price * option.quantity * current_price
        else:
            # For standard options, value is already in USD
            return mark_price * option.quantity

    async def calculate_portfolio_net_delta(self, current_price: float, volatility: float, risk_free_rate: float = 0.0, include_static_hedge: bool = False) -> float:
        """
        Calculate true portfolio net delta in BTC.

        For inverse options (BTC-settled):
        - Delta is in BTC per contract
        - Value is in BTC, needs to be converted to USD when needed

        For standard options (USD-settled):
        - Delta is in USD per contract
        - Value is already in USD, needs to be converted to BTC for net delta

        Args:
            current_price: Current price of the underlying
            volatility: Annualized volatility
            risk_free_rate: Risk-free interest rate
            include_static_hedge: Whether to include the static USD hedge in the delta calculation.
                                Should be False for dynamic hedging to avoid double-counting.

        Returns:
            float: Net delta in BTC
        """
        from datetime import datetime
        current_time = datetime.now()
        options = self.portfolio.list_options()

        total_net_delta_btc = 0.0
        total_usd_value = 0.0

        for option in options:
            time_to_expiry = (option.expiry - current_time).total_seconds() / (365 * 24 * 3600)

            # Get mark price and IV
            mark_price, iv = await self._get_mark_price_and_iv(option)
            option.mark_price = mark_price
            option.iv = iv or volatility  # Use provided volatility as fallback

            # Calculate delta using IV from Deribit
            delta = await self._calculate_option_delta(
                option=option,
                current_price=current_price,
                volatility=volatility,
                risk_free_rate=risk_free_rate,
                time_to_expiry=time_to_expiry
            )

            # Calculate USD value and store it
            usd_value = self._calculate_option_usd_value(option, current_price, mark_price, delta)
            option.usd_value = usd_value
            option.delta = delta
            option._greeks_calculated = True

            # Calculate position delta in BTC
            if option.contract_type == ContractType.INVERSE:
                # For inverse options, delta is already in BTC
                position_delta_btc = delta * option.quantity
            else:
                # For standard options, convert USD delta to BTC delta
                position_delta_btc = (delta * option.quantity) / current_price if current_price > 0 else 0

            total_net_delta_btc += position_delta_btc
            total_usd_value += usd_value

            logger.info(
                f"Option {option.instrument_name} ({option.contract_type.value}): "
                f"type={option.option_type.value}, qty={option.quantity}, "
                f"delta={delta:.6f}, mark_price={mark_price:.6f}, "
                f"usd_value={usd_value:.2f}, position_delta_btc={position_delta_btc:.6f}"
            )

        # Add dynamic futures hedge (already in BTC)
        dynamic_hedge_btc = getattr(self.portfolio, 'futures_position', 0.0) / current_price if current_price > 0 else 0.0
        total_net_delta_btc += dynamic_hedge_btc

        # Optionally add static hedge if requested
        if include_static_hedge:
            static_hedge_btc = getattr(self.portfolio, 'initial_usd_hedge_position', 0.0) / current_price if current_price > 0 else 0.0
            total_net_delta_btc += static_hedge_btc
            logger.info(f"Including static hedge in delta: {static_hedge_btc:.6f} BTC")
        
        logger.info(f"Dynamic futures hedge (BTC): {dynamic_hedge_btc:.6f}")
        logger.info(f"Portfolio net delta (BTC): {total_net_delta_btc:.6f}")

        return total_net_delta_btc

    async def calculate_portfolio_usd_pnl(self, current_price: float) -> float:
        """
        Calculate total portfolio PNL in USD.

        For inverse options (BTC-settled):
        - Value is in BTC, needs to be converted to USD using current price

        For standard options (USD-settled):
        - Value is already in USD

        Returns:
            float: Total PNL in USD
        """
        total_pnl_usd = 0.0

        # Calculate PNL from options
        for option in self.portfolio.list_options():
            mark_price, _ = await self._get_mark_price_and_iv(option)
            if mark_price is None:
                continue

            # Get initial USD value if available
            initial_value = self.portfolio.initial_option_usd_value.get(option.instrument_name, 0.0)

            # Calculate current USD value
            if option.contract_type == ContractType.INVERSE:
                # For inverse options, value is in BTC, convert to USD
                current_value = mark_price * option.quantity * current_price
            else:
                # For standard options, value is already in USD
                current_value = mark_price * option.quantity

            # Calculate PNL (current value - initial value)
            pnl = current_value - initial_value
            total_pnl_usd += pnl

            logger.debug(
                f"Option {option.instrument_name} PNL: "
                f"initial=${initial_value:.2f}, current=${current_value:.2f}, "
                f"PNL=${pnl:.2f}"
            )

        # Calculate PNL from futures positions
        futures_position = getattr(self.portfolio, 'futures_position', 0.0)
        futures_avg_entry = getattr(self.portfolio, 'futures_avg_entry', 0.0)

        if futures_position != 0 and futures_avg_entry > 0:
            # PNL = (current_price - avg_entry) * position_size
            futures_pnl = (current_price - futures_avg_entry) * futures_position / futures_avg_entry
            total_pnl_usd += futures_pnl
            logger.debug(f"Futures PNL: ${futures_pnl:.2f}")

        logger.info(f"Total portfolio PNL: ${total_pnl_usd:.2f}")
        return total_pnl_usd
