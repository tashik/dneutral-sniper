from dataclasses import dataclass
from typing import Optional
from enum import Enum
import numpy as np
from datetime import datetime
from scipy.stats import norm
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.models import OptionType, VanillaOption
import logging

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

    async def calculate_portfolio_net_delta(self, current_price: float, volatility: float, risk_free_rate: float = 0.0) -> float:
        """Calculate true portfolio net delta: sum of option net deltas (Deribit style), plus static and dynamic futures hedges (all in BTC)."""
        from datetime import datetime
        total_net_delta = 0.0
        current_time = datetime.now()
        options = self.portfolio.list_options()
        call_raw_delta = 0.0
        put_raw_delta = 0.0
        call_mark_total = 0.0
        put_mark_total = 0.0
        call_qty = 0.0
        put_qty = 0.0
        for option in options:
            T = (option.expiry - current_time).total_seconds() / (365 * 24 * 3600)
            if T <= 0:
                mark_price = 0.0
            else:
                if self.deribit_client is not None:
                    mark_price, iv = self.deribit_client.get_price_iv(option.instrument_name)
                    # If cache miss, optionally fallback to request (async fetch)
                    if mark_price is None or iv is None:
                        mark_price, iv = await self.deribit_client.get_instrument_mark_price_and_iv(option.instrument_name)
                    if mark_price is None:
                        mark_price = 0.0
                    if iv is None:
                        iv = 0.0
                else:
                    mark_price = 0.0
                    iv = 0.0
            option.mark_price = mark_price
            if mark_price == 0.0:
                logger.warning(f"Mark price for {option.instrument_name} is 0.0. Delta calculation may be inaccurate.")

            # If IV is zero or missing (e.g., for futures), fallback to config volatility for options
            if iv == 0.0 and option.option_type is not None:
                iv = volatility
                logger.info(f"IV for {option.instrument_name} unavailable, using fallback: {iv:.4f}")
            else:
                logger.info(f"Using market IV for {option.instrument_name}: {iv:.4f}")

            # Calculate option delta
            d1 = self.bs_model.calculate_d1(
                S=current_price,
                K=option.strike,
                T=T,
                r=risk_free_rate,
                sigma=iv
            )

            option_delta = self.bs_model.calculate_delta(option.option_type, d1)
            # Deribit-style net delta: (delta * qty) - (mark_price * qty)
            net_delta = option_delta * option.quantity - mark_price * option.quantity
            option_type_str = "CALL" if option.option_type == OptionType.CALL else "PUT"
            logger.info(f"Option {option.instrument_name}: type={option_type_str}, qty={option.quantity}, delta={option_delta:.6f}, mark_price={mark_price:.6f}, net_delta={net_delta:.6f}, IV={iv:.4f}")
            if option.option_type == OptionType.CALL:
                call_raw_delta += option_delta * option.quantity
                call_mark_total += mark_price * option.quantity
                call_qty += option.quantity
            else:
                put_raw_delta += option_delta * option.quantity
                put_mark_total += mark_price * option.quantity
                put_qty += option.quantity
            total_net_delta += net_delta
        # Log total call/put raw deltas and mark prices
        logger.info(f"Total call delta: {call_raw_delta:.6f}, mark sum: {call_mark_total:.6f}, qty: {call_qty}")
        logger.info(f"Total put delta: {put_raw_delta:.6f}, mark sum: {put_mark_total:.6f}, qty: {put_qty}")
        # Add static and dynamic futures hedges (USD notional / current price)
        static_hedge_btc = getattr(self.portfolio, 'initial_usd_hedge_position', 0.0) / current_price if current_price > 0 else 0.0
        dynamic_hedge_btc = getattr(self.portfolio, 'futures_position', 0.0) / current_price if current_price > 0 else 0.0
        logger.info(f"Static USD hedge (BTC): {static_hedge_btc:.6f}, Dynamic futures hedge (BTC): {dynamic_hedge_btc:.6f}")
        total_net_delta += static_hedge_btc + dynamic_hedge_btc
        logger.info(f"Portfolio net delta (Deribit style): {total_net_delta:.6f} BTC")
        return total_net_delta
