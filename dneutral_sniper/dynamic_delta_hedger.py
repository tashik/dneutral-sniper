from dataclasses import dataclass
from typing import Optional, Dict
import time
import logging
import asyncio
from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.options import OptionModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HedgerConfig:
    ddh_min_trigger_delta: float
    ddh_target_delta: float
    ddh_step_mode: str
    ddh_step_size: float
    price_check_interval: float = 2.0  # seconds
    instrument_name: str = "BTC-PERPETUAL"
    volatility: float = 0.8  # Default annualized volatility for BTC
    risk_free_rate: float = 0.0  # Default risk-free rate
    min_contract_usd: float = 10.0  # Minimum USD notional for a hedge order

class DynamicDeltaHedger:
    def __init__(self, config: HedgerConfig, portfolio: Portfolio, deribit_client: DeribitWebsocketClient):
        self.config = config
        self.portfolio = portfolio
        self.deribit_client = deribit_client
        self.price_last = getattr(portfolio, 'last_hedge_price', None)
        self.ddh_enabled: bool = False
        self.ddh_pending: bool = False
        self.cur_delta: Optional[float] = None
        self.target_delta: Optional[float] = self.config.ddh_target_delta if self.config.ddh_target_delta is not None else 0.0

        # Initialize option model with deribit_client for mark price lookup
        self.option_model = OptionModel(self.portfolio, deribit_client=self.deribit_client)

        # Initialize Deribit client
        self.deribit_client = deribit_client
        self.price_lock = asyncio.Lock()
        self.current_price: Optional[float] = None
        self.deribit_client.set_price_callback(self._price_callback)

    async def start(self):
        """Start the dynamic delta hedger"""
        logger.info("DynamicDeltaHedger.start() called. Starting hedger...")
        self.ddh_enabled = True
        # await self.deribit_client.connect()
        # await self.deribit_client.subscribe_to_ticker(self.config.instrument_name)

        # Start only the hedging loop; websocket listeners are started by deribit_client.connect()
        await self._run_hedging_loop()
        logger.info("DynamicDeltaHedger.start() exited. (This should not happen unless stopped.)")

    async def stop(self):
        """Stop the dynamic delta hedger"""
        self.ddh_enabled = False
        await self.deribit_client.close()

    def _price_callback(self, instrument_name: str, price: float):
        """Callback for price updates from Deribit"""
        if instrument_name == self.config.instrument_name:
            asyncio.create_task(self._update_price(price))

    async def _update_price(self, price: float):
        """Update current price with thread safety"""
        async with self.price_lock:
            self.current_price = price

    async def _run_hedging_loop(self):
        """Main hedging loop"""
        while self.ddh_enabled:
            try:
                await self._process_hedging_cycle()
                await asyncio.sleep(self.config.price_check_interval)
            except Exception as e:
                logger.error(f"Error in hedging loop: {e}")

    async def _process_hedging_cycle(self):
        """Process single hedging cycle"""
        current_price = await self._get_current_price()
        if current_price is None:
            return

        # Only perform delta hedging if initial USD hedge is done
        if not getattr(self.portfolio, 'initial_usd_hedged', False):
            logger.info("Initial USD notional hedge not completed, skipping dynamic delta hedging.")
            return

        if not self._should_process_hedge(current_price):
            return

        await self._calculate_and_update_delta()
        await self._execute_hedge_if_needed()

    def _should_process_hedge(self, current_price: float) -> bool:
        """Check if we should process hedging based on price changes and other conditions"""
        if self.price_last is None or current_price == self.price_last:
            return False

        if self.config.ddh_step_mode == "absolute":
            price_change = abs(current_price - self.price_last)
        else:
            price_change = abs(current_price - self.price_last) / self.price_last
        should_hedge = price_change >= self.config.ddh_step_size

        logger.info(f"Price change: {price_change}, should hedge: {should_hedge}")

        return should_hedge

    async def _calculate_and_update_delta(self):
        """Calculate and update current net delta position"""
        logger.info("Calculating portfolio net delta (Deribit style)...")
        current_price = await self._get_current_price()
        if current_price is None:
            logger.warning("Current price is None, skipping delta calculation.")
            return
        self.cur_delta = await self.option_model.calculate_portfolio_net_delta(
            current_price=current_price,
            volatility=self.config.volatility,
            risk_free_rate=self.config.risk_free_rate
        )

    async def _execute_hedge_if_needed(self):
        """Execute hedging if net delta difference exceeds threshold"""
        if self.cur_delta is None or self.target_delta is None:
            return

        hedge_price = await self._get_current_price()
        if hedge_price is None:
            logger.warning("Cannot execute hedge order: current price is None.")
            return
        # Net delta is already calculated and stored in self.cur_delta
        required_hedge = self.target_delta - self.cur_delta
        logger.info(f"[HEDGE DECISION] cur_delta={self.cur_delta}, target_delta={self.target_delta}, required_hedge={required_hedge}")

        if abs(required_hedge) >= self.config.ddh_min_trigger_delta:
            await self._execute_hedge_order(required_hedge)
        else:
            logger.info(f"Required net delta hedge {required_hedge} is less than min_trigger_delta {self.config.ddh_min_trigger_delta}, skipping hedge.")

    async def _execute_hedge_order(self, required_hedge: float):
        """Execute the actual hedging order (paper trading for futures)"""
        price = await self._get_current_price()
        if price is None:
            logger.warning("Cannot execute hedge order: current price is None.")
            return
        if required_hedge == 0:
            logger.info("No hedge required.")
            return
        import math
        usd_qty = required_hedge * price  # Convert BTC delta to USD notional
        sign = 1 if usd_qty >= 0 else -1
        logger.info(f"[HEDGE ORDER] required_hedge={required_hedge}, price={price}, usd_qty={usd_qty}, sign={'BUY' if sign > 0 else 'SELL'}")
        if abs(usd_qty) < self.config.min_contract_usd:
            logger.info(f"Required hedge USD notional {usd_qty:.2f} is less than min contract {self.config.min_contract_usd}, skipping order.")
            return
        # Round order to nearest lower multiple of min_contract_usd (hedge down)
        orig_usd_qty = usd_qty
        usd_qty = sign * math.floor(abs(usd_qty) / self.config.min_contract_usd) * self.config.min_contract_usd
        logger.info(f"Rounded hedge order: requested USD qty={orig_usd_qty:.2f}, rounded to {usd_qty:.2f} (step={self.config.min_contract_usd})")
        # Update portfolio's USD notional futures position and avg entry
        self.portfolio.update_futures_position(usd_qty, price)
        self.portfolio.last_hedge_price = price
        self.price_last = price
        self.portfolio.save_to_file('portfolio.json')
        # Log net delta after hedge
        net_delta = await self.option_model.calculate_portfolio_net_delta(
            current_price=price,
            volatility=self.config.volatility,
            risk_free_rate=self.config.risk_free_rate
        )
        logger.info(f"Net portfolio delta after hedge (Deribit style): {net_delta:.6f} BTC")
        logger.info(
            f"Executed paper hedge order: USD qty={usd_qty:.2f}, price={price}, "
            f"new USD futures position={self.portfolio.futures_position:.2f}, "
            f"avg entry={self.portfolio.futures_avg_entry}, "
            f"realized PNL={self.portfolio.realized_pnl:.2f}"
        )
        self.portfolio.save_to_file('portfolio.json')

    async def _get_current_price(self) -> Optional[float]:
        """Get current price from the market"""
        async with self.price_lock:
            return self.current_price
