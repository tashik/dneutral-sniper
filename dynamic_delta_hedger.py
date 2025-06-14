from dataclasses import dataclass
from typing import Optional, Dict
import time
import logging
import asyncio
from deribit_client import DeribitWebsocketClient, DeribitCredentials
from options import Portfolio, OptionModel

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

class DynamicDeltaHedger:
    def __init__(self, config: HedgerConfig, credentials: Optional[DeribitCredentials] = None):
        self.config = config
        self.ddh_enabled: bool = False
        self.ddh_pending: bool = False
        self.price_last: Optional[float] = None
        self.cur_delta: Optional[float] = None
        self.target_delta: Optional[float] = None

        # Initialize portfolio and option model
        self.portfolio = Portfolio()
        self.option_model = OptionModel(self.portfolio)

        # Initialize Deribit client
        self.deribit_client = DeribitWebsocketClient(credentials)
        self.price_lock = asyncio.Lock()
        self.current_price: Optional[float] = None

    async def start(self):
        """Start the dynamic delta hedger"""
        self.ddh_enabled = True
        await self.deribit_client.connect()
        self.deribit_client.set_price_callback(self._price_callback)
        await self.deribit_client.subscribe_to_ticker(self.config.instrument_name)

        # Start price feed listener and hedging loop concurrently
        await asyncio.gather(
            self.deribit_client.listen(),
            self._run_hedging_loop()
        )

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

        if not self._should_process_hedge(current_price):
            return

        await self._calculate_and_update_delta()
        await self._execute_hedge_if_needed()

    def _should_process_hedge(self, current_price: float) -> bool:
        """Check if we should process hedging based on price changes and other conditions"""
        if self.price_last is None:
            self.price_last = current_price
            return False

        price_change = abs(current_price - self.price_last) / self.price_last
        should_hedge = price_change >= self.config.ddh_step_size

        if should_hedge:
            self.price_last = current_price

        return should_hedge

    async def _calculate_and_update_delta(self):
        """Calculate and update current delta position"""
        current_price = await self._get_current_price()
        if current_price is None:
            return

        self.cur_delta = self.option_model.calculate_portfolio_delta(
            current_price=current_price,
            volatility=self.config.volatility,
            risk_free_rate=self.config.risk_free_rate
        )

    async def _execute_hedge_if_needed(self):
        """Execute hedging if delta difference exceeds threshold"""
        if not self.cur_delta or not self.target_delta:
            return

        required_hedge = self.option_model.calculate_required_hedge(
            current_price=await self._get_current_price(),
            volatility=self.config.volatility,
            target_delta=self.config.ddh_target_delta,
            risk_free_rate=self.config.risk_free_rate
        )

        if abs(required_hedge) >= self.config.ddh_min_trigger_delta:
            await self._execute_hedge_order(required_hedge)

    async def _execute_hedge_order(self, required_hedge: float):
        """Execute the actual hedging order"""
        # TODO: Implement order execution via Deribit client
        logger.info(f"Executing hedge order for {required_hedge} contracts")
        pass

    async def _get_current_price(self) -> Optional[float]:
        """Get current price from the market"""
        async with self.price_lock:
            return self.current_price
