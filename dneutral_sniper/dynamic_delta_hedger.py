from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
import time
import logging
import asyncio
import math
from decimal import Decimal, ROUND_HALF_UP

from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.options import OptionModel
from dneutral_sniper.models import ContractType, OptionType, VanillaOption

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HedgerConfig:
    """Configuration for the DynamicDeltaHedger.

    Attributes:
        ddh_min_trigger_delta: Minimum delta difference to trigger a hedge (in BTC)
        ddh_target_delta: Target delta for the portfolio (in BTC)
        ddh_step_mode: Either 'absolute' or 'percentage' for step size calculation
        ddh_step_size: Size of price move to trigger a hedge (absolute or percentage)
        price_check_interval: How often to check prices (in seconds)
        underlying: The underlying asset (e.g., 'BTC' or 'ETH')
        instrument_name: The futures instrument to use for hedging (e.g., 'BTC-PERPETUAL')
        volatility: Default annualized volatility (used as fallback)
        risk_free_rate: Risk-free rate for options pricing
        min_hedge_usd: Minimum USD notional for a hedge order
    """
    ddh_min_trigger_delta: float
    ddh_target_delta: float
    ddh_step_mode: str
    ddh_step_size: float
    price_check_interval: float = 2.0  # seconds
    underlying: str = "BTC"
    instrument_name: str = "BTC-PERPETUAL"
    volatility: float = 0.8  # Default annualized volatility for BTC
    risk_free_rate: float = 0.0  # Default risk-free rate
    min_hedge_usd: float = 10.0  # Minimum USD notional for a hedge order

class DynamicDeltaHedger:
    """Dynamic delta hedger that maintains delta neutrality for a portfolio of options.

    The hedger works with both inverse (BTC-settled) and standard (USD-settled) options,
    and maintains all PNL calculations in USD.
    """

    def __init__(self, config: HedgerConfig, portfolio: Portfolio, deribit_client: DeribitWebsocketClient):
        """Initialize the dynamic delta hedger.

        Args:
            config: Hedger configuration
            portfolio: The portfolio to hedge
            deribit_client: Initialized Deribit client
        """
        self.config = config
        self.portfolio = portfolio
        self.deribit_client = deribit_client
        self.price_last = getattr(portfolio, 'last_hedge_price', None)

        # State tracking
        self.ddh_enabled: bool = False
        self.ddh_pending: bool = False
        self.cur_delta: Optional[float] = None
        self.target_delta: float = self.config.ddh_target_delta if self.config.ddh_target_delta is not None else 0.0
        self.last_hedge_time: Optional[float] = None
        self.hedge_count: int = 0

        # Initialize option model with deribit_client for mark price lookup
        self.option_model = OptionModel(self.portfolio, deribit_client=self.deribit_client)

        # Price tracking
        self.price_lock = asyncio.Lock()
        self.current_price: Optional[float] = None
        self.deribit_client.set_price_callback(self._price_callback)

        # Statistics
        self.stats = {
            'total_hedges': 0,
            'last_hedge_time': 0.0,
            'total_usd_hedged': 0.0,
            'total_btc_hedged': 0.0,
        }

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
        """Check if we should process hedging based on price changes and other conditions.

        Args:
            current_price: Current price of the underlying

        Returns:
            bool: True if we should proceed with hedging, False otherwise
        """
        if self.price_last is None:
            logger.info("No previous price available, processing initial hedge.")
            return True

        if current_price == self.price_last:
            logger.debug("Price unchanged, skipping hedge check.")
            return False

        # Calculate price change based on step mode
        if self.config.ddh_step_mode == "absolute":
            price_change = abs(current_price - self.price_last)
            threshold = self.config.ddh_step_size
        else:  # percentage
            price_change_pct = abs(current_price - self.price_last) / self.price_last
            price_change = price_change_pct * 100  # Convert to percentage
            threshold = self.config.ddh_step_size * 100  # Config is in decimal

        # Check if we should hedge based on price movement
        should_hedge = price_change >= threshold

        # Also consider time-based hedging if we haven't hedged in a while
        time_based_hedge = None
        if not self.last_hedge_time:
            time_since_last_hedge = time.time() - (self.last_hedge_time or 0)
            time_based_hedge = time_since_last_hedge > (self.config.price_check_interval * 5)

        if should_hedge or time_based_hedge:
            logger.info(
                f"Price change: {price_change:.6f} {'$' if self.config.ddh_step_mode == 'absolute' else '%'} "
                f"(threshold: {threshold:.6f}), "
                f"Hedging: {'YES' if should_hedge else 'TIME_BASED' if time_based_hedge else 'NO'}"
            )
        else:
            logger.info(f"Price change: {price_change:.2f} {'$' if self.config.ddh_step_mode == 'absolute' else '%'} (should_hedge: {should_hedge}, time_based_ hedge: {time_based_hedge})")
        return should_hedge or time_based_hedge

    async def _calculate_and_update_delta(self) -> None:
        """Calculate and update current net delta position.

        This calculates the portfolio's net delta in BTC, accounting for both
        inverse and standard options, as well as any existing futures positions.
        """
        current_price = await self._get_current_price()
        if current_price is None:
            logger.warning("Current price is None, skipping delta calculation.")
            return

        logger.info("Calculating portfolio net delta...")

        # Calculate net delta in BTC using IVs from Deribit
        self.cur_delta = await self.option_model.calculate_portfolio_net_delta(
            current_price=current_price,
            volatility=self.config.volatility,
            risk_free_rate=self.config.risk_free_rate
        )

        # Also calculate and log USD PNL
        usd_pnl = await self.option_model.calculate_portfolio_usd_pnl(current_price)
        logger.info(
            f"Portfolio delta: {self.cur_delta:.6f} BTC, "
            f"USD PNL: ${usd_pnl:,.2f}, "
            f"Price: ${current_price:,.2f}"
        )

        return self.cur_delta

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
            if not self.last_hedge_time:
                self.last_hedge_time = time.time()
            logger.info(f"Required net delta hedge {required_hedge} is less than min_trigger_delta {self.config.ddh_min_trigger_delta}, skipping hedge.")

    async def _execute_hedge_order(self, required_hedge: float) -> None:
        """Execute a hedge order to adjust portfolio delta.

        Args:
            required_hedge: The amount of delta to hedge, in BTC. Positive means buy BTC,
                          negative means sell BTC.
        """
        current_price = await self._get_current_price()
        if current_price is None:
            logger.warning("Cannot execute hedge order: current price is None.")
            return

        if abs(required_hedge) < 1e-8:  # Near zero
            logger.info("No hedge required (delta is effectively zero).")
            return

        # Calculate USD notional to trade
        # required_hedge = target_delta - cur_delta
        # If cur_delta > target_delta, we need to sell BTC (go short) to reduce delta
        # If cur_delta < target_delta, we need to buy BTC (go long) to increase delta
        usd_qty = required_hedge * current_price

        # Round to avoid tiny orders and ensure we meet minimums
        sign = 1 if usd_qty >= 0 else -1
        abs_usd_qty = abs(usd_qty)

        # Apply minimum notional check
        if abs_usd_qty < self.config.min_hedge_usd:
            logger.info(
                f"Hedge notional ${abs_usd_qty:.2f} below minimum ${self.config.min_hedge_usd}, "
                f"skipping hedge of {required_hedge:.6f} BTC delta"
            )
            if not self.last_hedge_time:
                # Still update the last_hedge_time to prevent immediate re-hedging
                self.last_hedge_time = time.time()
                return

        # Round down to nearest min_hedge_usd to avoid odd lot sizes
        # Using math.floor to always round down to the nearest multiple of min_hedge_usd
        rounded_usd_qty = sign * (math.floor(abs_usd_qty / self.config.min_hedge_usd) * self.config.min_hedge_usd)

        logger.info(
            f"Executing hedge: {'BUY' if rounded_usd_qty > 0 else 'SELL'} "
            f"${abs(rounded_usd_qty):.2f} notional at ${current_price:.2f} "
            f"to hedge {required_hedge:.6f} BTC delta"
        )

        try:
            # Update portfolio's futures position (paper trading)
            self.portfolio.update_futures_position(rounded_usd_qty, current_price)
            self.price_last = current_price

            # Update stats
            self.stats['total_hedges'] += 1
            self.stats['last_hedge_time'] = time.time()
            self.stats['total_usd_hedged'] += abs(rounded_usd_qty)
            self.stats['total_btc_hedged'] += abs(rounded_usd_qty / current_price)

            # Recalculate and update the portfolio delta
            self.cur_delta = await self.option_model.calculate_portfolio_net_delta(
                current_price=current_price,
                volatility=self.config.volatility,
                risk_free_rate=self.config.risk_free_rate
            )

            # Log hedge execution details
            logger.info(
                f"Hedge executed: {'BOUGHT' if rounded_usd_qty > 0 else 'SOLD'} "
                f"${abs(rounded_usd_qty):.2f} at ${current_price:.2f}\n"
                f"New futures position: ${self.portfolio.futures_position:,.2f} "
                f"(avg ${self.portfolio.futures_avg_entry:,.2f})\n"
                f"Updated portfolio delta: {self.cur_delta:.6f} BTC, "
                f"Realized PNL: ${self.portfolio.realized_pnl:,.2f}"
            )

            # Save updated portfolio state
            self.portfolio.save_to_file('portfolio.json')

            self.hedge_count += 1
            self.last_hedge_time = time.time()

        except Exception as e:
            logger.error(f"Error executing hedge order: {e}", exc_info=True)
            raise

    async def _get_current_price(self) -> Optional[float]:
        """Get current price from the market"""
        async with self.price_lock:
            return self.current_price
