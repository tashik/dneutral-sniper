from dataclasses import dataclass
from typing import Optional, Any, Callable, Dict, List
import time
import logging
import asyncio
import math

from dneutral_sniper.deribit_client import DeribitWebsocketClient
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.options import OptionModel

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

    def __init__(self, config: HedgerConfig, portfolio: Portfolio, deribit_client: Optional[Any] = None):
        """Initialize the dynamic delta hedger.

        Note: The deribit_client parameter is kept for backward compatibility but will be removed in a future version.
        Price updates should be provided through the _price_callback method.

        Args:
            config: Hedger configuration
            portfolio: The portfolio to hedge
            deribit_client: [DEPRECATED] Do not use. Will be removed in a future version.
        """
        self.config = config
        self.portfolio = portfolio
        self.price_last = getattr(portfolio, 'last_hedge_price', None)

        # State tracking
        self.ddh_enabled: bool = False
        self.ddh_pending: bool = False
        self.cur_delta: Optional[float] = None
        self.target_delta: float = self.config.ddh_target_delta if self.config.ddh_target_delta is not None else 0.0
        self.last_hedge_time: Optional[float] = None
        self.hedge_count: int = 0
        self._stop_event = asyncio.Event()
        self._price_update_event = asyncio.Event()
        self._last_price_update = 0.0

        # Initialize option model with deribit_client for mark price lookup
        # We'll set up the option model with None deribit_client since we'll handle price lookups ourselves
        self.option_model = OptionModel(self.portfolio, deribit_client=None)

        self.deribit_client = deribit_client
        self.price_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self.current_price: Optional[float] = None
        self.last_hedge_time: Optional[float] = None
        
        # Only set price callback if deribit_client is provided and has the method
        if self.deribit_client is not None and hasattr(self.deribit_client, 'set_price_callback'):
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
        if self.ddh_enabled:
            return

        logger.info("Starting dynamic delta hedger...")
        self.ddh_enabled = True
        self._stop_event.clear()

        # Create and store the task
        self._hedging_task = asyncio.create_task(self._run_hedging_loop())

        # Add a callback to handle task completion/cancellation
        def on_task_done(task):
            try:
                # Check if the task completed with an exception
                task.result()
            except asyncio.CancelledError:
                logger.debug("Hedging task was cancelled")
            except Exception as e:
                logger.error(f"Hedging task failed: {e}", exc_info=True)
            finally:
                self.ddh_enabled = False

        self._hedging_task.add_done_callback(on_task_done)
        logger.info("DynamicDeltaHedger.start() exited. (This should not happen unless stopped.)")

    async def stop(self):
        """Stop the dynamic delta hedger

        This will signal the hedging loop to stop and wait for it to complete.
        """
        if not hasattr(self, '_hedging_task') or not self.ddh_enabled:
            return

        logger.info("Stopping dynamic delta hedger...")
        self.ddh_enabled = False

        # Signal all tasks to stop
        self._stop_event.set()
        self._price_update_event.set()  # Wake up any waiting tasks

        # Cancel the hedging task if it's still running
        if not self._hedging_task.done():
            self._hedging_task.cancel()
            try:
                # Wait for the task to complete with a timeout
                await asyncio.wait_for(self._hedging_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                # Task was cancelled or timed out, which is expected
                pass
            except Exception as e:
                logger.error(f"Error during hedger shutdown: {e}", exc_info=True)

            # Ensure the task is done
            if not self._hedging_task.done():
                logger.warning("Hedging task did not complete cleanly, forcing cancellation")
                self._hedging_task.cancel()

        # Clean up the task reference
        if hasattr(self, '_hedging_task'):
            del self._hedging_task

    async def _price_callback(self, instrument_name: str, price: float):
        """Callback for price updates from the HedgingManager.

        This method is called by the HedgingManager when a price update is received
        for an instrument this hedger is interested in.

        Args:
            instrument_name: Name of the instrument that was updated
            price: The new price
        """
        try:
            # Process price updates for either the exact instrument or the base instrument
            # (e.g., accept both 'BTC-PERPETUAL' and 'BTC' for a BTC-PERPETUAL hedger)
            base_instrument = self.config.instrument_name.split('-')[0]
            if instrument_name not in (self.config.instrument_name, base_instrument):
                logger.debug(f"Ignoring price update for {instrument_name}, waiting for {self.config.instrument_name} or {base_instrument}")
                return

            logger.debug(f"Processing price update for {instrument_name}: {price}")
            # Update the current price with thread safety
            await self._update_price(price)
        except Exception as e:
            logger.error(f"Error in _price_callback for {instrument_name}: {e}", exc_info=True)

    async def _update_price(self, price: float):
        """Update current price with thread safety

        Args:
            price: The new price to set (must be int, float, or bool)

        Raises:
            TypeError: If price is not a number
        """
        # Validate input type
        if not isinstance(price, (int, float, bool)):
            raise TypeError(f"Price must be a number, got {type(price).__name__}")

        # Convert bool to int (True -> 1, False -> 0) since it's a subclass of int
        if isinstance(price, bool):
            price = float(int(price))
        else:
            price = float(price)

        async with self.price_lock:
            old_price = self.current_price
            self.current_price = price
            self._last_price_update = time.time()

            # Set the event to notify any waiting tasks
            self._price_update_event.set()

            # Log price update with change percentage if we had a previous price
            # if old_price is not None and old_price > 0:
            #     pct_change = ((price - old_price) / old_price) * 100
            #     logger.info(
            #         f"[PRICE_UPDATE] Updated price for {self.config.instrument_name}: "
            #         f"${old_price:.2f} -> ${price:.2f} ({pct_change:+.2f}%)"
            #     )
            # else:
            #     logger.info(
            #         f"[PRICE_UPDATE] Initial price set for {self.config.instrument_name}: "
            #         f"${price:.2f}"
            #     )

    async def _run_hedging_loop(self):
        """Main hedging loop"""
        logger.info("Hedging loop started")

        # Wait for initial price update with a timeout
        logger.info("Waiting for initial price update...")
        try:
            # Wait for the first price update
            initial_price = await asyncio.wait_for(
                self._get_current_price(),
                timeout=30.0  # 30 second timeout for initial price
            )
            if initial_price is None:
                logger.error("Failed to get initial price after timeout")
                return
            logger.info(f"Initial price received: ${initial_price:.2f}")
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for initial price update")
            return
        except Exception as e:
            logger.error(f"Error waiting for initial price: {e}", exc_info=True)
            return

        try:
            while not self._stop_event.is_set():
                try:
                    await self._process_hedging_cycle()

                    # Sleep for the configured interval, but check for stop more frequently
                    # to ensure timely shutdown
                    for _ in range(int(self.config.price_check_interval * 10)):
                        if self._stop_event.is_set():
                            break
                        await asyncio.sleep(0.1)

                except asyncio.CancelledError:
                    logger.info("Hedging loop cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error in hedging loop: {e}", exc_info=True)
                    # Don't re-raise, just continue the loop after a delay
                    await asyncio.sleep(5)  # Prevent tight loop on error

        except asyncio.CancelledError:
            logger.debug("Hedging loop task was cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in hedging loop: {e}", exc_info=True)
            raise
        finally:
            logger.info("Hedging loop stopped")

    async def _process_hedging_cycle(self):
        """Process single hedging cycle"""
        try:
            logger.info("Starting hedging cycle...")

            # Get current price with a reasonable timeout
            try:
                logger.debug("Getting current price...")
                current_price = await asyncio.wait_for(
                    self._get_current_price(),
                    timeout=10.0  # 10 second timeout for price check
                )
                if current_price is None:
                    logger.warning("Current price is None, skipping hedging cycle")
                    return
                logger.debug(f"Got current price: ${current_price:.2f}")
            except asyncio.TimeoutError:
                logger.warning("Timed out waiting for price update")
                return
            except Exception as e:
                logger.error(f"Error getting current price: {e}", exc_info=True)
                return

            # STATIC OPTION PREMIUM HEDGING
            await self._execute_initial_option_premium_hedge_if_needed(current_price)

            if self.ddh_pending:
                return

            # DYNAMIC DELTA HEDGING - Check if we should process a hedge
            should_hedge = self._should_process_hedge(current_price)

            if not should_hedge:
                return

            self.ddh_pending = True

            # Calculate and update current delta position
            try:
                await self._calculate_and_update_delta()
            except Exception as e:
                logger.error(f"Error calculating delta: {e}", exc_info=True)
                self.ddh_pending = False
                return

            # Execute hedging if needed
            try:
                await self._execute_hedge_if_needed()
            except Exception as e:
                logger.error(f"Error executing hedge: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.debug("Hedging cycle cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in hedging cycle: {e}", exc_info=True)

    def _should_process_hedge(self, current_price: float) -> bool:
        """Check if we should process hedging based on price changes and other conditions.

        Args:
            current_price: Current price of the underlying

        Returns:
            bool: True if we should proceed with hedging, False otherwise
        """
        logger.debug(f"_should_process_hedge called with price: {current_price}")

        if self.price_last is None:
            logger.info("No previous price available, processing initial hedge.")
            return True

        if current_price == self.price_last:
            logger.debug("Price unchanged, skipping hedge check.")
            return False

        logger.debug(f"Previous price: {self.price_last}, Current price: {current_price}")

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
        time_based_hedge = False
        if self.last_hedge_time is not None:
            time_since_last_hedge = time.time() - self.last_hedge_time
            time_based_hedge = time_since_last_hedge > 3600
        else:
            # If we've never hedged before, we should do an initial hedge
            time_based_hedge = True

        # Format the last hedge time for logging
        last_hedge_str = "never"
        if self.last_hedge_time is not None:
            last_hedge_str = f"{time.time() - self.last_hedge_time:.1f}s ago"

        # Log the decision with more context
        logger.info(
            f"Price change: {price_change:.6f} {'$' if self.config.ddh_step_mode == 'absolute' else '%'} "
            f"(threshold: {threshold:.6f}), "
            f"Last hedge: {last_hedge_str}, "
            f"Hedging: {'PRICE' if should_hedge else 'TIME' if time_based_hedge else 'NO'}, "
            f"Price: {current_price:.2f}, Last price: {self.price_last}"
        )

        return should_hedge or time_based_hedge

    async def _calculate_and_update_delta(self) -> None:
        """Calculate and update current net delta position.

        This calculates the portfolio's net delta in BTC, accounting for both
        inverse and standard options, as well as any existing futures positions.
        """
        logger.debug("Starting delta calculation...")
        current_price = await self._get_current_price()
        if current_price is None:
            logger.warning("Cannot calculate delta: current price is None")
            return
        logger.debug(f"Got current price for delta calc: ${current_price:.2f}")

        # Calculate option deltas
        try:
            logger.debug("Calculating option delta...")
            option_delta_btc = await self.option_model.calculate_portfolio_net_delta(
                current_price=current_price,
                volatility=self.config.volatility,
                risk_free_rate=self.config.risk_free_rate,
                include_static_hedge=False
            )
            logger.debug(f"Calculated option delta: {option_delta_btc:.6f} BTC")
        except Exception as e:
            logger.error(f"Error calculating option delta: {e}", exc_info=True)
            return

        # Get futures position delta in BTC
        # Convert from USD to BTC using current price
        futures_delta_btc = 0.0
        if hasattr(self.portfolio, '_futures_position') and self.portfolio._futures_position is not None:
            futures_delta_btc = self.portfolio._futures_position / current_price if current_price != 0 else 0.0
        logger.debug(f"Current futures position: {futures_delta_btc:.6f} BTC")

        # Calculate net delta in BTC
        net_delta_btc = option_delta_btc + futures_delta_btc

        # Update state
        self.cur_delta = net_delta_btc
        logger.info(
            f"Updated delta: {net_delta_btc:.6f} BTC "
            f"(Options: {option_delta_btc:.6f} BTC, "
            f"Futures: {futures_delta_btc:.6f} BTC)"
        )

        # Also calculate and log USD PNL
        usd_pnl = await self.option_model.calculate_portfolio_usd_pnl(current_price)
        logger.info(
            f"Portfolio delta: {self.cur_delta:.6f} BTC, "
            f"USD PNL: ${usd_pnl:,.2f}, "
            f"Price: ${current_price:,.2f}"
        )

        return self.cur_delta

    async def _get_current_price_with_timeout(self, timeout: float = 5.0) -> Optional[float]:
        """Get current price, waiting for an update if necessary.

        Args:
            timeout: Maximum time to wait for a price update in seconds

        Returns:
            The current price, or None if no price is available after the timeout
        """
        # If we already have a recent price, return it immediately
        current_time = time.time()
        if self.current_price is not None and (current_time - self._last_price_update) < 5.0:
            logger.debug(f"Using cached price for {self.config.instrument_name}: ${self.current_price:.2f}")
            return self.current_price

        # Log that we're waiting for a price update
        logger.info(
            f"[PRICE_GET] Waiting for price update for {self.config.instrument_name} "
            f"(timeout: {timeout}s)..."
        )

        # Otherwise, wait for a price update with timeout
        self._price_update_event.clear()
        try:
            # Wait for the price update event or timeout
            await asyncio.wait_for(self._price_update_event.wait(), timeout=timeout)

            # After the event is set, check if we have a valid price
            if self.current_price is not None:
                logger.debug(
                    f"[PRICE_GET] Received price update for {self.config.instrument_name}: "
                    f"${self.current_price:.2f}"
                )
                return self.current_price
            else:
                logger.warning(
                    f"[PRICE_GET] Price update event received but current_price is None for {self.config.instrument_name}"
                )
                return None

        except asyncio.TimeoutError:
            if self.current_price is not None:
                logger.warning(
                    f"[PRICE_GET] Timed out waiting for price update for {self.config.instrument_name}, "
                    f"using last known price: ${self.current_price:.2f} "
                    f"(age: {current_time - self._last_price_update:.1f}s)"
                )
                return self.current_price
            else:
                logger.error(
                    f"[PRICE_GET] No price available for {self.config.instrument_name} "
                    f"after waiting {timeout:.1f}s"
                )
                return None
        except Exception as e:
            logger.error(
                f"[PRICE_GET] Error getting price for {self.config.instrument_name}: {e}",
                exc_info=True
            )
            return None

    async def _execute_hedge_if_needed(self):
        """Execute hedging if net delta difference exceeds threshold"""
        if self.cur_delta is None or self.target_delta is None:
            self.ddh_pending = False
            return

        hedge_price = await self._get_current_price()
        if hedge_price is None:
            logger.warning("Cannot execute hedge order: current price is None.")
            self.ddh_pending = False
            return
        # Net delta is already calculated and stored in self.cur_delta
        required_hedge = self.target_delta - self.cur_delta
        logger.info(
            f"[HEDGE DECISION] cur_delta={self.cur_delta}, " +
            f"target_delta={self.target_delta}, required_hedge={required_hedge}"
        )

        if abs(required_hedge) >= self.config.ddh_min_trigger_delta:
            await self._execute_hedge_order(required_hedge)
        else:
            if not self.last_hedge_time:
                self.last_hedge_time = time.time()
            logger.info(
                f"Required net delta hedge {required_hedge} is less than " +
                f"min_trigger_delta {self.config.ddh_min_trigger_delta}, skipping hedge."
            )
            self.ddh_pending = False

    async def _execute_hedge_order(self, required_hedge: float) -> None:
        """Execute a hedge order to adjust portfolio delta.

        Args:
            required_hedge: The amount of delta to hedge, in BTC. Positive means buy BTC,
                          negative means sell BTC.
        """
        current_price = await self._get_current_price()
        if current_price is None:
            logger.warning("Cannot execute hedge order: current price is None.")
            self.ddh_pending = False
            return

        if abs(required_hedge) < 1e-8:  # Near zero
            logger.info("No hedge required (delta is effectively zero).")
            self.ddh_pending = False
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
                self.ddh_pending = False
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
            await self.portfolio.update_futures_position(rounded_usd_qty, current_price)
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

            # Mark portfolio as dirty to trigger save via event system
            await self.portfolio._mark_dirty()

            self.hedge_count += 1
            self.last_hedge_time = time.time()
            self.ddh_pending = False

        except Exception as e:
            logger.error(f"Error executing hedge order: {e}", exc_info=True)
            self.ddh_pending = False
            raise

    async def _execute_initial_option_premium_hedge_if_needed(self, current_price: float):
        """Execute initial option premium hedge if needed

        This method checks all options in the portfolio and executes a hedge if the difference
        between the needed hedge amount and the actual hedged amount exceeds the minimum threshold.

        The initial_option_usd_value dict stores tuples of (needed_hedge, actual_hedge).
        When actual_hedge reaches needed_hedge, no more hedging is needed for that option.
        """
        required_hedge_qty = 0.0
        options_to_hedge = []

        # Only proceed if the absolute difference between needed and actual hedge exceeds min_hedge_usd
        min_hedge = self.config.min_hedge_usd

        # Calculate total required hedge across all options
        for instrument, (needed_hedge, actual_hedge) in list(self.portfolio.initial_option_usd_value.items()):
            required_qty = needed_hedge - actual_hedge
            if abs(required_qty) > min_hedge:  # If there's any hedge needed
                # Track the unrounded amount for proportional distribution later
                required_hedge_qty += required_qty
                options_to_hedge.append((instrument, needed_hedge, actual_hedge))


        if abs(required_hedge_qty) >= min_hedge:
            logger.info("Processing initial hedge")
            # Round up to the nearest multiple of min_hedge_usd in the direction of required_hedge_qty
            abs_hedge = abs(required_hedge_qty)
            # Calculate number of min_hedge units needed, rounding up
            units = math.ceil(abs_hedge / min_hedge)
            # Ensure we hedge at least min_hedge
            units = max(1, units)  # At least 1 unit
            rounded_hedge_qty = math.copysign(units * min_hedge, required_hedge_qty)

            logger.info(
                f"Hedging required: ${required_hedge_qty:.2f} >= min_hedge_usd=${min_hedge:.2f}, " +
                f"rounded to ${rounded_hedge_qty:.2f}"
            )
        else:
            logger.debug(
                f"Skipping hedge: required=${required_hedge_qty:.2f} " +
                f"< min_hedge_usd=${min_hedge:.2f}"
            )
            return

        # Execute the hedge
        await self._execute_initial_option_premium_hedge(current_price, rounded_hedge_qty)

        # Update the actual hedged amounts proportionally using the rounded hedge amount
        for instrument, needed_hedge, actual_hedge in options_to_hedge:
            # Calculate this option's portion of the hedge based on the original required amount
            option_portion = (needed_hedge - actual_hedge) / required_hedge_qty

            # Calculate the original hedge amount for this option
            original_hedge_amount = required_hedge_qty * option_portion

            # Calculate the rounded hedge amount for this option, ensuring it's rounded up to min_hedge_usd
            # if it's a new hedge (actual_hedge is 0) or if the remaining amount is significant
            if abs(original_hedge_amount) >= min_hedge or abs(actual_hedge) < 1e-9:  # Close to zero
                # For new hedges or significant amounts, round up to the nearest min_hedge_usd
                abs_hedge = abs(original_hedge_amount)
                units = math.ceil(abs_hedge / min_hedge)
                rounded_hedge_amount = math.copysign(units * min_hedge, original_hedge_amount)
            else:
                # For small adjustments to existing hedges, use the exact amount
                rounded_hedge_amount = original_hedge_amount

            # Update the actual hedge amount with the rounded value
            new_actual_hedge = actual_hedge + rounded_hedge_amount

            # Store the actual executed hedge amount in the portfolio
            # Keep needed_hedge as is, only round the actual_hedge
            self.portfolio.initial_option_usd_value[instrument][1] = new_actual_hedge

            logger.info(
                f"Updated hedge for {instrument}: " +
                f"needed=${needed_hedge:.2f}, " +
                f"hedged=${rounded_hedge_amount:.2f} (from ${original_hedge_amount:.2f}), " +
                f"total_hedged=${new_actual_hedge:.2f} (was ${actual_hedge:.2f})"
            )

    async def _execute_initial_option_premium_hedge(self, current_price: float, required_hedge_qty: float):
        """Execute initial option premium hedge"""
        logger.info("Executing initial option premium hedge...")

        # Update portfolio state
        logger.debug(f"[DEBUG] Before hedge - initial_usd_hedge_position: {self.portfolio.initial_usd_hedge_position}")
        logger.debug(f"[DEBUG] Required hedge qty: {required_hedge_qty}")
        
        self.portfolio.initial_usd_hedged = True
        old_hedge_qty = self.portfolio.initial_usd_hedge_position
        new_hedge_qty = old_hedge_qty + required_hedge_qty
        
        logger.debug(f"[DEBUG] Setting initial_usd_hedge_position to {new_hedge_qty}")
        self.portfolio.initial_usd_hedge_position = new_hedge_qty
        
        # Verify the value was set correctly
        logger.debug(f"[DEBUG] After set - initial_usd_hedge_position: {self.portfolio.initial_usd_hedge_position}")

        realized_pnl = 0.0

        # Closing or reducing a position
        if (required_hedge_qty < 0 and old_hedge_qty > 0) or (required_hedge_qty > 0 and old_hedge_qty < 0):
            # Calculate PNL for the closed portion
            realized_pnl = -required_hedge_qty * (current_price - self.portfolio.initial_usd_hedge_avg_entry)

            # If we're closing the position completely, reset the average entry price
            if abs(new_hedge_qty) < 1e-8:  # Floating point comparison with tolerance
                self.portfolio.initial_usd_hedge_avg_entry = 0.0
        # Adding to or opening a new position
        elif abs(old_hedge_qty) > 1e-8:  # If we have an existing position
            # Calculate new average entry price using volume-weighted average
            total_cost = old_hedge_qty * self.portfolio.initial_usd_hedge_avg_entry + required_hedge_qty * current_price
            self.portfolio.initial_usd_hedge_avg_entry = total_cost / new_hedge_qty
        else:  # New position
            self.portfolio.initial_usd_hedge_avg_entry = current_price

        self.portfolio.realized_pnl += realized_pnl

        # Mark portfolio as dirty to trigger save via event system
        await self.portfolio._mark_dirty()

        # Log hedge execution details
        logger.info(
            f"Initial premium hedge executed: {'BOUGHT' if required_hedge_qty > 0 else 'SOLD'} "
            f"${abs(required_hedge_qty):.2f} at ${current_price:.2f}\n"
            f"New initial_usd_hedge_position: ${self.portfolio.initial_usd_hedge_position:,.2f} "
            f"(avg ${self.portfolio.initial_usd_hedge_avg_entry:,.2f})\n"
            f"Realized PNL: ${self.portfolio.realized_pnl:,.2f}"
        )

    async def _get_current_price(self) -> Optional[float]:
        """Get current price from the market

        This is a convenience method that calls _get_current_price_with_timeout with a default timeout.

        Returns:
            The current price, or None if no price is available
        """
        try:
            # Use a slightly longer timeout to allow for network latency
            return await self._get_current_price_with_timeout(timeout=2.0)
        except Exception as e:
            logger.warning(f"Error getting current price: {e}")
            return None
