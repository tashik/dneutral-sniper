"""
Hedging manager for coordinating multiple DynamicDeltaHedger instances.

This module provides the HedgingManager class which manages multiple DynamicDeltaHedger
instances, each responsible for hedging a single portfolio. It handles the lifecycle
of hedgers, distributes price updates, and provides a unified interface for managing
all hedges.
"""
import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Union

from dataclasses import dataclass

from .deribit_client import DeribitWebsocketClient
from .dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from .portfolio import Portfolio
from .portfolio_manager import PortfolioManager
from .subscription_manager import SubscriptionManager
from .types import PortfolioEvent, PortfolioEventType

logger = logging.getLogger(__name__)


@dataclass
class HedgerInfo:
    """Container for information about a running hedger."""
    hedger: DynamicDeltaHedger
    task: Optional[asyncio.Task] = None


class HedgingManager:
    """Manages multiple DynamicDeltaHedger instances for different portfolios.

    This class is responsible for:
    - Creating and managing DynamicDeltaHedger instances for each portfolio
    - Distributing price updates to relevant hedgers
    - Managing the lifecycle of hedgers (start/stop)
    - Coordinating subscriptions to market data
    """

    def __init__(
        self,
        portfolio_manager: PortfolioManager,
        subscription_manager: Optional[SubscriptionManager] = None,
        deribit_client: Optional[DeribitWebsocketClient] = None,
        default_hedger_config: Optional[Union[Dict[str, Any], HedgerConfig]] = None
    ):
        """Initialize the HedgingManager.

        Args:
            portfolio_manager: The portfolio manager instance
            subscription_manager: Optional subscription manager instance.
                If not provided, a new one will be created.
            deribit_client: The Deribit client instance (shared between all hedgers).
                Required if subscription_manager is not provided.
            default_hedger_config: Default configuration for new hedgers (dict or HedgerConfig)
        """
        self.portfolio_manager = portfolio_manager
        self.deribit_client = deribit_client

        # Track subscription events by instrument
        self._subscription_events: Dict[str, asyncio.Event] = {}

        # Initialize subscription manager with deribit_client if not provided
        if subscription_manager is None:
            if deribit_client is None:
                raise ValueError("Either subscription_manager or deribit_client must be provided")
            self.subscription_manager = SubscriptionManager(deribit_client=deribit_client)
            logger.info("Initialized new SubscriptionManager with provided Deribit client")
        else:
            self.subscription_manager = subscription_manager
            logger.info("Using provided SubscriptionManager")

        # Register subscription confirmation handler if we have a deribit client
        if self.deribit_client:
            self.deribit_client.add_subscription_handler(
                self._handle_subscription_confirmation
            )

        # Set up price callback for the shared client
        self._price_handlers: Dict[
            str,
            List[Callable[[str, float], None]]
        ] = defaultdict(list)
        if self.deribit_client:
            self.deribit_client.set_price_callback(self._on_price_update)

        # Convert HedgerConfig to dict if needed
        if isinstance(default_hedger_config, HedgerConfig):
            self.default_hedger_config = {
                'ddh_min_trigger_delta': default_hedger_config.ddh_min_trigger_delta,
                'ddh_target_delta': default_hedger_config.ddh_target_delta,
                'ddh_step_mode': default_hedger_config.ddh_step_mode,
                'ddh_step_size': default_hedger_config.ddh_step_size,
                'price_check_interval': getattr(default_hedger_config, 'price_check_interval', 2.0),
                'underlying': getattr(default_hedger_config, 'underlying', 'BTC'),
                'instrument_name': getattr(default_hedger_config, 'instrument_name', ''),
                'volatility': getattr(default_hedger_config, 'volatility', 0.8),
                'risk_free_rate': getattr(default_hedger_config, 'risk_free_rate', 0.0),
                'min_hedge_usd': getattr(default_hedger_config, 'min_hedge_usd', 10.0)
            }
        else:
            self.default_hedger_config = default_hedger_config or {}

        # Map from portfolio_id to HedgerInfo
        self.hedgers: Dict[str, HedgerInfo] = {}

        # Set of all instruments we're subscribed to
        self.subscribed_instruments: Set[str] = set()

        # Lock for thread-safety
        self._lock = asyncio.Lock()

        # Event to signal when the manager is shutting down
        self._shutdown_event = asyncio.Event()

        # Background task for monitoring portfolios
        self._monitor_task: Optional[asyncio.Task] = None

        # Event handlers for portfolio events
        self._event_handlers = {
            PortfolioEventType.OPTION_ADDED: self._on_option_added,
            PortfolioEventType.OPTION_REMOVED: self._on_option_removed
        }

    async def start(self) -> None:
        """Start the hedging manager.

        This will start monitoring for portfolio changes and manage hedgers accordingly.
        Ensures only one monitor task is running at a time and handles cleanup of any
        existing tasks before starting a new one.
        """
        # Use the lock to prevent concurrent starts/stops
        async with self._lock:
            # Check if we already have a running task
            if self._monitor_task is not None:
                if not self._monitor_task.done():
                    logger.warning("Hedging manager is already running")
                    return
                else:
                    # Clean up the done task
                    self._monitor_task = None

            logger.info("Starting hedging manager...")
            self._shutdown_event.clear()

            try:
                # Create and start the monitor task
                self._monitor_task = asyncio.create_task(
                    self._monitor_portfolios(),
                    name=f"HedgingManager-{id(self)}"
                )
                logger.info("Hedging manager started successfully")
            except Exception as e:
                logger.error(f"Failed to start hedging manager: {e}", exc_info=True)
                self._monitor_task = None
                raise

    async def stop(self) -> None:
        """Stop the hedging manager and all hedgers.

        This method performs a graceful shutdown by:
        1. Setting the shutdown event to signal all tasks to stop
        2. Cancelling the monitor task with a timeout
        3. Stopping all individual hedgers
        4. Cleaning up resources
        """
        logger.info("Stopping hedging manager...")

        # Set the shutdown event first to prevent new operations
        if hasattr(self, '_shutdown_event'):
            self._shutdown_event.set()

        # Stop the monitor task if it exists
        monitor_task = None
        if hasattr(self, '_monitor_task') and self._monitor_task:
            monitor_task = self._monitor_task
            self._monitor_task = None  # Clear the reference first to prevent race conditions

            if not monitor_task.done():
                monitor_task.cancel()
                try:
                    # Give the task some time to shut down gracefully
                    await asyncio.wait_for(monitor_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                except Exception as e:
                    logger.warning(f"Error waiting for monitor task to stop: {e}")

        # Stop all hedgers
        if hasattr(self, 'hedgers') and self.hedgers:
            logger.info(f"Stopping {len(self.hedgers)} hedgers...")
            # Create a copy of the keys to avoid modification during iteration
            portfolio_ids = list(self.hedgers.keys())
            for portfolio_id in portfolio_ids:
                try:
                    await self._remove_hedger(portfolio_id)
                except Exception as e:
                    logger.error(f"Error removing hedger for {portfolio_id}: {e}", exc_info=True)

        logger.info("Hedging manager stopped")

    async def _monitor_portfolios(self) -> None:
        """Monitor for portfolio changes and manage hedgers.

        This method runs in a loop, periodically checking for portfolio changes and
        managing hedgers accordingly. It handles proper cleanup on shutdown and
        prevents multiple instances from running simultaneously.
        """
        logger.info("Portfolio monitor started")

        # Add a reference to this task for proper cleanup
        self._monitor_task = asyncio.current_task()

        try:
            while not self._shutdown_event.is_set():
                try:
                    # Get all portfolio IDs from the portfolio manager
                    portfolio_ids = await self.portfolio_manager.list_portfolios()
                    if not portfolio_ids:
                        logger.debug("No portfolios found, waiting...")
                        await asyncio.sleep(5)
                        continue

                    # Update hedgers based on current portfolios
                    current_hedgers = set(self.hedgers.keys())
                    target_hedgers = set(portfolio_ids)

                    # Add new hedgers for new portfolios
                    for portfolio_id in (target_hedgers - current_hedgers):
                        try:
                            await self._add_hedger(portfolio_id)
                            # Set up event listeners for the new portfolio
                            portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
                            if portfolio:
                                await self._setup_portfolio_listeners(portfolio)
                        except Exception as e:
                            logger.error(f"Failed to add hedger for portfolio {portfolio_id}: {e}", exc_info=True)

                    # Remove hedgers for deleted portfolios
                    for portfolio_id in (current_hedgers - target_hedgers):
                        try:
                            await self._remove_hedger(portfolio_id)
                        except Exception as e:
                            logger.error(f"Failed to remove hedger for portfolio {portfolio_id}: {e}", exc_info=True)

                    # Wait before next check, but allow for shutdown
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=30.0  # Check every 30 seconds or on shutdown
                        )
                    except asyncio.TimeoutError:
                        continue  # Normal case, just continue the loop

                except asyncio.CancelledError:
                    logger.info("Portfolio monitor cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error in portfolio monitor loop: {e}", exc_info=True)
                    await asyncio.sleep(5)  # Prevent tight loop on errors

        except asyncio.CancelledError:
            logger.info("Portfolio monitor cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in portfolio monitor: {e}", exc_info=True)
            raise
        finally:
            # Clear the task reference and log shutdown
            if hasattr(self, '_monitor_task') and self._monitor_task is asyncio.current_task():
                self._monitor_task = None
            logger.info("Portfolio monitor stopped")

    async def _setup_portfolio_listeners(self, portfolio: 'Portfolio') -> None:
        """Set up event listeners for a portfolio.

        Args:
            portfolio: The portfolio to set up listeners for
        """
        logger.debug("Setting up event listeners for portfolio %s", portfolio.id)

        # Remove any existing listeners first to avoid duplicates
        await self._remove_portfolio_listeners(portfolio)

        # Add new listeners
        for event_type, handler in self._event_handlers.items():
            # Create a bound method that includes the portfolio ID
            async def event_handler(event: PortfolioEvent, handler=handler, portfolio_id=portfolio.id):
                if event.portfolio.id == portfolio_id:  # Only handle events for this portfolio
                    await handler(event)

            # Store the handler so we can remove it later
            if not hasattr(self, '_portfolio_handlers'):
                self._portfolio_handlers = {}
            if portfolio.id not in self._portfolio_handlers:
                self._portfolio_handlers[portfolio.id] = {}

            self._portfolio_handlers[portfolio.id][event_type] = event_handler
            await portfolio.add_listener(event_type, event_handler)

        logger.debug("Event listeners set up for portfolio %s", portfolio.id)

    async def _remove_portfolio_listeners(self, portfolio: 'Portfolio') -> None:
        """Remove event listeners for a portfolio.

        Args:
            portfolio: The portfolio to remove listeners for
        """
        if not hasattr(self, '_portfolio_handlers') or portfolio.id not in self._portfolio_handlers:
            return

        logger.debug("Removing event listeners for portfolio %s", portfolio.id)

        for event_type, handler in self._portfolio_handlers[portfolio.id].items():
            try:
                await portfolio.remove_listener(event_type, handler)
            except Exception as e:
                logger.warning("Failed to remove listener for portfolio %s: %s", portfolio.id, e)

        del self._portfolio_handlers[portfolio.id]
        logger.debug("Event listeners removed for portfolio %s", portfolio.id)

    async def _on_option_added(self, event: PortfolioEvent) -> None:
        """Handle OPTION_ADDED event.

        Args:
            event: The portfolio event
        """
        portfolio_id = event.portfolio.id
        instrument_name = event.data.get('instrument_name')

        # Skip if we don't have a valid instrument name or if there's no hedger for this portfolio
        if not instrument_name or portfolio_id not in self.hedgers:
            return

        logger.info(
            "Option %s added to portfolio %s, updating subscriptions",
            instrument_name, portfolio_id
        )

        hedger_info = self.hedgers[portfolio_id]

        # Skip if we're already shutting down
        if self._shutdown_event.is_set():
            logger.debug("Skipping subscription for %s - manager is shutting down", instrument_name)
            return

        try:
            # Check if we're already subscribed to this instrument
            if instrument_name in self.subscribed_instruments:
                logger.debug("Already subscribed to %s, skipping duplicate subscription", instrument_name)
                return

            # Register price handler for the new option
            await self._register_price_handler(instrument_name, hedger_info.hedger._price_callback)

            # Subscribe to the instrument
            await self.subscription_manager.add_subscription(
                portfolio_id,
                instrument_name,
                wait_for_confirmation=False  # Don't wait to avoid blocking the event loop
            )

            logger.info(
                "Subscribed to new option %s for portfolio %s",
                instrument_name, portfolio_id
            )

        except Exception as e:
            logger.error("Failed to subscribe to new option %s: %s", instrument_name, e, exc_info=True)

    async def _on_option_removed(self, event: PortfolioEvent) -> None:
        """Handle OPTION_REMOVED event.

        Args:
            event: The portfolio event
        """
        portfolio_id = event.portfolio.id
        instrument_name = event.data.get('instrument_name')

        if not instrument_name or portfolio_id not in self.hedgers:
            return

        logger.info(
            "Option %s removed from portfolio %s, cleaning up",
            instrument_name, portfolio_id
        )

        hedger_info = self.hedgers[portfolio_id]

        try:
            # Unregister price handler for the removed option
            await self._unregister_price_handler(instrument_name, hedger_info.hedger._price_callback)
            logger.debug("Unregistered price handler for %s", instrument_name)

            # Unsubscribe from the instrument
            await self.subscription_manager.remove_subscription(portfolio_id, instrument_name)
            logger.info(
                "Unsubscribed from removed option %s for portfolio %s",
                instrument_name, portfolio_id
            )

        except Exception as e:
            logger.error("Failed to clean up removed option %s: %s", instrument_name, e, exc_info=True)
            # Even if cleanup fails, we should remove it from our tracking
            if instrument_name in self.subscribed_instruments:
                self.subscribed_instruments.remove(instrument_name)

    async def _register_price_handler(self, instrument: str, handler: Callable[[str, float], None]) -> None:
        """Register a price update handler for an instrument.

        Args:
            instrument: The instrument to receive price updates for
            handler: Callback function that takes (instrument: str, price: float)
        """
        if not instrument:
            logger.warning("Attempted to register price handler with empty instrument")
            return

        async with self._lock:
            logger.debug("Registering price handler for %s", instrument)

            # Register for the base instrument if this is a specific option/future
            if '-' in instrument and 'PERPETUAL' not in instrument:
                base_instrument = instrument.split('-')[0]
                logger.debug("Registering for base instrument %s", base_instrument)
                if base_instrument not in self._price_handlers:
                    self._price_handlers[base_instrument] = []
                if handler not in self._price_handlers[base_instrument]:
                    self._price_handlers[base_instrument].append(handler)
                    logger.debug(
                        "Registered handler for base instrument %s (now %d handlers)",
                        base_instrument, len(self._price_handlers[base_instrument])
                    )

            # Always register for the specific instrument as well
            if instrument not in self._price_handlers:
                self._price_handlers[instrument] = []
            if handler not in self._price_handlers[instrument]:
                self._price_handlers[instrument].append(handler)
                logger.debug(
                    "Registered handler for instrument %s (now %d handlers)",
                    instrument, len(self._price_handlers[instrument])
                )

    async def _unregister_price_handler(self, instrument: str, handler: Callable[[str, float], None]) -> None:
        """Unregister a price update handler for an instrument.

        Args:
            instrument: The instrument to stop receiving price updates for
            handler: The callback function to remove
        """
        if not instrument:
            logger.debug("Skipping unregister for empty instrument")
            return

        async with self._lock:
            # First handle the specific instrument
            if instrument in self._price_handlers:
                if handler in self._price_handlers[instrument]:
                    self._price_handlers[instrument].remove(handler)
                    logger.debug(
                        "Unregistered handler for instrument %s (%d handlers remain)",
                        instrument, len(self._price_handlers[instrument])
                    )

                # Clean up empty handler lists
                if not self._price_handlers[instrument]:
                    del self._price_handlers[instrument]
                    logger.debug("Removed empty handler list for instrument %s", instrument)

            # Also unregister from the base instrument if this is a specific option/future
            if '-' in instrument and 'PERPETUAL' not in instrument:
                base_instrument = instrument.split('-')[0]
                if base_instrument in self._price_handlers:
                    if handler in self._price_handlers[base_instrument]:
                        self._price_handlers[base_instrument].remove(handler)
                        logger.debug(
                            "Unregistered handler for base instrument %s (%d handlers remain)",
                            base_instrument, len(self._price_handlers[base_instrument])
                        )

                    # Clean up empty handler lists
                    if not self._price_handlers[base_instrument]:
                        del self._price_handlers[base_instrument]
                        logger.debug("Removed empty handler list for base instrument %s", base_instrument)

    async def _on_price_update(self, instrument: str, price: float) -> None:
        """Handle a price update from the WebSocket client.

        Args:
            instrument: The instrument that was updated
            price: The new price
        """
        logger.debug("[PRICE_UPDATE] Received price update - Instrument: %s, Price: %f", instrument, price)

        if not instrument or price is None:
            logger.warning("Received invalid price update: instrument=%s, price=%s", instrument, price)
            return

        # Get the base instrument (without expiry/strike) for broad subscriptions
        base_instrument = instrument.split('-')[0] if '-' in instrument else instrument

        # Get handlers for both the specific instrument and its base
        handlers = []

        # Use a single lock context to get all relevant handlers
        async with self._lock:
            logger.debug("[PRICE_UPDATE] Checking handlers for %s (base: %s)", instrument, base_instrument)
            logger.debug("[PRICE_UPDATE] Available handlers: %s", list(self._price_handlers.keys()))

            if instrument in self._price_handlers:
                logger.debug("[PRICE_UPDATE] Found handlers for specific instrument")
                handlers.extend(self._price_handlers[instrument])
            elif base_instrument in self._price_handlers and base_instrument != instrument:
                logger.debug("[PRICE_UPDATE] Found handlers for base instrument")
                handlers.extend(self._price_handlers[base_instrument])

        if not handlers:
            logger.debug("No handlers registered for %s (base: %s)", instrument, base_instrument)
            return

        logger.debug("Dispatching price update for %s: %f to %d handlers", instrument, price, len(handlers))

        # Make a copy of handlers to avoid modification during iteration
        handlers_to_call = list(handlers)
        logger.debug("Dispatching price update for %s: %f to %d handlers", instrument, price, len(handlers_to_call))

        # Execute all handlers concurrently
        tasks = []
        for handler in handlers_to_call:
            try:
                handler_name = getattr(handler, '__name__', str(handler))
                logger.debug(
                    "[PRICE_UPDATE] Calling handler %s for %s",
                    handler_name, instrument
                )

                if asyncio.iscoroutinefunction(handler):
                    # Schedule the coroutine to run concurrently
                    task = asyncio.create_task(handler(instrument, float(price)))
                    task.add_done_callback(
                        lambda t, h=handler_name:
                        logger.debug("[PRICE_UPDATE] Handler %s completed for %s", h, instrument)
                        if not t.cancelled() and t.exception() is None
                        else logger.error(
                            "[PRICE_UPDATE] Error in handler %s for %s: %s",
                            h, instrument,
                            str(t.exception()) if t.exception() else "Cancelled"
                        )
                    )
                    tasks.append(task)
                else:
                    # Run sync callbacks in a thread
                    loop = asyncio.get_event_loop()
                    task = loop.run_in_executor(None, handler, instrument, float(price))
                    tasks.append(task)
            except Exception as e:
                logger.error(
                    "Error scheduling price handler %s: %s",
                    getattr(handler, '__name__', str(handler)), e, exc_info=True
                )

        # Wait for all handlers to complete with timeout
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("Timed out waiting for price handlers to complete for %s", instrument)
            except Exception as e:
                logger.error("Error in price handlers for %s: %s", instrument, e, exc_info=True)

        # logger.info("Price update processed for %s: %f", instrument, price)

    async def _handle_subscription_confirmation(self, instrument: str) -> None:
        """Handle subscription confirmation for an instrument.

        This method is called when a subscription to an instrument is confirmed by the exchange.
        It updates internal state, notifies the subscription manager, and triggers any
        necessary portfolio updates.

        Args:
            instrument: The instrument that was confirmed (e.g., 'BTC-PERPETUAL')
        """

        # Set the event for this instrument if we're waiting for it
        if instrument in self._subscription_events:
            logger.debug("Setting subscription event for %s", instrument)
            self._subscription_events[instrument].set()
            # Don't remove the event here - let the cleanup happen in the callback

        # Continue with the rest of the confirmation handling

        logger.info("Subscription confirmed for instrument: %s", instrument)

        # Update our tracking of subscribed instruments
        async with self._lock:
            self.subscribed_instruments.add(instrument)

        # Forward the confirmation to the subscription manager
        if hasattr(self, 'subscription_manager'):
            try:
                await self.subscription_manager.handle_subscription_confirmation(instrument)
                logger.debug("Forwarded subscription confirmation for %s to subscription manager", instrument)

                # Notify all portfolios that might be interested in this instrument
                for portfolio_id, hedger_info in list(self.hedgers.items()):
                    try:
                        # Get the portfolio from the portfolio manager
                        portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
                        if not portfolio:
                            continue

                        # Check if this portfolio is interested in this instrument
                        portfolio_instruments = await self.portfolio_manager.get_subscribed_instruments(portfolio_id)
                        if instrument in portfolio_instruments:
                            # Emit a STATE_CHANGED event with subscription confirmation info
                            await portfolio.emit(PortfolioEvent(
                                event_type=PortfolioEventType.STATE_CHANGED,
                                portfolio=portfolio,
                                data={
                                    'type': 'subscription_confirmed',
                                    'instrument': instrument,
                                    'timestamp': datetime.now().isoformat()
                                }
                            ))
                            logger.debug(
                                "Emitted subscription confirmation event for %s on portfolio %s",
                                instrument, portfolio_id
                            )
                    except Exception as e:
                        logger.error(
                            "Error notifying portfolio %s about subscription confirmation: %s",
                            portfolio_id, e, exc_info=True
                        )

            except Exception as e:
                logger.error("Error forwarding subscription confirmation for %s: %s", instrument, e, exc_info=True)

        # Notify any waiting tasks about the subscription confirmation
        if hasattr(self, '_subscription_waiter') and not self._subscription_waiter.done():
            self._subscription_waiter.set_result(instrument)

        # Log the current state of subscriptions
        logger.debug(
            "Current subscribed instruments: %s",
            list(self.subscribed_instruments)[:10] +
            (['...'] if len(self.subscribed_instruments) > 10 else [])
        )

        # If we have a price for this instrument, trigger an immediate update
        if hasattr(self, 'deribit_client') and self.deribit_client:
            try:
                price, _ = self.deribit_client.get_price_iv(instrument)
                if price is not None:
                    logger.debug("Triggering immediate price update for %s: %f", instrument, price)
                    await self._on_price_update(instrument, price)
            except Exception as e:
                logger.warning("Failed to get initial price for %s: %s", instrument, e)

        # If we have any hedgers waiting for this instrument, start them
        for portfolio_id, hedger_info in list(self.hedgers.items()):
            # Check if this hedger is for the confirmed instrument and not already started
            config = hedger_info.hedger.config
            instrument_name = getattr(config, 'instrument_name', None) if hasattr(config, 'instrument_name') else None
            if instrument_name == instrument and not hedger_info.task:
                logger.info("Starting hedger for portfolio %s now that subscription is confirmed", portfolio_id)
                hedger_info.task = asyncio.create_task(hedger_info.hedger.start())

    async def _add_hedger(
        self,
        portfolio_id: str,
        config_override: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Add a new hedger for the given portfolio.

        Args:
            portfolio_id: ID of the portfolio to add a hedger for
            config_override: Optional dictionary of configuration overrides for the hedger

        Returns:
            bool: True if a new hedger was added, False if one already exists
        """
        # First check if we already have a running hedger without holding the lock
        async with self._lock:
            if portfolio_id in self.hedgers:
                hedger_info = self.hedgers[portfolio_id]
                if hedger_info.task and not hedger_info.task.done():
                    logger.debug("Hedger already exists and is running for portfolio: %s", portfolio_id)
                    return False
                else:
                    # Clean up the dead hedger
                    logger.debug("Cleaning up dead hedger for portfolio: %s", portfolio_id)
                    await self._cleanup_hedger(portfolio_id)

        # Get the portfolio outside the lock to avoid holding it during I/O
        portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
        if not portfolio:
            logger.error("Portfolio not found: %s", portfolio_id)
            return False

        # Create a new hedger with the portfolio and merged config
        config_dict = dict(self.default_hedger_config) if self.default_hedger_config else {}
        if config_override:
            config_dict.update(config_override)
        config = HedgerConfig(**config_dict)
        logger.info("Creating hedger for portfolio %s with config: %s", portfolio_id, config)
        hedger = DynamicDeltaHedger(config, portfolio, self.deribit_client)

        # Prepare the list of instruments to subscribe to
        perpetual_instrument = f"{portfolio.underlying.upper()}-PERPETUAL"
        instruments_to_subscribe = [perpetual_instrument]

        # Add all options in the portfolio
        for option in portfolio.options.values():
            instrument_name = getattr(option, 'instrument_name', None)
            if instrument_name:
                instruments_to_subscribe.append(instrument_name)

        # Register price handlers first
        for instrument in instruments_to_subscribe:
            await self._register_price_handler(instrument, hedger._price_callback)

        # Now acquire the lock again to update the state
        async with self._lock:
            # Double-check if another task added a hedger while we were working
            if portfolio_id in self.hedgers:
                if self.hedgers[portfolio_id].task and not self.hedgers[portfolio_id].task.done():
                    logger.debug("Another task added a hedger while we were working, aborting")
                    # Clean up the hedger we created
                    try:
                        await hedger.stop()
                    except Exception as e:
                        logger.warning("Error cleaning up duplicate hedger: %s", e)
                    return False

            # Store the hedger info
            self.hedgers[portfolio_id] = HedgerInfo(hedger=hedger)

            # Set up portfolio event listeners
            await self._setup_portfolio_listeners(portfolio)

            # Create subscription events
            subscription_event = asyncio.Event()
            self._subscription_events[perpetual_instrument] = subscription_event

        # Now do the subscriptions without holding the lock
        try:
            # Subscribe to all instruments
            for instrument in instruments_to_subscribe:
                await self.subscription_manager.add_subscription(
                    portfolio_id,
                    instrument,
                    wait_for_confirmation=False
                )

            # Start the hedger
            hedger_task = asyncio.create_task(hedger.start())

            # Update the task in the hedger info
            async with self._lock:
                if portfolio_id in self.hedgers:  # Check if still exists
                    self.hedgers[portfolio_id].task = hedger_task
                else:
                    # Oops, the hedger was removed while we were working
                    hedger_task.cancel()
                    try:
                        await hedger_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    return False

                # Set up the done callback
                async def _handle_hedger_done(task, pid=portfolio_id, pi=perpetual_instrument):
                    await self._on_hedger_done(task, pid, pi)

                # Add the callback
                hedger_task.add_done_callback(
                    lambda t: asyncio.create_task(_handle_hedger_done(t))
                )

            logger.info("Successfully added and started hedger for portfolio: %s", portfolio_id)
            return True

        except Exception as e:
            logger.error("Error adding subscriptions for portfolio %s: %s", portfolio_id, e, exc_info=True)
            async with self._lock:
                if portfolio_id in self.hedgers:
                    await self._cleanup_hedger(portfolio_id)
            return False

    async def _on_hedger_done(self, task: asyncio.Task, portfolio_id: str, perpetual_instrument: str) -> None:
        """Handle completion of a hedger task.

        Args:
            task: The completed task
            portfolio_id: The portfolio ID for the hedger
            perpetual_instrument: The perpetual instrument name for cleanup
        """
        try:
            task.result()  # This will raise if the task failed
        except asyncio.CancelledError:
            logger.debug(f"Hedger for {portfolio_id} was cancelled")
        except Exception as e:
            logger.error(f"Hedger for {portfolio_id} failed: {e}", exc_info=True)
        finally:
            # Clean up the hedger and its resources
            await self._cleanup_hedger(portfolio_id)

    async def _cleanup_hedger(self, portfolio_id: str) -> None:
        """Clean up a hedger and its resources.

        Args:
            portfolio_id: The portfolio ID of the hedger to clean up
        """
        async with self._lock:
            if portfolio_id not in self.hedgers:
                return

            hedger_info = self.hedgers.pop(portfolio_id, None)
            if not hedger_info:
                return

            logger.debug("Cleaning up hedger for portfolio %s", portfolio_id)

            # Clean up the hedger task if it exists
            if hedger_info.task:
                if not hedger_info.task.done():
                    hedger_info.task.cancel()
                    try:
                        await asyncio.wait_for(hedger_info.task, timeout=5.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass

            try:
                # Clean up the portfolio listeners if we have a hedger with a price callback
                if hedger_info.hedger is not None and hasattr(hedger_info.hedger, '_price_callback'):
                    portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
                    if portfolio is not None:
                        try:
                            # Clean up price handlers for the perpetual instrument
                            perpetual_instrument = f"{portfolio.underlying.upper()}-PERPETUAL"
                            await self._unregister_price_handler(
                                perpetual_instrument,
                                hedger_info.hedger._price_callback
                            )

                            # Clean up option price handlers
                            for option in portfolio.options.values():
                                instrument_name = getattr(option, 'instrument_name', None)
                                if instrument_name:
                                    await self._unregister_price_handler(
                                        instrument_name,
                                        hedger_info.hedger._price_callback
                                    )

                            # Remove portfolio listeners
                            await self._remove_portfolio_listeners(portfolio)
                        except Exception as e:
                            logger.warning(f"Error cleaning up portfolio {portfolio_id} handlers: {e}")

                logger.debug("Cleaned up hedger for portfolio: %s", portfolio_id)
            except Exception as e:
                logger.warning(f"Unexpected error during hedger cleanup for {portfolio_id}: {e}", exc_info=True)

    async def _remove_hedger(self, portfolio_id: str) -> None:
        """Remove and stop the hedger for the specified portfolio.

        Args:
            portfolio_id: ID of the portfolio to remove the hedger for
        """
        # First check if we have this hedger
        if portfolio_id not in self.hedgers:
            logger.debug("No hedger found for portfolio: %s", portfolio_id)
            return

        logger.info("Removing hedger for portfolio: %s", portfolio_id)

        # Get the hedger info and remove it from the dict immediately to prevent race conditions
        async with self._lock:
            if portfolio_id not in self.hedgers:  # Double-check in case another task removed it
                return
            hedger_info = self.hedgers.pop(portfolio_id, None)
            if not hedger_info:
                return

        # Clean up the task if it exists
        task = None
        if hedger_info.task and not hedger_info.task.done():
            task = hedger_info.task
            logger.debug("Cancelling hedger task for portfolio: %s", portfolio_id)
            task.cancel()

        # Stop the hedger if it exists
        stop_futures = []
        if hedger_info.hedger is not None:
            logger.debug("Stopping hedger for portfolio: %s", portfolio_id)
            stop_futures.append(self._safe_stop_hedger(hedger_info.hedger))

        # Wait for both the task and hedger to stop, but don't wait forever
        if task or stop_futures:
            try:
                # Create a list of all futures to wait for
                futures = []
                if task is not None:
                    futures.append(task)
                futures.extend(stop_futures)

                await asyncio.wait_for(
                    asyncio.gather(*futures, return_exceptions=True),
                    timeout=2.0
                )
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                logger.warning("Timeout or cancellation during hedger cleanup for %s: %s", portfolio_id, e)

        # Clean up any remaining resources
        await self._cleanup_hedger(portfolio_id)
        logger.debug("Successfully removed hedger for portfolio: %s", portfolio_id)

    async def _safe_stop_hedger(self, hedger) -> None:
        """Safely stop a hedger, catching and logging any exceptions."""
        try:
            if hasattr(hedger, 'stop'):
                await hedger.stop()
        except Exception as e:
            logger.warning("Error stopping hedger: %s", e, exc_info=True)

    async def _stop_all_hedgers(self) -> None:
        """Stop all managed hedgers."""
        async with self._lock:
            for portfolio_id in list(self.hedgers.keys()):
                await self._remove_hedger(portfolio_id)

    async def _update_subscriptions(self) -> None:
        """Update instrument subscriptions based on all portfolios' needs."""
        # Get all instruments that any portfolio is subscribed to
        all_instruments = set()
        for portfolio_id in self.hedgers.keys():
            instruments = await self.subscription_manager.get_portfolio_subscriptions(portfolio_id)
            all_instruments.update(instruments)

        # Subscribe to new instruments
        to_subscribe = all_instruments - self.subscribed_instruments
        if to_subscribe:
            logger.info(f"Subscribing to instruments: {to_subscribe}")
            try:
                # Convert to list as subscribe_to_instruments expects a sequence
                await self.deribit_client.subscribe_to_instruments(list(to_subscribe))
                self.subscribed_instruments.update(to_subscribe)
                logger.debug(f"Successfully subscribed to instruments: {to_subscribe}")
            except Exception as e:
                logger.error(f"Error subscribing to instruments {to_subscribe}: {e}")

        # Unsubscribe from instruments no longer needed
        to_unsubscribe = self.subscribed_instruments - all_instruments
        if to_unsubscribe:
            logger.info(f"Unsubscribing from instruments: {to_unsubscribe}")
            try:
                # Note: Deribit doesn't support unsubscribing from specific instruments,
                # so we'll just update our local tracking
                self.subscribed_instruments.difference_update(to_unsubscribe)
                logger.debug(f"Unsubscribed from instruments: {to_unsubscribe}")
            except Exception as e:
                logger.error(f"Error unsubscribing from instruments {to_unsubscribe}: {e}")

        # Log current subscription state
        logger.debug(f"Current subscriptions: {self.subscribed_instruments}")

    async def get_hedger(self, portfolio_id: str) -> Optional[DynamicDeltaHedger]:
        """Get the hedger for a specific portfolio.

        Args:
            portfolio_id: ID of the portfolio

        Returns:
            The DynamicDeltaHedger instance, or None if not found
        """
        async with self._lock:
            hedger_info = self.hedgers.get(portfolio_id)
            return hedger_info.hedger if hedger_info else None

    async def get_hedger_stats(self, portfolio_id: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a hedger.

        Args:
            portfolio_id: ID of the portfolio

        Returns:
            Dictionary of statistics, or None if hedger not found
        """
        hedger = await self.get_hedger(portfolio_id)
        if not hedger or not hasattr(hedger, 'stats'):
            return None

        return {
            'portfolio_id': portfolio_id,
            'enabled': hedger.ddh_enabled,
            'current_delta': hedger.cur_delta,
            'target_delta': hedger.target_delta,
            'last_hedge_time': hedger.last_hedge_time,
            'hedge_count': hedger.hedge_count,
            'stats': getattr(hedger, 'stats', {})
        }

    async def get_all_hedger_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all hedgers.

        Returns:
            Dictionary mapping portfolio IDs to their statistics
        """
        stats = {}
        for portfolio_id in list(self.hedgers.keys()):
            stats[portfolio_id] = await self.get_hedger_stats(portfolio_id) or {}
        return stats

    async def start_hedging(
        self,
        portfolio_id: str,
        timeout: float = 10.0,
        config_override: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Start hedging for a specific portfolio.

        Args:
            portfolio_id: ID of the portfolio to start hedging for
            timeout: Maximum time to wait for the operation to complete
            config_override: Optional dictionary of configuration overrides for the hedger

        Returns:
            bool: True if hedging was started, False if it was already running or failed
        """
        logger.info(f"[DEBUG] Entering start_hedging for portfolio {portfolio_id}")

        # Quick check without lock first to avoid unnecessary lock acquisition
        if portfolio_id in self.hedgers:
            logger.info(f"[DEBUG] Hedging already running for portfolio {portfolio_id}")
            return False

        try:
            # Get the portfolio to ensure it exists (without holding the lock)
            logger.debug(f"[DEBUG] Fetching portfolio {portfolio_id}")
            portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
            if not portfolio:
                logger.error(f"Cannot start hedging: Portfolio {portfolio_id} not found")
                return False

            # Let _add_hedger handle the locking
            success = await self._add_hedger(portfolio_id, config_override=config_override)

            if success:
                logger.info(f"Successfully started hedging for portfolio {portfolio_id}")
            else:
                logger.info(f"Failed to start hedging for portfolio {portfolio_id} (already running?)")

            return success

        except asyncio.TimeoutError:
            logger.error(f"Timeout ({timeout}s) while trying to start hedging for portfolio {portfolio_id}")
            return False

        except Exception as e:
            logger.error(f"Error starting hedging for portfolio {portfolio_id}: {e}", exc_info=True)
            return False

    async def stop_hedging(self, portfolio_id: str) -> bool:
        """Stop hedging for a specific portfolio.

        Args:
            portfolio_id: ID of the portfolio to stop hedging for

        Returns:
            bool: True if hedging was stopped, False if it wasn't running
        """
        async with self._lock:
            if portfolio_id not in self.hedgers:
                logger.info(f"No active hedging found for portfolio {portfolio_id}")
                return False

            # Remove the hedger
            await self._remove_hedger(portfolio_id)
            logger.info(f"Stopped hedging for portfolio {portfolio_id}")
            return True
