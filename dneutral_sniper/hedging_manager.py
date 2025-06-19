"""
Hedging manager for coordinating multiple DynamicDeltaHedger instances.

This module provides the HedgingManager class which manages multiple DynamicDeltaHedger
instances, each responsible for hedging a single portfolio. It handles the lifecycle
of hedgers, distributes price updates, and provides a unified interface for managing
all hedges.
"""
import asyncio
import json
import logging
import os
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, List, Callable
from dataclasses import dataclass

from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager
from dneutral_sniper.deribit_client import DeribitWebsocketClient

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
            subscription_manager: Optional subscription manager instance. If not provided, a new one will be created.
            deribit_client: The Deribit client instance (shared between all hedgers). Required if subscription_manager is not provided.
            default_hedger_config: Default configuration for new hedgers (dict or HedgerConfig)
        """
        self.portfolio_manager = portfolio_manager
        self.deribit_client = deribit_client

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
            self.deribit_client.add_subscription_handler(self._handle_subscription_confirmation)

        # Set up price callback for the shared client
        self._price_handlers: Dict[str, List[Callable[[str, float], None]]] = defaultdict(list)
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

    async def start(self) -> None:
        """Start the hedging manager.

        This will start monitoring for portfolio changes and manage hedgers accordingly.
        """
        if self._monitor_task is not None and not self._monitor_task.done():
            logger.warning("Hedging manager is already running")
            return

        self._shutdown_event.clear()
        self._monitor_task = asyncio.create_task(self._monitor_portfolios())
        logger.info("Hedging manager started")

    async def stop(self) -> None:
        """Stop the hedging manager and all hedgers."""
        if hasattr(self, '_shutdown_event'):
            self._shutdown_event.set()

        # Stop the monitor task if it exists
        if hasattr(self, '_monitor_task') and self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(self._monitor_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.warning(f"Error stopping monitor task: {e}")

        # Stop all hedgers
        if hasattr(self, 'hedgers'):
            # Create a copy of the keys to avoid modification during iteration
            portfolio_ids = list(self.hedgers.keys())
            for portfolio_id in portfolio_ids:
                try:
                    await self._remove_hedger(portfolio_id)
                except Exception as e:
                    logger.error(f"Error removing hedger for {portfolio_id}: {e}")

        # Clear the monitor task reference
        if hasattr(self, '_monitor_task'):
            self._monitor_task = None

        logger.info("Hedging manager stopped")

    async def _monitor_portfolios(self) -> None:
        """Monitor for portfolio changes and manage hedgers."""
        logger.info("Starting portfolio monitor...")

        try:
            while not self._shutdown_event.is_set():
                try:
                    # Get current list of portfolio IDs
                    portfolio_ids = set(await self.portfolio_manager.list_portfolios())

                    # Add hedgers for new portfolios
                    for portfolio_id in portfolio_ids:
                        # Ensure portfolio_id is a string
                        if not isinstance(portfolio_id, str):
                            logger.warning(f"Skipping non-string portfolio_id: {portfolio_id}")
                            continue

                        if portfolio_id not in self.hedgers:
                            try:
                                logger.debug(f"Adding hedger for portfolio: {portfolio_id}")
                                success = await self._add_hedger(portfolio_id)
                                if not success:
                                    logger.error(f"Failed to add hedger for {portfolio_id}")
                            except Exception as e:
                                logger.error(f"Error adding hedger for {portfolio_id}: {e}", exc_info=True)

                    # Remove hedgers for deleted portfolios
                    for portfolio_id in list(self.hedgers.keys()):
                        if portfolio_id not in portfolio_ids:
                            try:
                                await self._remove_hedger(portfolio_id)
                            except Exception as e:
                                logger.error(f"Error removing hedger for {portfolio_id}: {e}", exc_info=True)

                    # Update subscriptions for all portfolios
                    portfolio_ids = list(self.hedgers.keys())
                    for portfolio_id in portfolio_ids:
                        # Skip if portfolio was removed during iteration
                        if portfolio_id not in self.hedgers:
                            continue

                        try:
                            portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
                            if not portfolio:
                                logger.warning(f"Portfolio {portfolio_id} not found, skipping subscription update")
                                continue

                            # Subscribe to the perpetual futures instrument for this portfolio
                            try:
                                perpetual_instrument = f"{portfolio.underlying.upper()}-PERPETUAL"
                                await self.subscription_manager.add_subscription(portfolio_id, perpetual_instrument)
                            except Exception as e:
                                logger.error(f"Error subscribing to perpetual for {portfolio_id}: {e}")

                            # Subscribe to all options in the portfolio
                            options = list(portfolio.options.values())  # Create a copy to prevent modification during iteration
                            for option in options:
                                try:
                                    instrument_name = getattr(option, 'instrument_name', None)
                                    if instrument_name:
                                        await self.subscription_manager.add_subscription(portfolio_id, instrument_name)
                                except Exception as e:
                                    logger.error(f"Error subscribing to option {getattr(option, 'instrument_name', 'unknown')} for {portfolio_id}: {e}")
                        except Exception as e:
                            logger.error(f"Error updating subscriptions for portfolio {portfolio_id}: {e}", exc_info=True)

                    # Sleep for a bit before checking again, but check shutdown more frequently
                    for _ in range(10):
                        if self._shutdown_event.is_set():
                            break
                        await asyncio.sleep(0.5)

                except asyncio.CancelledError:
                    logger.info("Portfolio monitor cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error in portfolio monitor: {e}", exc_info=True)
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("Portfolio monitor cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in portfolio monitor: {e}", exc_info=True)
            raise
        finally:
            logger.info("Portfolio monitor stopped")

    async def _register_price_handler(self, instrument: str, handler: Callable[[str, float], None]) -> None:
        """Register a price update handler for an instrument.

        Args:
            instrument: The instrument to receive price updates for
            handler: Callback function that takes (instrument: str, price: float)
        """
        if not instrument:
            logger.warning("Attempted to register price handler with empty instrument")
            return

        # Initialize _price_handlers if it doesn't exist
        if not hasattr(self, '_price_handlers'):
            self._price_handlers = {}

        logger.debug("Registering price handler for %s", instrument)

        # Register for the base instrument if this is a specific option/future
        if '-' in instrument and 'PERPETUAL' not in instrument:
            base_instrument = instrument.split('-')[0]
            logger.debug("Registering for base instrument %s", base_instrument)
            if base_instrument not in self._price_handlers:
                self._price_handlers[base_instrument] = []
            self._price_handlers[base_instrument].append(handler)
        elif 'PERPETUAL' in instrument:
            logger.debug("Registering for perpetual instrument %s", instrument)
            if instrument not in self._price_handlers:
                self._price_handlers[instrument] = []
            self._price_handlers[instrument].append(handler)

    def _unregister_price_handler(self, instrument: str, handler: Callable[[str, float], None]) -> None:
        """Unregister a price update handler for an instrument.

        Args:
            instrument: The instrument to stop receiving price updates for
            handler: The callback function to remove
        """
        if not instrument:
            return

        logger.debug("Unregistering price handler for %s", instrument)
        if instrument in self._price_handlers and handler in self._price_handlers[instrument]:
            self._price_handlers[instrument].remove(handler)

        # Also unregister from the base instrument if this is a specific option/future
        if '-' in instrument:
            base_instrument = instrument.split('-')[0]
            if base_instrument in self._price_handlers and handler in self._price_handlers[base_instrument]:
                self._price_handlers[base_instrument].remove(handler)

    async def _on_price_update(self, instrument: str, price: float) -> None:
        """Handle a price update from the WebSocket client.

        Args:
            instrument: The instrument that was updated
            price: The new price
        """
        if not instrument or price is None:
            logger.warning("Received invalid price update: instrument=%s, price=%s", instrument, price)
            return

        # Get the base instrument (without expiry/strike) for broad subscriptions
        base_instrument = instrument.split('-')[0] if '-' in instrument else instrument

        # Get handlers for both the specific instrument and its base
        handlers = []

        # Use a single lock context to get all relevant handlers
        async with self._lock:
            if instrument in self._price_handlers:
                handlers.extend(self._price_handlers[instrument])
            elif base_instrument in self._price_handlers and base_instrument != instrument:
                handlers.extend(self._price_handlers[base_instrument])

        if not handlers:
            logger.debug("No handlers registered for %s (base: %s)", instrument, base_instrument)
            return

        logger.debug("Dispatching price update for %s: %f to %d handlers", instrument, price, len(handlers))

        # Call all registered handlers for this instrument
        for handler in handlers:  # Use the list we already created
            try:
                if asyncio.iscoroutinefunction(handler):
                    # Use create_task for fire-and-forget
                    asyncio.create_task(handler(instrument, float(price)))
                else:
                    # Handle sync callbacks in a thread
                    await asyncio.get_event_loop().run_in_executor(
                        None, handler, instrument, float(price)
                    )
            except Exception as e:
                logger.error("Error in price update handler for %s: %s", instrument, e, exc_info=True)

    async def _handle_subscription_confirmation(self, instrument: str) -> None:
        """Handle subscription confirmation for an instrument.

        Args:
            instrument: The instrument that was confirmed
        """
        logger.info("Subscription confirmed for instrument: %s", instrument)

        # Update our tracking of subscribed instruments
        async with self._lock:
            self.subscribed_instruments.add(instrument)

        # Notify any waiting tasks about the subscription confirmation
        if hasattr(self, '_subscription_waiter') and not self._subscription_waiter.done():
            self._subscription_waiter.set_result(instrument)

        # Log the current state of subscriptions
        logger.debug("Current subscribed instruments: %s",
                    list(self.subscribed_instruments)[:10] +
                    (['...'] if len(self.subscribed_instruments) > 10 else []))

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
            if hedger_info.hedger.config.get('instrument_name') == instrument and not hedger_info.task:
                logger.info("Starting hedger for portfolio %s now that subscription is confirmed", portfolio_id)
                hedger_info.task = asyncio.create_task(hedger_info.hedger.start())

    async def _add_hedger(self, portfolio_id: str) -> bool:
        """Add a new hedger for the given portfolio.

        Args:
            portfolio_id: ID of the portfolio to add a hedger for

        Returns:
            bool: True if a new hedger was added, False if one already exists
        """
        async with self._lock:
            if portfolio_id in self.hedgers:
                logger.debug("Hedger already exists for portfolio: %s", portfolio_id)
                return False

            try:
                # Get the portfolio
                portfolio = await self.portfolio_manager.get_portfolio(portfolio_id)
                if not portfolio:
                    logger.error("Portfolio not found: %s", portfolio_id)
                    return False

                # Create a new hedger with the portfolio and default config
                config = HedgerConfig(**self.default_hedger_config) if self.default_hedger_config else HedgerConfig()
                hedger = DynamicDeltaHedger(config, portfolio, self.deribit_client)

                # Create and store the hedger info first
                self.hedgers[portfolio_id] = HedgerInfo(hedger=hedger)

                # Register for price updates before subscribing to avoid missing updates
                perpetual_instrument = f"{portfolio.underlying.upper()}-PERPETUAL"
                await self._register_price_handler(perpetual_instrument, hedger._price_callback)

                # Create a future to wait for subscription confirmation
                self._subscription_waiter = asyncio.Future()

                try:
                    # Subscribe to the instrument with a timeout
                    await asyncio.wait_for(
                        self.subscription_manager.add_subscription(portfolio_id, perpetual_instrument, wait_for_confirmation=True),
                        timeout=10.0
                    )

                    # Start the hedger after successful subscription
                    self.hedgers[portfolio_id].task = asyncio.create_task(hedger.start())

                    # Add a callback to handle task completion
                    def on_hedger_done(task):
                        try:
                            task.result()  # This will raise if the task failed
                        except asyncio.CancelledError:
                            logger.debug(f"Hedger for {portfolio_id} was cancelled")
                        except Exception as e:
                            logger.error(f"Hedger for {portfolio_id} failed: {e}", exc_info=True)
                        finally:
                            # Clean up the hedger
                            if portfolio_id in self.hedgers:
                                del self.hedgers[portfolio_id]

                    self.hedgers[portfolio_id].task.add_done_callback(on_hedger_done)

                    logger.info("Successfully added and started hedger for portfolio: %s", portfolio_id)
                    return True

                except asyncio.TimeoutError:
                    logger.error("Timed out waiting for subscription to %s", perpetual_instrument)
                    # Clean up the hedger we just added
                    if portfolio_id in self.hedgers:
                        del self.hedgers[portfolio_id]
                    return False

            except Exception as e:
                logger.error("Error adding hedger for portfolio %s: %s", portfolio_id, e, exc_info=True)
                # Ensure we clean up if anything goes wrong
                if portfolio_id in self.hedgers:
                    del self.hedgers[portfolio_id]
                return False

    async def _remove_hedger(self, portfolio_id: str) -> None:
        """Remove and stop the hedger for the specified portfolio.

        Args:
            portfolio_id: ID of the portfolio to remove the hedger for
        """
        async with self._lock:
            # Safely get and remove the hedger info if it exists
            hedger_info = self.hedgers.pop(portfolio_id, None)
            if hedger_info is None:
                return

            hedger = hedger_info.hedger
            task = hedger_info.task

            # Unregister all price handlers for this hedger
            if hasattr(hedger, '_price_callback') and hasattr(self, '_price_handlers'):
                for instrument_name in list(self._price_handlers.keys()):
                    if hedger._price_callback in self._price_handlers[instrument_name]:
                        self._unregister_price_handler(instrument_name, hedger._price_callback)

            # Clean up the hedger
            try:
                # First try to stop the hedger gracefully
                if hasattr(hedger, 'stop'):
                    await asyncio.wait_for(hedger.stop(), timeout=5.0)
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"Error stopping hedger for {portfolio_id}: {e}")

            # Cancel the task if it's still running
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                except Exception as e:
                    logger.warning(f"Error waiting for hedger task to complete for {portfolio_id}: {e}")

            logger.info(f"Removed hedger for portfolio {portfolio_id}")

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

    async def start_hedging(self, portfolio_id: str, timeout: float = 10.0) -> bool:
        """Start hedging for a specific portfolio.

        Args:
            portfolio_id: ID of the portfolio to start hedging for
            timeout: Maximum time to wait for the operation to complete

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
            success = await self._add_hedger(portfolio_id)

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
