"""Subscription manager for handling instrument subscriptions.

This module provides the SubscriptionManager class which manages instrument
subscriptions for multiple portfolios, allowing efficient lookup of
subscriptions and notification of relevant portfolio managers when new data
arrives.
"""
from collections import defaultdict
import asyncio
import logging
from typing import Dict, Set, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .deribit_client import DeribitWebsocketClient

logger = logging.getLogger(__name__)


class SubscriptionManager:
    """Manages instrument subscriptions for multiple portfolios.

    This class maintains a mapping of instrument names to the set of portfolio
    IDs that are subscribed to them, and vice versa. It provides thread-safe
    methods for managing subscriptions and querying subscription status.
    """

    def __init__(self, deribit_client: Optional['DeribitWebsocketClient'] = None):
        """Initialize the SubscriptionManager with empty mappings.
        
        Args:
            deribit_client: Optional DeribitWebsocketClient instance for direct subscription management
        """
        # Map from instrument name to set of portfolio IDs
        self.instrument_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        # Map from portfolio ID to set of instrument names
        self.portfolio_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()
        # Reference to Deribit client for direct subscription management
        self.deribit_client = deribit_client
        # Track all subscribed instruments across all portfolios
        self._all_subscribed_instruments: Set[str] = set()
        # Track pending subscription confirmations
        self._pending_subscriptions: Set[str] = set()
        # Event triggered when a subscription is confirmed
        self._subscription_confirmed = asyncio.Event()

    async def add_subscription(
        self, portfolio_id: str, instrument: str, wait_for_confirmation: bool = False
    ) -> bool:
        """Add a subscription for a portfolio to an instrument.

        Args:
            portfolio_id: ID of the portfolio to add the subscription for
            instrument: Name of the instrument to subscribe to
            wait_for_confirmation: If True, wait for subscription confirmation before returning

        Returns:
            bool: True if the subscription was added, False if it existed or failed
        """
        async with self._lock:
            # Check if this portfolio is already subscribed to this instrument
            if instrument in self.portfolio_subscriptions[portfolio_id]:
                logger.debug(
                    "Portfolio %s already subscribed to %s",
                    portfolio_id, instrument
                )
                return False

            # Add to our tracking
            self.instrument_subscriptions[instrument].add(portfolio_id)
            self.portfolio_subscriptions[portfolio_id].add(instrument)
            
            # Track if this is a new instrument that needs subscribing to
            is_new_instrument = instrument not in self._all_subscribed_instruments
            if is_new_instrument:
                self._all_subscribed_instruments.add(instrument)

            logger.info(
                "Added subscription: portfolio=%s, instrument=%s (new_instrument=%s)",
                portfolio_id, instrument, is_new_instrument
            )
            
            # If we have a Deribit client and this is a new instrument, subscribe to it
            if self.deribit_client and is_new_instrument:
                try:
                    logger.info("Subscribing to instrument: %s", instrument)
                    self._subscription_confirmed.clear()
                    self._pending_subscriptions.add(instrument)
                    
                    # Subscribe and wait for confirmation if requested
                    success = await self.deribit_client.subscribe_to_instruments([instrument])
                    
                    if wait_for_confirmation:
                        logger.debug("Waiting for subscription confirmation for %s", instrument)
                        try:
                            # Wait for confirmation with a timeout
                            await asyncio.wait_for(self._subscription_confirmed.wait(), timeout=5.0)
                            logger.info("Subscription confirmed for %s", instrument)
                        except asyncio.TimeoutError:
                            logger.warning("Timeout waiting for subscription confirmation for %s", instrument)
                            success = False
                    
                    if not success:
                        logger.error("Failed to subscribe to instrument: %s", instrument)
                        # Clean up if subscription failed
                        self._all_subscribed_instruments.discard(instrument)
                        self.instrument_subscriptions[instrument].discard(portfolio_id)
                        self.portfolio_subscriptions[portfolio_id].discard(instrument)
                        if not self.instrument_subscriptions[instrument]:
                            del self.instrument_subscriptions[instrument]
                        if not self.portfolio_subscriptions[portfolio_id]:
                            del self.portfolio_subscriptions[portfolio_id]
                        return False
                        
                    return success
                    
                except Exception as e:
                    logger.error(
                        "Failed to subscribe to instrument %s: %s",
                        instrument, str(e), exc_info=True
                    )
                    # Clean up if subscription failed
                    self._all_subscribed_instruments.discard(instrument)
                    self.instrument_subscriptions[instrument].discard(portfolio_id)
                    self.portfolio_subscriptions[portfolio_id].discard(instrument)
                    if not self.instrument_subscriptions[instrument]:
                        del self.instrument_subscriptions[instrument]
                    if not self.portfolio_subscriptions[portfolio_id]:
                        del self.portfolio_subscriptions[portfolio_id]
                    return False
            
            return True

    async def handle_subscription_confirmation(self, instrument: str) -> None:
        """Handle subscription confirmation for an instrument.
        
        Args:
            instrument: Name of the instrument that was confirmed
        """
        async with self._lock:
            if instrument in self._pending_subscriptions:
                self._pending_subscriptions.remove(instrument)
                self._subscription_confirmed.set()
                logger.info("Subscription confirmed for instrument: %s", instrument)
    
    async def remove_subscription(
        self, portfolio_id: str, instrument: str
    ) -> bool:
        """Remove a subscription for a portfolio from an instrument.

        Args:
            portfolio_id: ID of the portfolio to remove the subscription for
            instrument: Name of the instrument to unsubscribe from

        Returns:
            bool: True if subscription was removed, False if it didn't exist
        """
        async with self._lock:
            # Check if this portfolio is actually subscribed to this instrument
            if instrument not in self.portfolio_subscriptions[portfolio_id]:
                logger.debug(
                    "Portfolio %s is not subscribed to %s",
                    portfolio_id, instrument
                )
                return False

            # Remove from our tracking
            self.portfolio_subscriptions[portfolio_id].discard(instrument)
            self.instrument_subscriptions[instrument].discard(portfolio_id)

            # Track if this was the last subscription to this instrument
            is_last_subscriber = not bool(self.instrument_subscriptions[instrument])
            
            # Clean up empty sets
            if not self.portfolio_subscriptions[portfolio_id]:
                del self.portfolio_subscriptions[portfolio_id]
            if not self.instrument_subscriptions[instrument]:
                del self.instrument_subscriptions[instrument]
                if instrument in self._all_subscribed_instruments:
                    self._all_subscribed_instruments.remove(instrument)

            logger.info(
                "Removed subscription: portfolio=%s, instrument=%s (last_subscriber=%s)",
                portfolio_id, instrument, is_last_subscriber
            )
            
            # Note: We don't actually unsubscribe from Deribit here since other portfolios
            # might still be interested in the same instrument. The WebSocket connection
            # maintains the subscription until all interested parties are gone.
            
            return True

    async def get_portfolio_subscriptions(self, portfolio_id: str) -> Set[str]:
        """Get all instruments a portfolio is subscribed to.

        Args:
            portfolio_id: ID of the portfolio

        Returns:
            Set of instrument names the portfolio is subscribed to
        """
        async with self._lock:
            return set(self.portfolio_subscriptions.get(portfolio_id, set()))

    async def get_subscribed_portfolios(self, instrument: str) -> Set[str]:
        """Get all portfolios subscribed to a specific instrument.

        Args:
            instrument: Name of the instrument

        Returns:
            Set of portfolio IDs subscribed to the instrument
        """
        async with self._lock:
            return set(self.instrument_subscriptions.get(
                instrument, set()
            ))

    async def get_all_subscribed_instruments(self) -> Set[str]:
        """Get all instruments that have at least one subscriber.

        Returns:
            Set of all subscribed instrument names
        """
        async with self._lock:
            return set(self.instrument_subscriptions.keys())

    async def get_all_portfolios(self) -> Set[str]:
        """Get all portfolios that have at least one subscription.

        Returns:
            Set of all portfolio IDs with subscriptions
        """
        async with self._lock:
            return set(self.portfolio_subscriptions.keys())

    async def update_portfolio_subscriptions(
        self,
        portfolio_id: str,
        new_instruments: Set[str],
        remove_old: bool = True
    ) -> None:
        """Update all subscriptions for a portfolio in a single operation.

        This is more efficient than adding/removing subscriptions one by one
        when updating multiple instruments at once.

        Args:
            portfolio_id: ID of the portfolio to update
            new_instruments: Set of instrument names the portfolio should be
                subscribed to
            remove_old: If True, remove subscriptions not in new_instruments
        """
        async with self._lock:
            curr = set(
                self.portfolio_subscriptions.get(portfolio_id, set())
            )
            to_add = new_instruments - curr
            to_remove = curr - new_instruments if remove_old else set()

            # Add new subscriptions
            for instrument in to_add:
                self.instrument_subscriptions[instrument].add(portfolio_id)
                self.portfolio_subscriptions[portfolio_id].add(instrument)
                logger.debug(
                    "Added subscription: portfolio=%s, instrument=%s",
                    portfolio_id, instrument
                )

            # Remove old subscriptions if requested
            for instrument in to_remove:
                self.portfolio_subscriptions[portfolio_id].discard(instrument)
                self.instrument_subscriptions[instrument].discard(portfolio_id)
                logger.debug(
                    "Removed subscription: portfolio=%s, instrument=%s",
                    portfolio_id, instrument
                )

                # Clean up empty sets
                if not self.instrument_subscriptions[instrument]:
                    del self.instrument_subscriptions[instrument]

            # Clean up empty portfolio entries
            if (
                remove_old
                and portfolio_id in self.portfolio_subscriptions
                and not self.portfolio_subscriptions[portfolio_id]
            ):
                del self.portfolio_subscriptions[portfolio_id]

    async def clear_portfolio_subscriptions(self, portfolio_id: str) -> None:
        """Remove all subscriptions for a portfolio.

        Args:
            portfolio_id: ID of the portfolio to clear
        """
        await self.update_portfolio_subscriptions(portfolio_id, set())
