"""
Portfolio manager for handling multiple portfolio instances.

This module provides the PortfolioManager class which manages multiple portfolio instances,
handling their lifecycle, persistence, and providing thread-safe operations.
"""

import asyncio
import json
import logging
import uuid
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime

from dneutral_sniper.models import OptionType, ContractType, VanillaOption
from dneutral_sniper.portfolio import Portfolio, PortfolioEvent, PortfolioEventType

logger = logging.getLogger(__name__)


class PortfolioManager:
    """Manages multiple portfolio instances with thread-safe operations.

    This class provides methods to create, load, save, and manage multiple
    portfolio instances. It handles persistence to disk and ensures thread-safe
    access to portfolios.
    """

    def __init__(self, data_dir: str = "portfolios", portfolios_dir: Optional[str] = None):
        """Initialize the portfolio manager.

        Args:
            data_dir: Directory where portfolio files will be stored
            portfolios_dir: Deprecated, use data_dir instead. Will be removed in a future version.
        """
        # For backward compatibility, allow both data_dir and portfolios_dir
        # but data_dir takes precedence if both are provided
        if portfolios_dir is not None and data_dir == "portfolios":
            import warnings
            warnings.warn(
                "The 'portfolios_dir' parameter is deprecated and will be removed in a future version. "
                "Use 'data_dir' instead.",
                DeprecationWarning,
                stacklevel=2
            )
            self.data_dir = Path(portfolios_dir)
        else:
            self.data_dir = Path(data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.portfolios: Dict[str, Portfolio] = {}
        self._lock = asyncio.Lock()
        self._save_tasks: Dict[str, asyncio.Task] = {}
        self._save_delay = 1.0  # seconds to wait before saving after last change

    async def _on_portfolio_event(self, event: PortfolioEvent) -> None:
        """Handle portfolio events.

        This method processes portfolio events and triggers appropriate actions
        such as saving the portfolio state when needed.

        Args:
            event: The portfolio event to handle
        """
        portfolio = event.portfolio
        logger.debug("Received portfolio event: %s for portfolio %s", event.event_type, portfolio.id)
        logger.debug("Event data: %s", event.data)

        # Check if we should schedule a save
        is_state_changed = event.event_type == PortfolioEventType.STATE_CHANGED
        is_option_added = event.event_type == PortfolioEventType.OPTION_ADDED
        is_subscription_confirmed = (
            is_state_changed and
            event.data and
            event.data.get('type') == 'subscription_confirmed'
        )

        # Check if the portfolio is dirty or if this is a subscription confirmation
        is_portfolio_dirty = portfolio.is_dirty()
        should_save = is_portfolio_dirty or is_option_added or is_subscription_confirmed

        logger.debug(
            "Save conditions - is_state_changed: %s, is_option_added: %s, "
            "is_subscription_confirmed: %s, is_portfolio_dirty: %s, should_save: %s",
            is_state_changed, is_option_added, is_subscription_confirmed,
            is_portfolio_dirty, should_save
        )

        if should_save:
            logger.debug("Save conditions met, proceeding with save scheduling")

            # Get existing task if it exists
            existing_task = self._save_tasks.get(portfolio.id)

            # If there's an existing task, cancel it
            if existing_task and not existing_task.done():
                logger.debug(
                    "Cancelling pending save task %s for portfolio %s",
                    existing_task.get_name(), portfolio.id
                )
                existing_task.cancel()
                try:
                    await asyncio.wait_for(existing_task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    logger.debug("Pending save task cancelled successfully")
                except Exception as e:
                    logger.warning("Error cancelling pending save task: %s", e, exc_info=True)

            # Create a new save task with a delay
            logger.debug(
                "Creating new save task for portfolio %s with delay %.1fs",
                portfolio.id, self._save_delay
            )

            # Create a task that will be awaited in _debounced_save
            task = asyncio.create_task(
                self._debounced_save(portfolio),
                name=f"save_{portfolio.id}_{int(time.time())}"
            )

            # Store the task for potential cancellation
            self._save_tasks[portfolio.id] = task
            logger.debug("Save task %s created for portfolio %s", task.get_name(), portfolio.id)

            # Add a callback to clean up the task reference when done
            def cleanup(task):
                if portfolio.id in self._save_tasks and self._save_tasks[portfolio.id] is task:
                    del self._save_tasks[portfolio.id]
                    logger.debug("Cleaned up save task for portfolio %s", portfolio.id)

            task.add_done_callback(cleanup)
        else:
            logger.debug("Not scheduling save - save conditions not met")

    async def _debounced_save(self, portfolio: Portfolio) -> None:
        """Save the portfolio after a delay to batch multiple changes.

        Args:
            portfolio: The portfolio to save

        Note:
            If multiple changes occur within the save delay, only the last change
            will trigger a save.
        """
        portfolio_id = portfolio.id
        logger.debug(
            "[_debounced_save] Starting debounced save for portfolio %s with delay %.1fs",
            portfolio_id, self._save_delay
        )

        try:
            # Wait for the debounce period
            await asyncio.sleep(self._save_delay)

            # Verify this is still the current task (not cancelled and replaced)
            async with self._lock:
                current_task = self._save_tasks.get(portfolio_id)
                if current_task is not asyncio.current_task():
                    logger.debug(
                        "[_debounced_save] Ignoring stale save task for portfolio %s (current: %s, expected: %s)",
                        portfolio_id,
                        current_task.get_name() if current_task else 'None',
                        asyncio.current_task().get_name() if asyncio.current_task() else 'None'
                    )
                    return

                logger.debug(
                    "[_debounced_save] Delay completed, saving portfolio %s",
                    portfolio_id
                )

            # Save the portfolio
            await self._save_portfolio(portfolio_id, portfolio)

            logger.debug(
                "[_debounced_save] Successfully saved portfolio %s",
                portfolio_id
            )

        except asyncio.CancelledError:
            # Task was cancelled, which is expected when new changes occur
            logger.debug(
                "[_debounced_save] Save was cancelled for portfolio %s (new changes detected)",
                portfolio_id
            )
            raise
        except Exception as e:
            logger.error(
                "[_debounced_save] Error saving portfolio %s: %s",
                portfolio_id, str(e),
                exc_info=True
            )
        finally:
            # Only clean up if this is still the current task
            async with self._lock:
                current_task = self._save_tasks.get(portfolio_id)
                if current_task is asyncio.current_task():
                    self._save_tasks.pop(portfolio_id, None)
                    logger.debug(
                        "[_debounced_save] Cleaned up save task for portfolio %s",
                        portfolio_id
                    )

    def _get_portfolio_path(self, portfolio_id: str) -> Path:
        """Get the file path for a portfolio.

        Args:
            portfolio_id: The ID of the portfolio

        Returns:
            Path: The full path to the portfolio file
        """
        return self.data_dir / f"portfolio_{portfolio_id}.json"

    async def _save_portfolio(self, portfolio_id: str, portfolio: Portfolio) -> None:
        """Save a portfolio to disk.

        This method saves the portfolio to disk using an atomic write pattern
        to prevent corruption. It first writes to a temporary file and then
        performs an atomic rename.

        Args:
            portfolio_id: ID of the portfolio to save
            portfolio: Portfolio instance to save

        Raises:
            ValueError: If portfolio is None or portfolio_id doesn't match
            IOError: If there's an error writing to disk
            json.JSONEncodeError: If the portfolio can't be serialized to JSON
        """
        logger.debug("[_save_portfolio] Starting to save portfolio %s", portfolio_id)

        if portfolio is None:
            error_msg = "Cannot save None portfolio"
            logger.error("[_save_portfolio] %s", error_msg)
            raise ValueError(error_msg)

        if portfolio.id != portfolio_id:
            error_msg = f"Portfolio ID mismatch: {portfolio.id} != {portfolio_id}"
            logger.error("[_save_portfolio] %s", error_msg)
            raise ValueError(error_msg)

        # Create a temporary file in the same directory as the target file
        file_path = self._get_portfolio_path(portfolio_id)
        temp_file = file_path.with_suffix('.tmp')

        logger.debug("[_save_portfolio] Will save to: %s (temp: %s)", file_path, temp_file)

        try:
            # Ensure the directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("[_save_portfolio] Directory ensured: %s", file_path.parent)

            # Serialize the portfolio to JSON
            logger.debug("[_save_portfolio] Serializing portfolio %s", portfolio_id)
            portfolio_data = portfolio.to_dict()
            logger.debug("[_save_portfolio] Portfolio data serialized, converting to JSON")

            # Debug: Log the options being saved
            if 'options' in portfolio_data:
                logger.debug(
                    "[_save_portfolio] Options to be saved: %s",
                    [opt['instrument_name'] for opt in portfolio_data['options']]
                )

            json_data = json.dumps(portfolio_data, indent=2, default=str)
            logger.debug("[_save_portfolio] JSON data prepared, size: %d bytes", len(json_data))

            # Write to temporary file
            logger.debug("[_save_portfolio] Writing to temp file: %s", temp_file)
            with temp_file.open('w') as f:
                f.write(json_data)
            logger.debug("[_save_portfolio] Successfully wrote to temp file")

            # Atomically replace the target file
            logger.debug("[_save_portfolio] Renaming temp file to: %s", file_path)
            temp_file.replace(file_path)

            # Mark the portfolio as clean after successful save
            portfolio.mark_clean()
            logger.info("[_save_portfolio] Successfully saved portfolio %s to %s", portfolio_id, file_path)

        except json.JSONEncodeError as je:
            logger.error("[_save_portfolio] JSON encode error for portfolio %s: %s", portfolio_id, je, exc_info=True)
            if 'portfolio_data' in locals():
                logger.debug("[_save_portfolio] Problematic portfolio data: %s", portfolio_data)
            raise
        except OSError as oe:
            logger.error("[_save_portfolio] OS error saving portfolio %s: %s", portfolio_id, oe, exc_info=True)
            logger.debug("[_save_portfolio] File path: %s, Temp path: %s, Parent exists: %s",
                         file_path, temp_file, file_path.parent.exists())
            raise
        except Exception as e:
            logger.error("[_save_portfolio] Unexpected error saving portfolio %s: %s", portfolio_id, e, exc_info=True)
            raise
        finally:
            # Clean up the temporary file if it exists and wasn't moved
            if temp_file.exists():
                try:
                    logger.debug("[_save_portfolio] Cleaning up temp file: %s", temp_file)
                    temp_file.unlink()
                except Exception as e:
                    logger.warning("[_save_portfolio] Error cleaning up temp file %s: %s", temp_file, e)

    async def initialize(self) -> None:
        """Initialize the portfolio manager and load all portfolios.

        This method:
        1. Initializes the async lock if not already done
        2. Loads all portfolios from disk

        Raises:
            RuntimeError: If there's an error loading portfolios
        """
        if not hasattr(self, '_lock') or self._lock is None:
            self._lock = asyncio.Lock()

        try:
            await self.load_all_portfolios()

        except Exception as e:
            logger.error("Failed to initialize portfolio manager: %s", str(e))
            raise RuntimeError("Failed to initialize portfolio manager") from e

    async def _setup_portfolio_listeners(self, portfolio: Portfolio) -> None:
        """Set up event listeners for a portfolio.

        This method registers the portfolio manager's event handler for all
        portfolio event types, enabling automatic saving on state changes.

        Args:
            portfolio: The portfolio to set up listeners for

        Raises:
            Exception: If failed to setup listeners
        """

        try:
            # Add listener for all event types
            for event_type in PortfolioEventType:
                await portfolio.add_listener(event_type, self._on_portfolio_event)

            logger.debug("Set up event listeners for portfolio %s", portfolio.id)
        except Exception as e:
            logger.error(
                "Failed to set up listeners for portfolio %s: %s",
                getattr(portfolio, 'id', 'unknown'),
                str(e)
            )
            raise

    async def load_all_portfolios(self) -> None:
        """Load all portfolios from the data directory and set up event listeners.

        This method:
        1. Clears the current in-memory portfolio cache
        2. Scans the data directory for portfolio JSON files
        3. Loads and validates each portfolio
        4. Sets up event listeners for each portfolio

        Raises:
            RuntimeError: If the data directory is inaccessible or if there's an error
                loading portfolios
        """
        if not self.data_dir.exists():
            logger.info("Data directory %s does not exist, creating it", self.data_dir)
            try:
                self.data_dir.mkdir(parents=True, exist_ok=True)
                return
            except OSError as e:
                error_msg = f"Failed to create data directory {self.data_dir}: {e}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from e

        if not self.data_dir.is_dir():
            error_msg = f"Data directory path {self.data_dir} is not a directory"
            logger.error(error_msg)
            raise NotADirectoryError(error_msg)

        async with self._lock:
            self.portfolios.clear()
            loaded_count = 0
            error_count = 0

            try:
                portfolio_files = list(self.data_dir.glob("portfolio_*.json"))
                logger.debug("Found %d portfolio files to load", len(portfolio_files))

                for file_path in portfolio_files:
                    try:
                        logger.debug("Loading portfolio from %s", file_path)

                        # Load the portfolio from file
                        portfolio = Portfolio.load_from_file(str(file_path))
                        if not portfolio or not hasattr(portfolio, 'id'):
                            logger.error("Invalid portfolio data in %s: missing or invalid ID", file_path)
                            error_count += 1
                            continue

                        portfolio_id = portfolio.id

                        # Check for duplicate portfolio IDs
                        if portfolio_id in self.portfolios:
                            logger.warning(
                                "Duplicate portfolio ID %s found in %s, skipping",
                                portfolio_id, file_path
                            )
                            continue

                        # Set up event listeners
                        await self._setup_portfolio_listeners(portfolio)

                        # Add to in-memory cache
                        self.portfolios[portfolio_id] = portfolio
                        loaded_count += 1
                        logger.info(
                            "Successfully loaded portfolio %s from %s",
                            portfolio_id, file_path
                        )

                    except json.JSONDecodeError as e:
                        logger.error(
                            "Invalid JSON in portfolio file %s: %s",
                            file_path, str(e)
                        )
                        error_count += 1
                    except ValueError as e:
                        logger.error(
                            "Invalid portfolio data in %s: %s",
                            file_path, str(e)
                        )
                        error_count += 1
                    except Exception as e:
                        logger.error(
                            "Unexpected error loading portfolio from %s: %s",
                            file_path, str(e), exc_info=True
                        )
                        error_count += 1

                logger.info(
                    "Portfolio loading complete. Success: %d, Failed: %d",
                    loaded_count, error_count
                )

                if error_count > 0 and loaded_count == 0:
                    raise RuntimeError(
                        f"Failed to load any portfolios. {error_count} errors occurred."
                    )

            except Exception as e:
                logger.error("Unexpected error during portfolio loading: %s", str(e), exc_info=True)
                raise RuntimeError("Failed to load portfolios") from e

    async def create_portfolio(
        self,
        portfolio_id: Optional[str] = None,
        underlying: str = "BTC",
        initial_balance: float = 0.0
    ) -> Tuple[str, Portfolio]:
        """Create a new portfolio with a unique ID.

        This method creates a new portfolio with the specified parameters and sets up
        the necessary event listeners for automatic persistence.

        Args:
            portfolio_id: Optional custom ID for the portfolio. If None, a UUID will be generated.
            underlying: The underlying asset for this portfolio (e.g., 'BTC', 'ETH')
            initial_balance: Initial balance in USD

        Returns:
            A tuple of (portfolio_id, portfolio)

        Raises:
            ValueError: If the portfolio_id already exists or is invalid
            RuntimeError: If there's an error creating the portfolio
        """
        async with self._lock:
            # Generate a unique ID if not provided
            if portfolio_id is None:
                portfolio_id = str(uuid.uuid4())

            # Ensure ID is unique
            if portfolio_id in self.portfolios:
                raise ValueError(f"Portfolio with ID {portfolio_id} already exists")

            try:
                # Create the portfolio with the ID
                portfolio = Portfolio(portfolio_id=portfolio_id, underlying=underlying)
                portfolio.initial_balance = initial_balance

                # Set default values
                portfolio.futures_position = 0.0
                portfolio.futures_avg_entry = 0.0
                portfolio.last_hedge_price = None
                portfolio.realized_pnl = 0.0
                portfolio.initial_option_usd_value = {}
                portfolio.trades = []
                portfolio.initial_usd_hedged = False
                portfolio.initial_usd_hedge_position = 0.0
                portfolio.initial_usd_hedge_avg_entry = 0.0

                # Set up event listeners
                for event_type in PortfolioEventType:
                    # Don't use create_task here, just await the coroutine directly
                    await portfolio.add_listener(event_type, self._on_portfolio_event)

                # Save the portfolio
                await self._save_portfolio(portfolio_id, portfolio)

                # Add to in-memory cache
                self.portfolios[portfolio_id] = portfolio

                logger.info("Created new portfolio %s", portfolio_id)
                return portfolio_id, portfolio

            except Exception as e:
                logger.error("Failed to create portfolio %s: %s", portfolio_id, str(e))
                raise RuntimeError(f"Failed to create portfolio: {e}") from e

    async def get_portfolio(self, portfolio_id: str) -> Optional[Portfolio]:
        """Get a portfolio by ID.

        Args:
            portfolio_id: ID of the portfolio to retrieve

        Returns:
            The Portfolio instance, or None if not found
        """
        # Use the non-async version since we're just doing a simple dict lookup
        return await self.get_portfolio_by_id(portfolio_id)

    async def delete_portfolio(self, portfolio_id: str) -> bool:
        """Delete a portfolio by ID.

        Args:
            portfolio_id: ID of the portfolio to delete

        Returns:
            True if the portfolio was deleted, False if not found
        """
        async with self._lock:
            if portfolio_id not in self.portfolios:
                return False

            # Remove the portfolio file
            file_path = self.data_dir / f"portfolio_{portfolio_id}.json"
            if file_path.exists():
                file_path.unlink()

            # Remove the portfolio from memory
            del self.portfolios[portfolio_id]

            # Cancel any pending save for this portfolio
            if portfolio_id in self._save_tasks:
                self._save_tasks[portfolio_id].cancel()
                del self._save_tasks[portfolio_id]

            logger.info(f"Deleted portfolio {portfolio_id}")
            return True

    async def list_portfolios(self) -> List[str]:
        """List all portfolio IDs.

        Returns:
            List of portfolio IDs as strings
        """
        async with self._lock:
            return list(self.portfolios.keys())

    async def save_portfolio(self, portfolio_id: str) -> bool:
        """Save a portfolio to disk.

        Args:
            portfolio_id: ID of the portfolio to save

        Returns:
            True if saved successfully, False otherwise
        """
        async with self._lock:
            if portfolio_id not in self.portfolios:
                logger.warning(f"Cannot save non-existent portfolio {portfolio_id}")
                return False

            portfolio = self.portfolios[portfolio_id]
            try:
                await self._save_portfolio(portfolio_id, portfolio)
                return True
            except Exception as e:
                logger.error(f"Failed to save portfolio {portfolio_id}: {e}")
                return False

    async def get_portfolio_by_id(self, portfolio_id: str) -> Optional[Portfolio]:
        """Get a portfolio by ID.

        Args:
            portfolio_id: ID of the portfolio to retrieve

        Returns:
            The Portfolio instance, or None if not found
        """
        return self.portfolios.get(portfolio_id)

    async def save_all_portfolios(self) -> None:
        """Save all portfolios to disk"""
        async with self._lock:
            for portfolio_id, portfolio in self.portfolios.items():
                if portfolio.is_dirty():
                    await self._save_portfolio(portfolio_id, portfolio)

    async def close(self) -> None:
        """Clean up resources and save all portfolios.

        This method ensures all pending save tasks are properly cancelled and awaited,
        and all dirty portfolios are saved before closing.
        """
        # Create a copy of tasks to avoid modification during iteration
        tasks = list(self._save_tasks.values())

        # Cancel all pending save tasks
        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete with a timeout
        if tasks:
            done, pending = await asyncio.wait(
                tasks,
                timeout=5.0,
                return_when=asyncio.ALL_COMPLETED
            )

            # Log any pending tasks that didn't complete
            if pending:
                logger.warning(
                    "%d save tasks did not complete within timeout, forcing cancellation",
                    len(pending)
                )
                for task in pending:
                    if not task.done():
                        task.cancel()

        # Save any remaining dirty portfolios
        try:
            await self.save_all_portfolios()
        except Exception as e:
            logger.error("Error saving portfolios during close: %s", str(e))

        # Clear the cache and task references
        self.portfolios.clear()
        self._save_tasks.clear()

        logger.debug("PortfolioManager closed successfully")

    async def get_subscribed_instruments(self, portfolio_id: str) -> Set[str]:
        """Get the set of instruments a portfolio is subscribed to.

        Args:
            portfolio_id: ID of the portfolio

        Returns:
            Set of instrument names (perpetual and options)
        """
        portfolio = await self.get_portfolio(portfolio_id)
        if not portfolio:
            return set()

        # Get perpetual instrument name (e.g., "BTC-PERPETUAL")
        underlying = getattr(portfolio, 'underlying', 'BTC')
        perpetual = f"{underlying.upper()}-PERPETUAL"

        # Get all option instrument names
        options = getattr(portfolio, 'options', {})
        option_instruments = {opt.instrument_name for opt in options.values()}

        return {perpetual} | option_instruments

    async def add_option_to_portfolio(
        self,
        portfolio_id: str,
        option_instrument: str,
        quantity: float,
        strike: float,
        expiry: str,
        option_type: str,
        contract_type: str = "inverse",
        mark_price: Optional[float] = None,
        iv: Optional[float] = None,
        usd_value: Optional[float] = None,
        delta: Optional[float] = None,
        current_price: Optional[float] = None
    ) -> bool:
        """Add an option to a portfolio.

        Args:
            portfolio_id: ID of the portfolio
            option_instrument: Full instrument name (e.g., "BTC-28JUN24-60000-C")
            quantity: Number of contracts (positive for long, negative for short)
            strike: Strike price
            expiry: Expiration date in ISO format (YYYY-MM-DD)
            option_type: "call" or "put"
            contract_type: "inverse" or "standard"
            mark_price: Optional mark price in USD
            iv: Optional implied volatility
            usd_value: Optional USD value of the position
            delta: Optional delta of the option

        Returns:
            True if the option was added successfully, False otherwise
        """

        async with self._lock:
            if portfolio_id not in self.portfolios:
                logger.error(f"Portfolio {portfolio_id} not found")
                return False

            portfolio = self.portfolios[portfolio_id]

            try:
                # Parse inputs
                expiry_dt = datetime.fromisoformat(expiry)
                option_type_enum = OptionType(option_type.lower())
                contract_type_enum = ContractType(contract_type.lower())

                # Get the underlying from the portfolio or default to 'BTC'
                underlying = getattr(portfolio, 'underlying', 'BTC')

                # Create the option
                option = VanillaOption(
                    instrument_name=option_instrument,
                    option_type=option_type_enum,
                    strike=strike,
                    expiry=expiry_dt,
                    quantity=quantity,
                    underlying=underlying,
                    contract_type=contract_type_enum,
                    mark_price=mark_price,
                    iv=iv,
                    usd_value=usd_value,
                    delta=delta
                )

                # Add to portfolio
                await portfolio.add_option(option)

                logger.info(f"Added {quantity} of {option_instrument} to portfolio {portfolio_id}")
                return True

            except Exception as e:
                logger.error(f"Failed to add option to portfolio {portfolio_id}: {e}")
                return False
