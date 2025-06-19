"""
Portfolio manager for handling multiple portfolio instances.

This module provides the PortfolioManager class which manages multiple portfolio instances,
handling their lifecycle, persistence, and providing thread-safe operations.
"""

import asyncio
import json
import logging
import os
import uuid
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

        Args:
            event: The portfolio event to handle
        """
        portfolio = event.portfolio

        # Cancel any pending save for this portfolio
        if portfolio.id in self._save_tasks:
            self._save_tasks[portfolio.id].cancel()

        # Schedule a new save task with a delay
        self._save_tasks[portfolio.id] = asyncio.create_task(
            self._debounced_save(portfolio)
        )

    async def _debounced_save(self, portfolio: Portfolio) -> None:
        """Save the portfolio after a delay to batch multiple changes.

        Args:
            portfolio: The portfolio to save

        Note:
            If multiple changes occur within the save delay, only the last change
            will trigger a save.
        """
        try:
            await asyncio.sleep(self._save_delay)
            async with self._lock:
                await self._save_portfolio(portfolio.id, portfolio)
                if portfolio.id in self._save_tasks:
                    del self._save_tasks[portfolio.id]
        except asyncio.CancelledError:
            # Task was cancelled, which is expected when new changes occur
            logger.debug(
                "Debounced save cancelled for portfolio %s (new changes detected)",
                portfolio.id
            )
        except Exception as e:
            logger.error(
                "Error in debounced save for portfolio %s: %s",
                portfolio.id,
                str(e)
            )
        finally:
            if portfolio.id in self._save_tasks:
                del self._save_tasks[portfolio.id]

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
        if not portfolio:
            raise ValueError("Cannot save None portfolio")

        if portfolio.id != portfolio_id:
            raise ValueError("Portfolio ID mismatch")

        file_path = self._get_portfolio_path(portfolio_id)
        temp_path = file_path.with_suffix('.tmp')

        try:
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Serialize and write to temporary file
            portfolio_data = portfolio.to_dict()
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(portfolio_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            # Atomic rename (works on POSIX systems)
            temp_path.replace(file_path)

            # Mark the portfolio as clean after successful save
            portfolio.mark_clean()

            logger.debug(
                "Successfully saved portfolio %s to %s",
                portfolio_id, file_path
            )
            portfolio.mark_clean()
            logger.debug("Saved portfolio %s to %s", portfolio.id, file_path)

        except Exception as e:
            msg = f"Failed to save portfolio {portfolio.id} to {file_path}"
            logger.error("%s: %s", msg, str(e))
            raise

    async def initialize(self) -> None:
        """Initialize the portfolio manager and load all portfolios.

        This method:
        1. Initializes the async lock if not already done
        2. Loads all portfolios from disk
        3. Creates a default portfolio if none exist

        Raises:
            RuntimeError: If there's an error loading portfolios
        """
        if not hasattr(self, '_lock') or self._lock is None:
            self._lock = asyncio.Lock()

        try:
            await self.load_all_portfolios()

            # Create default portfolio if none exist
            if not self.portfolios:
                logger.info("No portfolios found, creating default portfolio")
                try:
                    await self.create_portfolio("default", "BTC")
                except Exception as e:
                    logger.error("Failed to create default portfolio: %s", str(e))
                    raise RuntimeError("Failed to initialize with default portfolio") from e

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
            ValueError: If the portfolio is None
        """
        if not portfolio:
            raise ValueError("Cannot set up listeners for None portfolio")

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
                    asyncio.create_task(
                        portfolio.add_listener(event_type, self._on_portfolio_event)
                    )

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
                if not hasattr(portfolio, 'options'):
                    portfolio.options = {}

                portfolio.options[option_instrument] = option

                # Calculate and hedge the initial premium if needed
                needs_hedging = not getattr(portfolio, 'initial_usd_hedged', False)
                if needs_hedging and current_price is not None:
                    try:
                        # Calculate premium in USD
                        if mark_price is not None:
                            premium_usd = mark_price * abs(quantity)
                            if contract_type_enum == ContractType.INVERSE:
                                # For inverse options, premium is in BTC, convert to USD
                                premium_usd = premium_usd * current_price

                            # Update portfolio with hedge details
                            portfolio.initial_usd_hedge_position = -premium_usd  # Negative since we're selling to hedge
                            portfolio.initial_usd_hedge_avg_entry = current_price
                            portfolio.initial_usd_hedged = True
                            logger.info(f"Hedged initial premium: ${premium_usd:.2f} at ${current_price:.2f}")
                    except Exception as e:
                        logger.error(f"Failed to hedge initial premium: {e}")

                # Save the updated portfolio
                await self._save_portfolio(portfolio_id, portfolio)
                logger.info(f"Added {quantity} of {option_instrument} to portfolio {portfolio_id}")
                return True

            except Exception as e:
                logger.error(f"Failed to add option to portfolio {portfolio_id}: {e}")
                return False
