from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from dneutral_sniper.models import ContractType, OptionType, VanillaOption

logger = logging.getLogger(__name__)


class PortfolioEventType(Enum):
    OPTION_ADDED = auto()
    OPTION_UPDATED = auto()
    OPTION_REMOVED = auto()
    FUTURES_POSITION_UPDATED = auto()
    INITIAL_HEDGE_UPDATED = auto()
    REALIZED_PNL_UPDATED = auto()
    STATE_CHANGED = auto()


@dataclass
class PortfolioEvent:
    event_type: PortfolioEventType
    portfolio: 'Portfolio'
    data: Optional[Dict[str, Any]] = None


class EventEmitter:
    def __init__(self):
        self._listeners: Dict[PortfolioEventType, List[Callable[[PortfolioEvent], None]]] = {}
        self._lock = asyncio.Lock()

    async def add_listener(
        self,
        event_type: PortfolioEventType,
        callback: Callable[[PortfolioEvent], None]
    ) -> None:
        """Add a listener for a specific event type.

        Args:
            event_type: The type of event to listen for
            callback: Function to call when the event occurs
        """
        async with self._lock:
            if event_type not in self._listeners:
                self._listeners[event_type] = []
            self._listeners[event_type].append(callback)

    async def remove_listener(
        self,
        event_type: PortfolioEventType,
        callback: Callable[[PortfolioEvent], None]
    ) -> None:
        """Remove a listener for a specific event type.

        Args:
            event_type: The event type to remove listener from
            callback: The callback function to remove
        """
        async with self._lock:
            if event_type in self._listeners:
                self._listeners[event_type].remove(callback)

    async def emit(self, event: PortfolioEvent) -> None:
        """Emit an event to all registered listeners.

        Args:
            event: The event to emit
        """
        logger = logging.getLogger(__name__)

        # Log the event being emitted
        logger.debug(f"Emitting event: {event.event_type}")

        async with self._lock:
            listeners = self._listeners.get(event.event_type, []).copy()
            logger.debug(f"Found {len(listeners)} listeners for {event.event_type}")

        # Call all listeners concurrently
        if listeners:
            logger.debug(f"Dispatching to {len(listeners)} listeners")
            results = await asyncio.gather(
                *[listener(event) for listener in listeners],
                return_exceptions=True
            )

            # Log any exceptions from listeners
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Error in event listener {i} for {event.event_type}: {result}",
                        exc_info=result
                    )
        else:
            logger.debug(f"No listeners for event: {event.event_type}")


class Portfolio(EventEmitter):
    def to_dict(self) -> Dict[str, Any]:
        """Convert the portfolio to a dictionary.

        Returns:
            Dictionary containing all portfolio data
        """
        return {
            "portfolio_id": self.id,
            "underlying": getattr(self, 'underlying', None),
            "initial_balance": getattr(self, 'initial_balance', 0.0),
            "futures_position": getattr(self, 'futures_position', 0.0),
            "futures_avg_entry": getattr(self, 'futures_avg_entry', 0.0),
            "last_hedge_price": getattr(self, 'last_hedge_price', None),
            "realized_pnl": getattr(self, 'realized_pnl', 0.0),
            "initial_option_usd_value": getattr(
                self, 'initial_option_usd_value', {}
            ),
            "trades": getattr(self, 'trades', []),
            "initial_usd_hedged": getattr(self, 'initial_usd_hedged', False),
            "initial_usd_hedge_position": getattr(
                self, 'initial_usd_hedge_position', 0.0
            ),
            "initial_usd_hedge_avg_entry": getattr(
                self, 'initial_usd_hedge_avg_entry', 0.0
            ),
            "options": [
                self._serialize_option(option)
                for option in getattr(self, 'options', {}).values()
                if option is not None
            ]
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Portfolio':
        """Create a Portfolio instance from a dictionary.

        Args:
            data: Dictionary containing portfolio data

        Returns:
            New Portfolio instance

        Raises:
            ValueError: If required fields are missing or invalid
        """
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")

        portfolio_id = data.get('portfolio_id')
        portfolio = cls(portfolio_id=portfolio_id)

        # Set basic fields
        basic_fields = [
            'underlying',
            'initial_balance',
            'futures_position',
            'futures_avg_entry',
            'last_hedge_price',
            'realized_pnl',
            'initial_option_usd_value',
            'trades',
            'initial_usd_hedged',
            'initial_usd_hedge_position',
            'initial_usd_hedge_avg_entry'
        ]

        for field in basic_fields:
            if field in data:
                setattr(portfolio, field, data[field])

        # Load options if present
        if 'options' in data and isinstance(data['options'], list):
            for option_data in data['options']:
                try:
                    option = cls._deserialize_option(option_data)
                    if option:
                        if not hasattr(portfolio, 'options'):
                            portfolio.options = {}
                        portfolio.options[option.instrument_name] = option
                        # We can't directly await here as this is called from a non-async context
                        # The option is already added to the dict, and events will be emitted on the next operation
                except (KeyError, ValueError, TypeError) as e:
                    logger.error(
                        "Failed to load option %s: %s",
                        option_data.get('instrument_name', 'unknown'),
                        str(e)
                    )
                    continue

        portfolio.mark_clean()
        return portfolio

    @classmethod
    def load_from_file(cls, filename: str) -> 'Portfolio':
        """Load a portfolio from a JSON file.

        Args:
            filename: Path to the portfolio JSON file

        Returns:
            Loaded Portfolio instance

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
            ValueError: If the data is invalid or corrupted
        """
        try:
            file_path = Path(filename)
            if not file_path.exists():
                raise FileNotFoundError(f"Portfolio file not found: {filename}")

            with file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)

            # Create portfolio from the loaded data
            portfolio = cls.from_dict(data)
            logger.debug("Successfully loaded portfolio from %s", filename)
            return portfolio

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in portfolio file %s: %s", filename, str(e))
            raise
        except Exception as e:
            logger.error(
                "Unexpected error loading portfolio from %s: %s",
                filename,
                str(e),
                exc_info=True
            )
            raise

    @classmethod
    def _deserialize_option(cls, option_data: Dict[str, Any]) -> Optional[VanillaOption]:
        """Deserialize an option from a dictionary.

        Args:
            option_data: Dictionary containing option data

        Returns:
            Deserialized VanillaOption instance, or None if deserialization fails

        Raises:
            KeyError: If required fields are missing
            ValueError: If data is invalid or expiry is not a datetime
        """
        try:
            if not isinstance(option_data, dict):
                raise ValueError("Option data must be a dictionary")

            required_fields = [
                'instrument_name',
                'quantity',
                'strike',
                'expiry',
                'option_type',
                'underlying',
                'contract_type'
            ]

            for field in required_fields:
                if field not in option_data:
                    raise ValueError(f"Missing required field: {field}")

            # Ensure expiry is a valid datetime object
            expiry = option_data['expiry']
            if not isinstance(expiry, datetime):
                if isinstance(expiry, str):
                    try:
                        # Parse ISO format string to datetime
                        expiry = datetime.fromisoformat(expiry)
                    except (ValueError, TypeError) as err:
                        raise ValueError(
                            f"Invalid expiry format. Must be ISO format datetime string or datetime object: {err}"
                        )
                else:
                    raise ValueError("Expiry must be a datetime object or ISO format datetime string")

            # Ensure expiry is timezone-aware
            if expiry.tzinfo is None:
                logger.warning(
                    "Expiry datetime is timezone-naive, assuming UTC: %s",
                    expiry
                )
                # Make it timezone-aware by assuming UTC
                expiry = expiry.replace(tzinfo=timezone.utc)

            # Validate required fields
            quantity = float(option_data['quantity'])
            if quantity == 0:
                raise ValueError("Option quantity cannot be zero")

            strike = float(option_data['strike'])
            if strike <= 0:
                raise ValueError(f"Strike price must be positive, got {strike}")

            # Ensure expiry is in the future
            if expiry <= datetime.now(timezone.utc):
                logger.warning(f"Option {option_data['instrument_name']} has already expired: {expiry}")

            # Create and return the option with optional fields
            option_args = {
                'instrument_name': option_data['instrument_name'],
                'quantity': quantity,
                'strike': strike,
                'expiry': expiry,
                'option_type': OptionType(option_data['option_type']),
                'underlying': option_data['underlying'],
                'contract_type': ContractType(option_data['contract_type']),
                'mark_price': (
                    float(option_data.get('mark_price', 0.0))
                    if option_data.get('mark_price') is not None
                    else None
                ),
                'iv': (
                    float(option_data['iv'])
                    if 'iv' in option_data and option_data['iv'] is not None
                    else None
                ),
                'usd_value': (
                    float(option_data['usd_value'])
                    if 'usd_value' in option_data and option_data['usd_value'] is not None
                    else None
                ),
                'delta': (
                    float(option_data['delta'])
                    if 'delta' in option_data and option_data['delta'] is not None
                    else None
                )
            }

            logger.debug("Creating VanillaOption with args: %s",
                         {k: v for k, v in option_args.items() if k != 'expiry'})

            try:
                return VanillaOption(**option_args)
            except Exception as e:
                raise ValueError(f"Failed to create VanillaOption: {str(e)}") from e

        except (ValueError, TypeError) as e:
            inst_name = option_data.get('instrument_name', 'unknown')
            logger.error("Invalid option data: %s - %s", inst_name, str(e))
            raise

    def __init__(self, portfolio_id: Optional[str] = None, underlying: Optional[str] = None):
        super().__init__()
        self.id = portfolio_id or str(uuid.uuid4())
        self._underlying = underlying  # The underlying asset (e.g., 'BTC', 'ETH')
        self.options: Dict[str, VanillaOption] = {}
        self._total_delta: Optional[float] = None
        # Net position in futures (USD notional, dynamic delta hedge only)
        self._futures_position: float = 0.0
        # Average entry price for futures (dynamic hedge)
        self._futures_avg_entry: float = 0.0
        self._last_hedge_price: Optional[float] = None
        self._realized_pnl: float = 0.0
        # instrument_name -> [need to be hedged USD value, actual initial hedged USD value]
        self.initial_option_usd_value: Dict[str, List[float]] = {}
        # List of trade dicts for PNL charting
        self.trades: List[Dict[str, Any]] = []
        # Flag for initial USD notional hedge phase
        self._initial_usd_hedged: bool = False
        # Static hedge position (USD notional)
        self._initial_usd_hedge_position: float = 0.0
        # Average entry price for static hedge
        self._initial_usd_hedge_avg_entry: float = 0.0
        # Track if portfolio has unsaved changes
        self._dirty = False

    def is_dirty(self) -> bool:
        """Check if the portfolio has unsaved changes.

        Returns:
            bool: True if the portfolio has unsaved changes, False otherwise
        """
        return self._dirty

    @property
    def underlying(self) -> Optional[str]:
        return self._underlying

    @underlying.setter
    def underlying(self, value: Optional[str]) -> None:
        if self._underlying != value:
            self._underlying = value
            asyncio.create_task(self._mark_dirty())

    @property
    def futures_position(self) -> float:
        return self._futures_position

    @futures_position.setter
    def futures_position(self, value: float) -> None:
        if self._futures_position != value:
            old_value = self._futures_position
            self._futures_position = value
            asyncio.create_task(self._on_futures_position_changed(old_value, value))

    @property
    def futures_avg_entry(self) -> float:
        return self._futures_avg_entry

    @futures_avg_entry.setter
    def futures_avg_entry(self, value: float) -> None:
        if self._futures_avg_entry != value:
            self._futures_avg_entry = value
            asyncio.create_task(self._mark_dirty())

    @property
    def last_hedge_price(self) -> Optional[float]:
        return self._last_hedge_price

    @last_hedge_price.setter
    def last_hedge_price(self, value: Optional[float]) -> None:
        if self._last_hedge_price != value:
            self._last_hedge_price = value
            asyncio.create_task(self._mark_dirty())

    @property
    def realized_pnl(self) -> float:
        return self._realized_pnl

    @realized_pnl.setter
    def realized_pnl(self, value: float) -> None:
        if self._realized_pnl != value:
            old_value = self._realized_pnl
            self._realized_pnl = value
            asyncio.create_task(self._on_realized_pnl_changed(old_value, value))

    @property
    def initial_usd_hedged(self) -> bool:
        return self._initial_usd_hedged

    @initial_usd_hedged.setter
    def initial_usd_hedged(self, value: bool) -> None:
        if self._initial_usd_hedged != value:
            self._initial_usd_hedged = value
            asyncio.create_task(self._mark_dirty())

    @property
    def initial_usd_hedge_position(self) -> float:
        return self._initial_usd_hedge_position

    @initial_usd_hedge_position.setter
    def initial_usd_hedge_position(self, value: float) -> None:
        if self._initial_usd_hedge_position != value:
            self._initial_usd_hedge_position = value
            asyncio.create_task(self._mark_dirty())

    @property
    def initial_usd_hedge_avg_entry(self) -> float:
        return self._initial_usd_hedge_avg_entry

    @initial_usd_hedge_avg_entry.setter
    def initial_usd_hedge_avg_entry(self, value: float) -> None:
        if self._initial_usd_hedge_avg_entry != value:
            self._initial_usd_hedge_avg_entry = value
            asyncio.create_task(self._mark_dirty())

    async def _on_futures_position_changed(self, old_value: float, new_value: float) -> None:
        """Handle futures position changes."""
        await self._mark_dirty()
        await self.emit(PortfolioEvent(
            event_type=PortfolioEventType.FUTURES_POSITION_UPDATED,
            portfolio=self,
            data={
                'old_value': old_value,
                'new_value': new_value,
                'delta': new_value - old_value
            }
        ))

    async def _on_realized_pnl_changed(self, old_value: float, new_value: float) -> None:
        """Handle realized PnL changes."""
        await self._mark_dirty()
        await self.emit(PortfolioEvent(
            event_type=PortfolioEventType.REALIZED_PNL_UPDATED,
            portfolio=self,
            data={
                'old_value': old_value,
                'new_value': new_value,
                'delta': new_value - old_value
            }
        ))

    async def _mark_dirty(self):
        """Mark the portfolio as having unsaved changes and emit STATE_CHANGED event"""
        if not self._dirty:
            self._dirty = True
            # Ensure the event is fully processed before continuing
            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.STATE_CHANGED,
                portfolio=self,
                data={'dirty': True}
            ))
            logger.debug("Marked portfolio %s as dirty", self.id)

    def mark_clean(self):
        """Mark the portfolio as clean (no unsaved changes).

        This is called by the PortfolioManager after a successful save.
        """
        if self._dirty:
            self._dirty = False
            # We don't await here since this is called from a sync context in PortfolioManager
            asyncio.create_task(self.emit(PortfolioEvent(
                event_type=PortfolioEventType.STATE_CHANGED,
                portfolio=self,
                data={'dirty': False}
            )))

    def _serialize_option(self, option: Any) -> Dict[str, Any]:
        """Serialize a single option to a dictionary.

        Args:
            option: The option to serialize

        Returns:
            Dict containing the serialized option data

        Raises:
            ValueError: If the option is missing required attributes or has invalid expiry
        """
        # Ensure we have all required attributes
        if not hasattr(option, 'expiry') or not isinstance(option.expiry, datetime):
            raise ValueError("Option must have a valid datetime expiry")

        if not hasattr(option, 'instrument_name') or not option.instrument_name:
            raise ValueError("Option must have an instrument_name")

        try:
            # Ensure expiry is timezone-aware
            expiry = option.expiry
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)

            # Convert to ISO format string
            expiry_iso = expiry.isoformat()

            # Build the serialized option data with required fields
            serialized = {
                "instrument_name": option.instrument_name,
                "quantity": option.quantity,
                "strike": option.strike,
                "expiry": expiry_iso,
                "option_type": option.option_type.value,
                "underlying": option.underlying,
                "contract_type": option.contract_type.value,
            }

            # Add optional fields if they exist and are not None
            optional_fields = [
                'mark_price',  # Current mark price in USD
                'iv',          # Implied volatility
                'usd_value',   # Current USD value of the position
                'delta'        # Current delta of the position
            ]

            for field in optional_fields:
                if hasattr(option, field):
                    value = getattr(option, field)
                    if value is not None:
                        serialized[field] = value

            logger.debug(
                "Serialized option %s: %s",
                option.instrument_name,
                {k: v for k, v in serialized.items() if k != 'expiry'}
            )

            return serialized

        except Exception as e:
            logger.error(
                "Failed to serialize option %s: %s",
                getattr(option, 'instrument_name', 'unknown'),
                str(e)
            )
            raise

    async def add_option(
        self,
        option: VanillaOption,
        entry_price: Optional[float] = None,
        premium_usd: Optional[float] = None,
        trade_time: Optional[datetime] = None
    ) -> None:
        """Add or update an option in the portfolio.

        If the option exists, update quantity and average entry price (weighted by quantity).
        If not, add as new.


        Args:
            option: The option to add or update
            entry_price: Optional entry price for updating average entry (uses strike as fallback)
            premium_usd: Optional premium in USD for trade recording
            trade_time: Optional timestamp for the trade (defaults to current time)

        Emits:
            OPTION_ADDED: When a new option is added
            OPTION_UPDATED: When an existing option is updated
        """

        logger.debug(f"[add_option] Starting to add option: {option.instrument_name}")
        logger.debug(f"[add_option] Option details: {option}")
        logger.debug(f"[add_option] Entry price: {entry_price}, Premium USD: {premium_usd}")

        if trade_time is None:
            trade_time = datetime.now(timezone.utc)
            logger.debug(f"[add_option] Using current time for trade: {trade_time}")

        existing = self.options.get(option.instrument_name)
        logger.debug(f"[add_option] Existing option found: {existing is not None}")

        if existing:
            # Update existing option
            old_qty = existing.quantity
            new_qty = option.quantity
            total_qty = old_qty + new_qty

            if total_qty == 0:
                # Remove position if net zero
                await self.remove_option(option.instrument_name)
                return

            # Weighted average entry price
            old_entry = getattr(existing, 'mark_price')
            if old_entry is not None and entry_price is not None:
                avg_entry = (old_entry * abs(old_qty) + entry_price * abs(new_qty)) / abs(total_qty)
            else:
                avg_entry = None

            # Update existing option
            existing.quantity = total_qty
            if avg_entry is not None:
                existing.mark_price = avg_entry

            # Update initial USD value for existing option
            if premium_usd is not None and option.instrument_name in self.initial_option_usd_value:
                # For sold options (new_qty < 0), add negative premium to track hedge needed
                hedge_delta = premium_usd * new_qty
                self.initial_option_usd_value[option.instrument_name][0] += hedge_delta
                new_total = self.initial_option_usd_value[option.instrument_name][0]
                existing.usd_value = new_total / total_qty
                logger.debug(
                    f"[add_option] Updated initial_option_usd_value for {option.instrument_name} " +
                    f"by {hedge_delta}, new value: {new_total}"
                )

            # Emit update event with trade details
            trade_data = None
            if premium_usd is not None:
                trade_data = {
                    'timestamp': trade_time.isoformat(),
                    'type': 'option',
                    'instrument': option.instrument_name,
                    'qty': new_qty,
                    'premium_usd': premium_usd,
                    'side': 'buy' if new_qty > 0 else 'sell',
                    'position_after': total_qty
                }
                self.trades.append(trade_data)

            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.OPTION_UPDATED,
                portfolio=self,
                data={
                    'instrument_name': option.instrument_name,
                    'old_quantity': old_qty,
                    'new_quantity': total_qty,
                    'trade': trade_data,
                    'option': existing
                }
            ))
        else:
            # Add new option
            if entry_price is not None:
                option.mark_price = entry_price

            if premium_usd is not None:
                option.usd_value = premium_usd

            self.options[option.instrument_name] = option

            # Store initial USD value if provided
            if premium_usd is not None:
                # For sold options (qty < 0), store the negative premium to indicate hedge needed
                hedge_value = premium_usd * abs(option.quantity)
                if option.quantity < 0:
                    hedge_value = -hedge_value  # Negative value indicates sold option needs hedging
                self.initial_option_usd_value[option.instrument_name] = [hedge_value, 0]
                logger.debug(f"[add_option] Set initial_option_usd_value for {option.instrument_name} to {hedge_value}")

            # Emit add event for new option
            logger.debug(f"[add_option] Emitting OPTION_ADDED event for {option.instrument_name}")
            event = PortfolioEvent(
                event_type=PortfolioEventType.OPTION_ADDED,
                portfolio=self,
                data={
                    'instrument_name': option.instrument_name,
                    'option': option,
                    'premium_usd': premium_usd
                }
            )
            logger.debug(f"[add_option] Event details: {event}")
            await self.emit(event)
            logger.debug("[add_option] Successfully emitted OPTION_ADDED event")

            # Record trade if premium provided
            if premium_usd is not None:
                trade_data = {
                    'timestamp': trade_time.isoformat(),
                    'type': 'option',
                    'instrument': option.instrument_name,
                    'qty': option.quantity,
                    'premium_usd': premium_usd,
                    'side': 'buy' if option.quantity > 0 else 'sell',
                    'position_after': option.quantity
                }
                self.trades.append(trade_data)

        # Reset cached values and mark as dirty
        logger.debug("[add_option] Resetting cached values and marking as dirty")
        self._total_delta = None
        self.initial_usd_hedged = False
        logger.debug(f"[add_option] Before _mark_dirty, is_dirty: {self.is_dirty()}")
        await self._mark_dirty()
        logger.debug(f"[add_option] After _mark_dirty, is_dirty: {self.is_dirty()}")
        logger.debug(f"[add_option] Option {option.instrument_name} added/updated successfully")

    async def remove_option(self, instrument_name: str) -> Optional[VanillaOption]:
        """Remove an option from the portfolio.

        Args:
            instrument_name: The instrument name of the option to remove

        Returns:
            The removed option, or None if not found

        Emits:
            OPTION_REMOVED: When an option is removed
        """
        option = self.options.pop(instrument_name, None)
        if option is not None:
            # Emit remove event
            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.OPTION_REMOVED,
                portfolio=self,
                data={
                    'instrument_name': instrument_name,
                    'option': option
                }
            ))

            self._total_delta = None  # Reset cached delta
            await self._mark_dirty()

        return option

    def get_option(self, instrument_name: str) -> Optional[VanillaOption]:
        """Get option by instrument name"""
        return self.options.get(instrument_name)

    def list_options(self) -> List[VanillaOption]:
        """Get list of all options in portfolio"""
        return list(self.options.values())

    async def update_option_quantity(self, instrument_name: str, new_quantity: float):
        """Update option quantity"""
        if instrument_name in self.options:
            old_quantity = self.options[instrument_name].quantity
            self.options[instrument_name].quantity = new_quantity

            # Emit update event
            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.OPTION_UPDATED,
                portfolio=self,
                data={
                    'instrument_name': instrument_name,
                    'old_quantity': old_quantity,
                    'new_quantity': new_quantity
                }
            ))

            self._total_delta = None  # Reset cached delta
            await self._mark_dirty()

    async def update_futures_position(self, quantity: float, price: float):
        """
        Update the USD notional futures position and recalculate the average entry price.
        quantity: positive for buy (USD notional), negative for sell (USD notional).
                 Must not be zero - use a no-op instead.
        price: execution price (USD)
        All position and PNL values are in USD notional.
        Also appends a trade record to self.trades for charting.
        """
        # Skip zero-quantity updates as they don't affect the position
        if abs(quantity) < 1e-10:  # Using a small epsilon to account for floating point precision
            logger.debug(f"Skipping zero-quantity futures position update at price {price}")
            return

        from datetime import datetime

        # Initialize attributes if they don't exist
        if not hasattr(self, 'realized_pnl'):
            self._realized_pnl = 0.0
        if not hasattr(self, 'trades'):
            self.trades = []

        # Update last hedge price using property setter
        self.last_hedge_price = price

        # Get current position values
        old_pos = self._futures_position  # Use _futures_position directly to avoid triggering events
        new_pos = old_pos + quantity
        realized_pnl_before = self._realized_pnl

        # Determine if we're opening, adding to, reducing, or closing a position
        is_opening = (old_pos == 0 and quantity != 0)
        is_closing = (new_pos == 0 and old_pos != 0)
        is_flipping = (old_pos > 0 and new_pos < 0) or (old_pos < 0 and new_pos > 0)
        is_adding = (old_pos > 0 and quantity > 0) or (old_pos < 0 and quantity < 0)
        is_reducing = (old_pos > 0 > quantity and new_pos > 0) or (old_pos < 0 < quantity and new_pos < 0)

        # Log the trade
        side = 'buy' if quantity > 0 else 'sell'
        trade_time = datetime.now().isoformat()

        # Update position and calculate PNL
        if is_opening or is_adding:
            # For opening or adding to a position, update the average entry price
            if old_pos + quantity == 0:  # Edge case: exactly closing the position
                self.futures_avg_entry = 0.0
            else:
                total_cost = old_pos * self._futures_avg_entry + quantity * price
                self.futures_avg_entry = total_cost / (old_pos + quantity)
            # Update position using property setter to trigger events
            self.futures_position = new_pos

        elif is_reducing or is_closing:
            # For reducing or closing a position, calculate realized PNL in BTC terms
            # Convert USD notional to BTC using the entry price for the portion being closed
            btc_quantity = abs(quantity) / self._futures_avg_entry
            realized_pnl_btc = btc_quantity * (price - self._futures_avg_entry) * (-1 if quantity > 0 else 1)
            self.realized_pnl = self._realized_pnl + realized_pnl_btc  # Use property setter
            # Update position using property setter to trigger events
            self.futures_position = new_pos
            if is_closing:
                self.futures_avg_entry = 0.0

        elif is_flipping:
            # Calculate PNL for the closed portion in BTC terms
            closed_portion = -old_pos
            btc_closed_portion = (
                abs(closed_portion) / self._futures_avg_entry
            )
            realized_pnl_btc = (
                btc_closed_portion *
                (price - self._futures_avg_entry) *
                (-1 if closed_portion > 0 else 1)
            )
            self.realized_pnl = self._realized_pnl + realized_pnl_btc  # Use property setter
            # For the new position, the average entry is the current price
            self.futures_avg_entry = price if new_pos != 0 else 0.0
            # Update position using property setter to trigger events
            self.futures_position = new_pos

        # Calculate PNL for this trade
        realized_pnl_for_trade = self._realized_pnl - realized_pnl_before

        # Only record non-zero quantity trades
        if abs(quantity) > 1e-10:  # Using a small epsilon to account for floating point precision
            trade_data = {
                'timestamp': trade_time,
                'qty_usd': quantity,
                'price': price,
                'side': side,
                'realized_pnl_for_trade': realized_pnl_for_trade,
                'realized_pnl_after': self._realized_pnl,
                'position_after': self._futures_position
            }
            self.trades.append(trade_data)
            logger.debug(f"Recorded trade: {trade_data}")
        else:
            logger.debug(f"Skipping zero-quantity trade at price {price}")

        # Mark as dirty to ensure state is saved
        await self._mark_dirty()
