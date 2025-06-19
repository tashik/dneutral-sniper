from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
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
                    logger.error(f"Error in event listener {i} for {event.event_type}: {result}", 
                               exc_info=result)
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

    def save_to_file(self, filename: str) -> None:
        """Save portfolio to a JSON file including all positions and state.

        Args:
            filename: Path to save the portfolio JSON file

        Raises:
            IOError: If there's an error writing to the file
        """
        try:
            data = self.to_dict()

            # Ensure directory exists
            file_path = Path(filename)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to a temporary file first
            temp_path = file_path.with_suffix('.tmp')
            try:
                with temp_path.open('w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())

                # Atomic rename on POSIX systems
                temp_path.replace(file_path)

                # Mark as clean after successful save
                self.mark_clean()
                logger.debug(
                    "Successfully saved portfolio %s to %s",
                    self.id,
                    filename
                )

            except (IOError, OSError) as e:
                # Clean up temp file if it exists
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError as cleanup_error:
                        logger.warning(
                            "Failed to clean up temp file %s: %s",
                            temp_path,
                            cleanup_error
                        )
                raise IOError(
                    f"Failed to save portfolio {self.id} to {filename}: {e}"
                ) from e

        except Exception as e:
            logger.error(
                "Unexpected error saving portfolio %s to %s: %s",
                self.id,
                filename,
                e,
                exc_info=True
            )
            raise

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
            ValueError: If data is invalid
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

            # Convert string timestamp to datetime if needed
            expiry = option_data['expiry']
            if isinstance(expiry, (int, float)):
                expiry = datetime.fromtimestamp(expiry)
            elif isinstance(expiry, str):
                expiry = datetime.fromisoformat(expiry)

            # Create and return the option
            return VanillaOption(
                instrument_name=option_data['instrument_name'],
                quantity=float(option_data['quantity']),
                strike=float(option_data['strike']),
                expiry=expiry,
                option_type=OptionType(option_data['option_type']),
                underlying=option_data['underlying'],
                contract_type=ContractType(option_data['contract_type']),
                mark_price=float(option_data.get('mark_price', 0)),
                iv=float(option_data.get('iv', 0))
                if option_data.get('iv') is not None else None,
                usd_value=float(option_data['usd_value'])
                if 'usd_value' in option_data else None,
                delta=float(option_data['delta'])
                if option_data.get('delta') is not None else None
            )

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
        # USD notional, static hedge
        self._initial_usd_hedge_position: float = 0.0
        # Avg entry for static hedge
        self._initial_usd_hedge_avg_entry: float = 0.0
        # Track if portfolio has unsaved changes
        self._dirty = False
        
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
            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.STATE_CHANGED,
                portfolio=self,
                data={'dirty': True}
            ))

    def _serialize_option(self, option: Any) -> Dict[str, Any]:
        """Serialize a single option to a dictionary.

        Args:
            option: The option to serialize

        Returns:
            Dict containing the serialized option data

        Raises:
            ValueError: If the option is missing required attributes
        """
        try:
            return {
                "instrument_name": getattr(option, 'instrument_name', ''),
                "quantity": getattr(option, 'quantity', 0.0),
                "strike": getattr(option, 'strike', 0.0),
                "expiry": (
                    option.expiry.isoformat()
                    if hasattr(option, 'expiry') and option.expiry
                    else None
                ),
                "option_type": (
                    option.option_type.value
                    if hasattr(option, 'option_type') and option.option_type
                    else None
                ),
                "underlying": getattr(option, 'underlying', None),
                "contract_type": (
                    option.contract_type.value
                    if hasattr(option, 'contract_type') and option.contract_type
                    else None
                ),
                "mark_price": getattr(option, 'mark_price', None),
                "iv": getattr(option, 'iv', None),
                "usd_value": getattr(option, 'usd_value', None),
                "delta": getattr(option, 'delta', None)
            }
        except AttributeError as e:
            logger.error(
                "Failed to serialize option %s: missing required attribute: %s",
                getattr(option, 'instrument_name', 'unknown'),
                str(e)
            )
            raise ValueError(
                f"Invalid option object: missing required attribute: {e}"
            ) from e

    def is_dirty(self) -> bool:
        """Check if portfolio has unsaved changes"""
        return self._dirty

    def mark_clean(self):
        """Mark the portfolio as clean (saved)"""
        self._dirty = False

    async def add_option(
        self,
        option: VanillaOption,
        entry_price: float = None,
        premium_usd: float = None
    ) -> None:
        """
        Add or update an option in the portfolio.
        If the option exists, update quantity and average entry price (weighted by quantity).
        If not, add as new.
        Optionally takes entry_price for updating avg entry (otherwise uses strike as proxy).
        Optionally records the option trade (premium_usd) in the trades journal.
        Also stores premium_usd as initial_option_usd_value for reporting.
        """
        from datetime import datetime
        existing = self.options.get(option.instrument_name)
        trade_time = datetime.now().isoformat()

        if existing:
            # Update existing option position
            existing_option = self.options[option.instrument_name]
            old_qty = existing_option.quantity
            new_qty = old_qty + option.quantity
            
            # If the quantity hasn't changed, no need to update
            if option.quantity == 0:
                return
                
            # Update the quantity
            existing_option.quantity = new_qty

            # Update average entry price if needed
            if entry_price is not None and option.quantity != 0:
                total_qty = abs(old_qty) + abs(option.quantity)
                if total_qty > 0:
                    existing_option.avg_entry = (
                        abs(old_qty) * existing_option.avg_entry +
                        abs(option.quantity) * entry_price
                    ) / total_qty

            # Record the trade if premium_usd is provided
            trade_data = None
            if premium_usd is not None:
                trade_time = datetime.now(timezone.utc).isoformat()
                trade_data = {
                    'timestamp': trade_time,
                    'type': 'option',
                    'instrument': option.instrument_name,
                    'qty': option.quantity,
                    'premium_usd': premium_usd,
                    'side': 'buy' if option.quantity > 0 else 'sell',
                    'position_after': new_qty
                }
                self.trades.append(trade_data)

            # Emit update event after modifying the option
            await self.emit(PortfolioEvent(
                event_type=PortfolioEventType.OPTION_UPDATED,
                portfolio=self,
                data={
                    'instrument_name': option.instrument_name,
                    'old_quantity': old_qty,
                    'new_quantity': new_qty,
                    'trade': trade_data,
                    'option': existing_option  # Include the full option in the event data
                }
            ))
            return  # Skip the rest of the function since we've handled the update

        # If we get here, we're adding a new option
        self.options[option.instrument_name] = option
        if entry_price is not None:
            option.avg_entry = entry_price
        # Store initial USD value for hedging
        if premium_usd is not None:
            self.initial_option_usd_value[option.instrument_name] = [premium_usd, 0]

        # Emit add event for new option
        await self.emit(PortfolioEvent(
            event_type=PortfolioEventType.OPTION_ADDED,
            portfolio=self,
            data={
                'instrument_name': option.instrument_name,
                'option': option,
                'premium_usd': premium_usd
            }
        ))

        # Option trade record for new option
        if premium_usd is not None:
            trade_data = {
                'timestamp': trade_time,
                'type': 'option',
                'instrument': option.instrument_name,
                'qty': option.quantity,
                'premium_usd': premium_usd,
                'side': 'buy' if option.quantity > 0 else 'sell',
                'position_after': option.quantity
            }
            self.trades.append(trade_data)

        self._total_delta = None  # Reset cached delta
        self.initial_usd_hedged = False
        await self._mark_dirty()

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
        quantity: positive for buy (USD notional), negative for sell (USD notional)
        price: execution price (USD)
        All position and PNL values are in USD notional.
        Also appends a trade record to self.trades for charting.
        """
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
            btc_closed_portion = abs(closed_portion) / self._futures_avg_entry
            realized_pnl_btc = btc_closed_portion * (price - self._futures_avg_entry) * (-1 if closed_portion > 0 else 1)
            self.realized_pnl = self._realized_pnl + realized_pnl_btc  # Use property setter
            # For the new position, the average entry is the current price
            self.futures_avg_entry = price if new_pos != 0 else 0.0
            # Update position using property setter to trigger events
            self.futures_position = new_pos

        # Calculate PNL for this trade
        realized_pnl_for_trade = self._realized_pnl - realized_pnl_before
        
        # Record trade
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
        
        # Mark as dirty to ensure state is saved
        await self._mark_dirty()
