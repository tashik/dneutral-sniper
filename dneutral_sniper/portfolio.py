from typing import Dict, List, Optional
from datetime import datetime
import json
from dneutral_sniper.models import OptionType, VanillaOption

class Portfolio:
    def save_to_file(self, filename: str):
        """Save portfolio positions, last_hedge_price, realized_pnl, initial_option_usd_value, trades, initial_usd_hedged flag, and initial_usd_hedge_position to a JSON file"""
        data = {
            "futures_position": self.futures_position,
            "futures_avg_entry": self.futures_avg_entry,
            "last_hedge_price": getattr(self, 'last_hedge_price', None),
            "realized_pnl": getattr(self, 'realized_pnl', 0.0),
            "initial_option_usd_value": getattr(self, 'initial_option_usd_value', {}),
            "trades": getattr(self, 'trades', []),
            "initial_usd_hedged": getattr(self, 'initial_usd_hedged', False),
            "initial_usd_hedge_position": getattr(self, 'initial_usd_hedge_position', 0.0),
            "initial_usd_hedge_avg_entry": getattr(self, 'initial_usd_hedge_avg_entry', 0.0),
            "options": [
                {
                    "instrument_name": o.instrument_name,
                    "quantity": o.quantity,
                    "strike": o.strike,
                    "expiry": o.expiry.isoformat(),
                    "option_type": o.option_type.value,
                    "underlying": o.underlying,
                    "mark_price": o.mark_price,
                    "iv": o.iv
                }
                for o in self.options.values()
            ]
        }
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)



    @staticmethod
    def load_from_file(filename: str) -> 'Portfolio':
        """Load portfolio positions, last_hedge_price, realized_pnl, initial_option_usd_value, and trades from a JSON file"""
        portfolio = Portfolio()
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            portfolio.futures_position = data.get("futures_position", 0.0)
            portfolio.futures_avg_entry = data.get("futures_avg_entry", 0.0)
            portfolio.last_hedge_price = data.get("last_hedge_price", None)
            portfolio.realized_pnl = data.get("realized_pnl", 0.0)
            portfolio.initial_option_usd_value = data.get("initial_option_usd_value", {})
            portfolio.trades = data.get("trades", [])
            portfolio.initial_usd_hedged = data.get("initial_usd_hedged", False)
            portfolio.initial_usd_hedge_position = data.get("initial_usd_hedge_position", 0.0)
            portfolio.initial_usd_hedge_avg_entry = data.get("initial_usd_hedge_avg_entry", 0.0)
            if (portfolio.last_hedge_price is None):
                portfolio.last_hedge_price = portfolio.futures_avg_entry if portfolio.futures_avg_entry > 0 else None
            for o in data.get("options", []):
                option = VanillaOption(
                    instrument_name=o["instrument_name"],
                    option_type=OptionType(o["option_type"]),
                    strike=o["strike"],
                    expiry=datetime.fromisoformat(o["expiry"]),
                    quantity=o["quantity"],
                    underlying=o["underlying"],
                    mark_price=o.get("mark_price"),
                    iv=o.get("iv")
                )
                portfolio.add_option(option)
        except FileNotFoundError:
            pass  # Empty portfolio if file does not exist
        return portfolio

    def __init__(self):
        self.options: Dict[str, VanillaOption] = {}
        self._total_delta: Optional[float] = None
        self.futures_position: float = 0.0  # Net position in futures (USD notional, dynamic delta hedge only)
        self.futures_avg_entry: float = 0.0  # Average entry price for futures (dynamic hedge)
        self.last_hedge_price: Optional[float] = None
        self.realized_pnl: float = 0.0
        self.initial_option_usd_value: dict = {}  # instrument_name -> initial hedged USD value
        self.trades: list = []  # List of trade dicts for PNL charting
        self.initial_usd_hedged: bool = False  # Flag for initial USD notional hedge phase
        self.initial_usd_hedge_position: float = 0.0  # USD notional, static hedge
        self.initial_usd_hedge_avg_entry: float = 0.0  # Avg entry for static hedge

    def add_option(self, option: VanillaOption, entry_price: float = None, premium_usd: float = None):
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
            old_qty = existing.quantity
            new_qty = option.quantity
            total_qty = old_qty + new_qty
            if total_qty == 0:
                # Remove position if net zero
                del self.options[option.instrument_name]
                self._total_delta = None
                if option.instrument_name in self.initial_option_usd_value:
                    del self.initial_option_usd_value[option.instrument_name]
                return
            # Weighted average entry price
            old_entry = getattr(existing, 'avg_entry', existing.strike)
            new_entry = entry_price if entry_price is not None else option.strike
            avg_entry = (old_entry * abs(old_qty) + new_entry * abs(new_qty)) / abs(total_qty)
            # Update existing option
            existing.quantity = total_qty
            existing.avg_entry = avg_entry
            # Only set initial USD value if not already set
            if premium_usd is not None and option.instrument_name not in self.initial_option_usd_value:
                self.initial_option_usd_value[option.instrument_name] = premium_usd
            # Option trade record for increase
            if premium_usd is not None:
                self.trades.append({
                    'timestamp': trade_time,
                    'type': 'option',
                    'instrument': option.instrument_name,
                    'qty': new_qty,
                    'premium_usd': premium_usd,
                    'side': 'buy' if new_qty > 0 else 'sell',
                    'position_after': total_qty
                })
        else:
            # Add new option
            option.avg_entry = entry_price if entry_price is not None else option.strike
            self.options[option.instrument_name] = option
            if premium_usd is not None:
                self.initial_option_usd_value[option.instrument_name] = premium_usd
            # Option trade record for new
            if premium_usd is not None:
                self.trades.append({
                    'timestamp': trade_time,
                    'type': 'option',
                    'instrument': option.instrument_name,
                    'qty': option.quantity,
                    'premium_usd': premium_usd,
                    'side': 'buy' if option.quantity > 0 else 'sell',
                    'position_after': option.quantity
                })
        self._total_delta = None  # Reset cached delta

    def remove_option(self, instrument_name: str):
        """Remove option from portfolio"""
        if instrument_name in self.options:
            del self.options[instrument_name]
            self._total_delta = None  # Reset cached delta

    def get_option(self, instrument_name: str) -> Optional[VanillaOption]:
        """Get option by instrument name"""
        return self.options.get(instrument_name)

    def list_options(self) -> List[VanillaOption]:
        """Get list of all options in portfolio"""
        return list(self.options.values())

    def update_option_quantity(self, instrument_name: str, new_quantity: float):
        """Update option quantity"""
        if instrument_name in self.options:
            self.options[instrument_name].quantity = new_quantity
            self._total_delta = None  # Reset cached delta

    def update_futures_position(self, quantity: float, price: float):
        """
        Update the USD notional futures position and recalculate the average entry price.
        quantity: positive for buy (USD notional), negative for sell (USD notional)
        price: execution price (USD)
        All position and PNL values are in USD notional.
        Also appends a trade record to self.trades for charting.
        """
        from datetime import datetime
        self.last_hedge_price = price
        if not hasattr(self, 'realized_pnl'):
            self.realized_pnl = 0.0
        if not hasattr(self, 'trades'):
            self.trades = []
        side = 'buy' if quantity > 0 else 'sell'
        realized_pnl_before = self.realized_pnl
        if self.futures_position == 0:
            # Opening new position
            self.futures_position = quantity
            self.futures_avg_entry = price
        elif (self.futures_position > 0 and quantity > 0) or (self.futures_position < 0 and quantity < 0):
            # Increasing position in same direction, recalc avg price
            total_qty = self.futures_position + quantity
            avg_entry = (
                self.futures_avg_entry * abs(self.futures_position) + price * abs(quantity)
            ) / abs(total_qty)
            self.futures_position = total_qty
            self.futures_avg_entry = avg_entry
        else:
            # Reducing or flipping position
            new_position = self.futures_position + quantity
            closed_qty = min(abs(quantity), abs(self.futures_position))
            # Use avg_entry BEFORE the trade for realized PnL
            avg_entry_before = self.futures_avg_entry
            if closed_qty > 0:
                direction = 1 if self.futures_position > 0 else -1
                closed_btc = closed_qty / abs(avg_entry_before) if avg_entry_before != 0 else 0
                self.realized_pnl += (price - avg_entry_before) * closed_btc * direction
            if self.futures_position * new_position >= 0:
                # Reduced but not flipped
                self.futures_position = new_position
                # avg_entry stays the same
                if self.futures_position == 0:
                    self.futures_avg_entry = 0.0
            else:
                # Flipped direction: realized PnL, new avg entry
                self.futures_position = new_position
                self.futures_avg_entry = price if self.futures_position != 0 else 0.0
        # Record trade
        realized_pnl_for_trade = self.realized_pnl - realized_pnl_before
        self.trades.append({
            'timestamp': datetime.now().isoformat(),
            'qty_usd': quantity,
            'price': price,
            'side': side,
            'realized_pnl_for_trade': realized_pnl_for_trade,
            'realized_pnl_after': self.realized_pnl,
            'position_after': self.futures_position
        })
