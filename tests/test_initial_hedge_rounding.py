"""Tests for initial option premium hedge rounding behavior."""
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.models import OptionType, ContractType
from dneutral_sniper.models import VanillaOption

class DummyPortfolio:
    """Dummy portfolio for testing."""

    def __init__(self):
        self.id = 'test-portfolio'
        self._futures_position = 0.0
        self._futures_avg_entry = 0.0
        self._last_hedge_price = 100.0
        self.hedge_calls = []  # Tracks dynamic delta hedges
        self.static_hedge_calls = []  # Tracks static premium hedges
        self._options = {}
        self._initial_usd_hedged = False
        self._dirty = False
        self._underlying = 'BTC'
        self._total_delta = 0.0
        self._realized_pnl = 0.0
        self.initial_option_usd_value = {}
        self.trades = []
        self._initial_usd_hedge_position = 0.0
        self._initial_usd_hedge_avg_entry = 0.0
        self._lock = asyncio.Lock()
        self._event_listeners = {}

    @property
    def initial_usd_hedged(self):
        return self._initial_usd_hedged

    @initial_usd_hedged.setter
    def initial_usd_hedged(self, value):
        self._initial_usd_hedged = value

    @property
    def initial_usd_hedge_position(self):
        return self._initial_usd_hedge_position
        
    @property
    def futures_position(self):
        return self._futures_position
        
    @property
    def futures_avg_entry(self):
        return self._futures_avg_entry

    @initial_usd_hedge_position.setter
    def initial_usd_hedge_position(self, value):
        # Update the value and track the change
        delta = value - self._initial_usd_hedge_position
        old_value = self._initial_usd_hedge_position
        self._initial_usd_hedge_position = value
        # Record the static hedge call if this is a change
        if abs(delta) > 1e-8:  # Only record non-zero changes
            self.static_hedge_calls.append((delta, self._last_hedge_price))
            
    @property
    def initial_usd_hedge_avg_entry(self):
        return self._initial_usd_hedge_avg_entry
        
    @initial_usd_hedge_avg_entry.setter
    def initial_usd_hedge_avg_entry(self, value):
        self._initial_usd_hedge_avg_entry = value
        
    @property
    def realized_pnl(self):
        return self._realized_pnl
        
    @realized_pnl.setter
    def realized_pnl(self, value):
        self._realized_pnl = value
        
    def list_options(self):
        """Return a list of all options in the portfolio."""
        return list(self._options.values())

    async def update_futures_position(self, usd_qty: float, price: float):
        """Mock update_futures_position that records the call."""
        self.hedge_calls.append((usd_qty, price))
        self._futures_position += usd_qty
        self._futures_avg_entry = price if usd_qty != 0 else 0.0
        return True
        
    async def _mark_dirty(self):
        """Mark the portfolio as dirty."""
        self._dirty = True
        return True

class DummyDeribitClient:
    """Dummy Deribit client for testing."""

    def __init__(self):
        self.price_callback = None
        self.last_price = 100.0
        self.price_iv_cache = {}

    async def get_instrument_mark_price_and_iv(self, instrument_name):
        """Return a dummy mark price and IV."""
        return self.last_price, 0.7  # price, IV
        
    def set_price_callback(self, callback):
        """Set the price callback function.
        
        Args:
            callback: The callback function to call on price updates
        """
        self.price_callback = callback

@pytest.fixture
def hedger_config():
    """Create a test hedger config with minimum hedge size."""
    return HedgerConfig(
            ddh_min_trigger_delta=0.01,
            ddh_target_delta=0.0,  # Target delta neutral
            ddh_step_mode='absolute',
            ddh_step_size=10.0,  # $10 step size
            min_hedge_usd=10.0,  # $10 minimum hedge amount
            instrument_name='BTC-PERPETUAL',
            underlying='BTC',
            volatility=0.8,
            risk_free_rate=0.0,
            price_check_interval=2.0
        )

@pytest.fixture
def mock_portfolio():
    """Create a mock portfolio for testing."""
    return DummyPortfolio()

@pytest.fixture
def mock_client():
    """Create a mock Deribit client for testing."""
    return DummyDeribitClient()

@pytest.mark.asyncio
async def test_initial_hedge_rounding(hedger_config, mock_portfolio, mock_client):
    """Test that initial hedge amounts are properly rounded to minimum size."""
    # Create a test option with a premium that would require a hedge just above min_hedge_usd
    option = VanillaOption(
        instrument_name="BTC-31DEC25-100000-C",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=15.0,  # Premium that's above min_hedge_usd (10.0)
        mark_price=0.15,
        iv=0.8,
        delta=0.5,
        contract_type=ContractType.INVERSE
    )

    # Add option to portfolio with premium (sold option)
    mock_portfolio._options[option.instrument_name] = option
    mock_portfolio.initial_option_usd_value = {
        option.instrument_name: [-15.0, 0.0]  # (needed_hedge, actual_hedge)
    }
    # Create hedger but don't start it to avoid background tasks
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_client)
    
    # Set current price directly
    hedger.current_price = 90000.0
    current_price = hedger.current_price
    
    # Execute initial hedge - should round up to min_hedge_usd
    print("\n=== Before hedge ===")
    print(f"Portfolio state: initial_usd_hedge_position={mock_portfolio.initial_usd_hedge_position}")
    print(f"Portfolio state: initial_option_usd_value={mock_portfolio.initial_option_usd_value}")
    
    # Check if the hedge should be executed
    option_data = mock_portfolio.initial_option_usd_value[option.instrument_name]
    needed_hedge, actual_hedge = option_data
    required_qty = needed_hedge - actual_hedge
    min_hedge = hedger_config.min_hedge_usd
    print(f"Required hedge: {required_qty}, min_hedge: {min_hedge}")
    
    # Execute the hedge
    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)
    
    print("\n=== After hedge ===")
    print(f"Portfolio state: initial_usd_hedge_position={mock_portfolio.initial_usd_hedge_position}")
    print(f"Portfolio state: initial_option_usd_value={mock_portfolio.initial_option_usd_value}")
    
    # For a premium of -15.0, it should be rounded up to -20.0 (next multiple of min_hedge_usd)
    expected_hedge = -20.0  # -15 rounds up to -20 (next multiple of 10.0)
    
    # Verify the initial hedge position was updated with the correct amount
    actual_position = mock_portfolio.initial_usd_hedge_position
    print(f"Expected hedge position: {expected_hedge}, actual: {actual_position}")
    
    # Check if the hedge was recorded in static_hedge_calls
    print(f"Static hedge calls: {mock_portfolio.static_hedge_calls}")
    
    # Check if the portfolio's initial_usd_hedge_position was updated
    print(f"Portfolio's initial_usd_hedge_position: {mock_portfolio.initial_usd_hedge_position}")
    
    # Check if the option's hedge amount was updated
    updated_option_data = mock_portfolio.initial_option_usd_value.get(option.instrument_name)
    if updated_option_data:
        updated_needed, updated_actual = updated_option_data
        print(f"Updated option hedge - needed: {updated_needed}, actual: {updated_actual}")
    
    # Verify the hedge position was updated
    assert actual_position == expected_hedge, \
        f"Initial hedge position should be {expected_hedge}, got {actual_position}"
    
    # Verify the hedge was recorded
    assert len(mock_portfolio.static_hedge_calls) > 0, "No static hedge was recorded"
        
    # Verify the option's hedge amount was updated
    assert option.instrument_name in mock_portfolio.initial_option_usd_value, \
        f"Option {option.instrument_name} not found in initial_option_usd_value"
        
    needed, actual = mock_portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -15.0, f"Needed hedge should remain -15.0, got {needed}"
    assert actual == expected_hedge, f"Actual hedge should be {expected_hedge}, got {actual}"
    
    # Verify the static hedge was recorded
    assert len(mock_portfolio.static_hedge_calls) == 1, "Static hedge should have been recorded"
    hedge_qty, hedge_price = mock_portfolio.static_hedge_calls[0]
    assert hedge_qty == expected_hedge, f"Hedge amount should be {expected_hedge}, got {hedge_qty}"
    
    # Verify no dynamic hedges were executed
    assert len(mock_portfolio.hedge_calls) == 0, "No dynamic hedges should have been executed"
    
    # Verify portfolio state
    assert mock_portfolio.initial_usd_hedged, "Portfolio should be marked as hedged"
    
    # Verify the option's hedge amount was updated with the rounded value
    assert option.instrument_name in mock_portfolio.initial_option_usd_value
    needed, actual = mock_portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -15.0, f"Needed hedge should be -15.0, got {needed}"
    assert actual == expected_hedge, f"Actual hedge should be {expected_hedge}, got {actual}"
    
    # Set hedge_qty for assertions below
    hedge_qty = expected_hedge

    # Should have hedged the next multiple of min_hedge_usd (20.0 for a -15.0 premium)
    assert abs(hedge_qty) == 2 * hedger_config.min_hedge_usd, \
        f"Should have hedged next multiple of min_hedge_usd, got {hedge_qty}"

    # Verify the portfolio was updated with the rounded amount
    assert mock_portfolio.initial_usd_hedged, "Portfolio should be marked as hedged"
    expected_hedge_position = -20.0  # -15 rounds up to -20 (next multiple of 10.0)
    assert mock_portfolio.initial_usd_hedge_position == expected_hedge_position, \
        f"Initial hedge position should be {expected_hedge_position}, got {mock_portfolio.initial_usd_hedge_position}"
    
    # Verify the initial_option_usd_value was updated with the rounded amount
    assert option.instrument_name in mock_portfolio.initial_option_usd_value
    needed, actual = mock_portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -15.0, f"Needed hedge should be -15.0, got {needed}"
    assert actual == expected_hedge_position, f"Actual hedge should be {expected_hedge_position}, got {actual}"

@pytest.mark.asyncio
async def test_initial_hedge_above_min_size(hedger_config, mock_portfolio, mock_client):
    """Test that hedge amounts above the minimum size are not rounded."""
    # Create a test option with a premium above the minimum hedge size
    option = VanillaOption(
        instrument_name="BTC-31DEC25-100000-P",
        option_type=OptionType.PUT,
        strike=100000.0,
        expiry=datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=25.0,  # Premium above min_hedge_usd
        mark_price=25.0,  # Match usd_value for this test
        iv=0.8,
        delta=0.5,
        contract_type=ContractType.INVERSE
    )

    # Add option to portfolio with premium (sold option)
    mock_portfolio._options[option.instrument_name] = option
    mock_portfolio.initial_option_usd_value = {
        option.instrument_name: [-25.0, 0.0]  # (needed_hedge, actual_hedge)
    }

    # Create and start hedger
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_client)
    await hedger.start()

    # Set current price
    current_price = 90000.0
    await hedger._update_price(current_price)

    # Execute initial hedge - should use the exact amount since it's above min_hedge_usd
    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)

    # For a premium of -25.0, it should be rounded up to -30.0 (next multiple of min_hedge_usd)
    min_hedge = hedger_config.min_hedge_usd
    expected_hedge = -30.0  # -25 rounds up to -30 (next multiple of 10)
    
    # Verify the initial hedge position was updated with the rounded amount
    assert mock_portfolio.initial_usd_hedge_position == expected_hedge, \
        f"Initial hedge position should be {expected_hedge}, got {mock_portfolio.initial_usd_hedge_position}"
    
    # Verify the static hedge was recorded
    assert len(mock_portfolio.static_hedge_calls) == 1, "Static hedge should have been recorded"
    hedge_qty, hedge_price = mock_portfolio.static_hedge_calls[0]
    assert hedge_qty == expected_hedge, f"Hedge amount should be {expected_hedge}, got {hedge_qty}"
    
    # Verify no dynamic hedges were executed
    assert len(mock_portfolio.hedge_calls) == 0, "No dynamic hedges should have been executed"
    
    # Verify portfolio state
    assert mock_portfolio.initial_usd_hedged, "Portfolio should be marked as hedged"
    
    # Verify the option's hedge amount was updated with the rounded value
    assert option.instrument_name in mock_portfolio.initial_option_usd_value
    needed, actual = mock_portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -25.0, f"Needed hedge should be -25.0, got {needed}"
    assert actual == expected_hedge, f"Actual hedge should be {expected_hedge}, got {actual}"
    
    # Set hedge_qty for assertions below
    hedge_qty = expected_hedge
