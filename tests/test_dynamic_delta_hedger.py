import asyncio
import logging
import pytest
from unittest.mock import AsyncMock, MagicMock
from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.deribit_client import DeribitWebsocketClient
from dneutral_sniper.options import OptionModel
from dneutral_sniper.models import OptionType, VanillaOption, ContractType
import numpy as np
from datetime import datetime, timedelta, timezone

class DummyDeribitClient:
    def __init__(self):
        self.price_callback = None
        self.last_price = None
        # Simulate the price_iv_cache structure
        self.price_iv_cache = {}
    def set_price_callback(self, cb):
        self.price_callback = cb
    def get_price_iv(self, instrument_name):
        # Simulate cache lookup
        entry = self.price_iv_cache.get(instrument_name)
        if entry:
            return entry.get("mark_price"), entry.get("iv")
        return None, None
    async def get_instrument_mark_price_and_iv(self, instrument_name):
        # Simulate async fallback fetch
        return 100.0, 0.4

class DummyPortfolio(Portfolio):
    def __init__(self):
        # Initialize all required attributes to avoid property setters
        self.id = 'test-portfolio'  # Add id attribute
        self._futures_position = 0.0
        self._futures_avg_entry = 0.0  # Initialize average entry price
        self._last_hedge_price = 100.0
        self.hedge_calls = []
        self._options = {}
        self._initial_usd_hedged = True
        self._dirty = False
        self._underlying = 'BTC'  # Default underlying asset
        self._total_delta = 0.0
        self._realized_pnl = 0.0
        self.initial_option_usd_value = {}
        self.trades = []
        self._initial_usd_hedge_position = 0.0
        self._initial_usd_hedge_avg_entry = 0.0

        # Add lock for event emission
        self._lock = asyncio.Lock()

        # Mock event listeners
        self._event_listeners = {}

    async def _mark_dirty(self):
        """Mock _mark_dirty to avoid actual event emission in tests"""
        self._dirty = True
        return

    async def emit(self, event):
        """Mock emit to avoid actual event emission in tests"""
        async with self._lock:
            pass

    # save_to_file is no longer called directly by DynamicDeltaHedger
    # The portfolio manager handles saving via the event system

    @property
    def initial_usd_hedged(self):
        return self._initial_usd_hedged

    @initial_usd_hedged.setter
    def initial_usd_hedged(self, value):
        self._initial_usd_hedged = value

    @property
    def futures_position(self):
        return self._futures_position

    @futures_position.setter
    def futures_position(self, value):
        self._futures_position = value

    async def update_futures_position(self, usd_qty: float, price: float) -> None:
        self.hedge_calls.append((usd_qty, price))
        self._futures_position += usd_qty
        self.last_hedge_price = price

    def list_options(self):
        return list(self._options.values())

class DummyPortfolio(Portfolio):
    def __init__(self):
        # Initialize all required attributes to avoid property setters
        self.id = 'test-portfolio'  # Add id attribute
        self._futures_position = 0.0
        self._futures_avg_entry = 0.0  # Initialize average entry price
        self._last_hedge_price = 100.0
        self.hedge_calls = []
        self._options = {}
        self._initial_usd_hedged = False  # Start with False to test initial hedge
        self._dirty = False
        self._underlying = 'BTC'  # Default underlying asset
        self._total_delta = 0.0
        self._realized_pnl = 0.0
        self.initial_option_usd_value = {}
        self.trades = []
        self._initial_usd_hedge_position = 0.0
        self._initial_usd_hedge_avg_entry = 0.0

        # Add lock for event emission
        self._lock = asyncio.Lock()

        # Mock event listeners
        self._event_listeners = {}

        # Configure logging
        self.logger = logging.getLogger('DummyPortfolio')
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    async def _mark_dirty(self):
        """Mock _mark_dirty to avoid actual event emission in tests"""
        self._dirty = True
        return True

    async def emit(self, event):
        """Mock emit to avoid actual event emission in tests"""
        async with self._lock:
            self.logger.debug(f"Emitted event: {event.event_type}")

    @property
    def initial_usd_hedged(self):
        return self._initial_usd_hedged

    @initial_usd_hedged.setter
    def initial_usd_hedged(self, value):
        self.logger.debug(f"Setting initial_usd_hedged to {value}")
        self._initial_usd_hedged = value

    @property
    def futures_position(self):
        return self._futures_position

    @futures_position.setter
    def futures_position(self, value):
        self.logger.debug(f"Setting futures_position to {value}")
        self._futures_position = value

    @property
    def initial_usd_hedge_position(self):
        return self._initial_usd_hedge_position

    @initial_usd_hedge_position.setter
    def initial_usd_hedge_position(self, value):
        self.logger.debug(f"Setting initial_usd_hedge_position to {value}")
        self._initial_usd_hedge_position = value

    async def update_futures_position(self, usd_qty: float, price: float) -> None:
        self.logger.debug(f"Updating futures position: ${usd_qty:+,.2f} at ${price:,.2f}")
        self.hedge_calls.append((usd_qty, price))
        self._futures_position += usd_qty
        self.last_hedge_price = price
        self.logger.debug(f"New futures position: ${self._futures_position:,.2f}")

    def list_options(self):
        return list(self._options.values())

    def get_option(self, instrument_name):
        return self._options.get(instrument_name)

@pytest.mark.asyncio
async def test_negative_premium_hedge(hedger_config, mock_client):
    """Test that selling an option with premium creates a negative hedge position.

    When an option is sold (negative quantity) with a premium, we should record a negative
    initial_option_usd_value and hedge by selling futures (negative hedge).
    """
    # Create a fresh portfolio for this test
    portfolio = DummyPortfolio()

    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('test_negative_premium_hedge')

    # Create a sold option (negative quantity) with premium
    option = VanillaOption(
        instrument_name="BTC-31DEC25-100000-C",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=2000.0,  # Premium received for selling the option
        mark_price=0.05,
        iv=0.8,  # Implied volatility
        delta=0.5,
        contract_type=ContractType.INVERSE  # Use INVERSE for this test
    )

    # Add option to portfolio with premium (sold option)
    logger.debug("Adding option to portfolio")
    portfolio._options[option.instrument_name] = option

    # For inverse options, we should set up the initial_option_usd_value
    # For a sold option, we store negative premium to indicate hedge needed
    portfolio.initial_option_usd_value = {
        option.instrument_name: [-2000.0, 0.0]  # (needed_hedge, actual_hedge)
    }
    logger.debug(f"Set initial_option_usd_value: {portfolio.initial_option_usd_value}")

    # Create and start hedger
    logger.debug("Creating and starting hedger")
    hedger = DynamicDeltaHedger(hedger_config, portfolio, mock_client)
    await hedger.start()

    # Set current price
    current_price = 90000.0
    logger.debug(f"Setting current price to {current_price}")
    await hedger._update_price(current_price)

    # Process initial hedge - call the method directly to test it in isolation
    logger.debug("Executing initial option premium hedge if needed")

    # First, verify the initial state
    assert len(portfolio.hedge_calls) == 0, "No hedge calls should be made yet"
    assert option.instrument_name in portfolio.initial_option_usd_value, "Option should be in initial_option_usd_value"
    assert portfolio.initial_option_usd_value[option.instrument_name] == (-2000.0, 0.0), \
        f"Initial hedge state should be (-2000.0, 0.0), got {portfolio.initial_option_usd_value[option.instrument_name]}"

    # Ensure the portfolio is marked as not hedged yet
    portfolio.initial_usd_hedged = False
    portfolio.initial_usd_hedge_position = 0.0  # Reset hedge position
    logger.debug(f"Portfolio initial_usd_hedged set to {portfolio.initial_usd_hedged}")
    logger.debug(f"Portfolio initial_usd_hedge_position reset to {portfolio.initial_usd_hedge_position}")

    # Call the method we want to test
    logger.debug("Calling _execute_initial_option_premium_hedge_if_needed")
    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)

    # Verify the hedge was executed by checking the portfolio's futures position
    logger.debug(f"Portfolio futures position after hedge: {portfolio.futures_position}")
    logger.debug(f"Portfolio initial_usd_hedge_position: {portfolio.initial_usd_hedge_position}")
    logger.debug(f"Portfolio initial_option_usd_value: {portfolio.initial_option_usd_value}")
    logger.debug(f"Portfolio hedge calls: {portfolio.hedge_calls}")

    # Verify the hedge was executed
    assert len(portfolio.hedge_calls) > 0, "Hedge should have been executed"
    assert portfolio.initial_usd_hedged, "Portfolio should be marked as hedged"

    # Debug: Print hedge calls and portfolio state
    logger.debug(f"Hedge calls: {portfolio.hedge_calls}")
    logger.debug(f"Portfolio state: {portfolio.__dict__}")
    logger.debug(f"Initial option USD value: {portfolio.initial_option_usd_value}")

    # Verify that a hedge was executed for the inverse option
    assert len(portfolio.hedge_calls) > 0, "Hedge should have been executed for inverse option"
    usd_qty, price = portfolio.hedge_calls[0]

    # For a sold inverse option, we should be selling futures (negative usd_qty)
    # to hedge the premium received
    assert usd_qty < 0, "Should sell futures (negative usd_qty) to hedge sold inverse option premium"
    assert price == current_price, "Hedge should be executed at current price"

    # Verify the hedge amount is approximately equal to the premium (with rounding)
    expected_hedge = -2000.0
    assert abs(usd_qty - expected_hedge) <= hedger_config.min_hedge_usd, \
        f"Hedge amount {usd_qty} should be close to expected {expected_hedge}"

    # Check that the hedge was recorded in the portfolio
    assert option.instrument_name in portfolio.initial_option_usd_value, \
        "Option should still be in initial_option_usd_value"

    # The actual hedge should be updated to match the executed hedge
    needed, actual = portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -2000.0, "Needed hedge should still be -2000.0"
    assert abs(actual - expected_hedge) <= 1e-8, f"Actual hedge {actual} should match executed hedge {expected_hedge}"

    # Now test with a standard contract type - should not use initial USD hedge
    portfolio.hedge_calls.clear()  # Clear previous hedge calls
    portfolio.initial_option_usd_value.clear()  # Clear previous hedge values

    # Create a standard contract option
    std_option = VanillaOption(
        instrument_name="BTC-31DEC24-100000-C-STD",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=2000.0,  # Premium received for selling the option
        mark_price=2000.0,
        iv=0.8,
        delta=0.3,
        contract_type=ContractType.STANDARD
    )

    # Add standard option to portfolio
    portfolio._options[std_option.instrument_name] = std_option

    # For standard contracts, we should NOT set up initial_option_usd_value
    # But we'll set it to verify it's not used
    portfolio.initial_option_usd_value = {
        std_option.instrument_name: [0.0, 0.0]  # Should be ignored for standard contracts
    }

    # Process initial hedge - should not create any hedge for standard contracts
    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)

    # Verify no hedge was executed for standard contract
    assert len(portfolio.hedge_calls) == 0, \
        "Standard contract should not trigger initial USD hedge"

    # Clean up
    await hedger.stop()

@pytest.mark.asyncio
async def test_option_model_net_delta_various_portfolios():
    """
    Test OptionModel.calculate_portfolio_net_delta for:
    1. Only options
    2. Options + futures (futures delta should be excluded)
    3. Empty portfolio
    """
    # Setup
    portfolio = Portfolio()
    dummy_client = DummyDeribitClient()
    option_model = OptionModel(portfolio, deribit_client=dummy_client)
    price = 10000.0
    vol = 0.5

    # 1. Only options: 1 BTC call, ATM
    expiry = datetime.now(timezone.utc) + timedelta(days=7)
    call = VanillaOption(
        instrument_name="BTC-30JUN24-10000-C",
        option_type=OptionType.CALL,
        strike=10000.0,
        expiry=expiry,
        quantity=1.0,
        underlying="BTC",
        contract_type=ContractType.INVERSE
    )
    await portfolio.add_option(call, premium_usd=0.0)

    # Populate the dummy cache with a realistic BTC-denominated option price
    dummy_client.price_iv_cache[call.instrument_name] = {"mark_price": 0.05, "iv": 0.5}

    # Calculate net delta with only options
    net_delta = await option_model.calculate_portfolio_net_delta(price, vol)

    # Calculate expected delta (only from options, futures delta is excluded)
    d1 = option_model.bs_model.calculate_d1(
        S=price,
        K=call.strike,
        T=(call.expiry - datetime.now(timezone.utc)).total_seconds() / (365 * 24 * 3600),
        r=0.0,
        sigma=vol
    )
    call_delta = option_model.bs_model.calculate_delta(call.option_type, d1)
    expected_delta = (call_delta - 0.05) * 1.0  # (bs_delta - mark_price) * qty

    # Verify only options delta is considered
    assert abs(net_delta - expected_delta) < 0.01

    # 2. Options + futures: add +10000 USD futures (1 BTC equivalent at $10,000)
    # This should not affect the delta calculation as futures delta is excluded
    portfolio.futures_position = 10000.0  # 1 BTC equivalent at $10,000
    net_delta2 = await option_model.calculate_portfolio_net_delta(price, vol)

    # Should be the same as before (only options delta, futures delta excluded)
    assert abs(net_delta2 - expected_delta) < 0.01

    # 3. Empty portfolio: create a fresh portfolio to ensure no residual state
    empty_portfolio = DummyPortfolio()
    # Ensure no static hedge position
    empty_portfolio._initial_usd_hedge_position = 0.0
    # Create a new option model with the clean portfolio
    empty_option_model = OptionModel(empty_portfolio, dummy_client)

    # Call with include_static_hedge=False to exclude any static hedge from the calculation
    net_delta3 = await empty_option_model.calculate_portfolio_net_delta(
        price,
        vol,
        include_static_hedge=False
    )

    # The delta should be 0.0 with no options and no static hedge
    assert abs(net_delta3) < 0.01, f"Expected delta to be ~0.0, got {net_delta3}"

@pytest.fixture
def hedger_config():
    return HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="absolute",
        ddh_step_size=1,
        instrument_name="BTC-PERPETUAL",
        volatility=0.4,
        price_check_interval=0.1,
        min_hedge_usd=10.0
    )

@pytest.fixture
def mock_portfolio():
    return DummyPortfolio()

@pytest.fixture
def mock_client():
    return DummyDeribitClient()

@pytest.mark.asyncio
async def test_hedger_basic_delta_zero(hedger_config, mock_portfolio, mock_client):
    """Test that the hedger doesn't hedge when delta is already at target."""
    # Setup
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_client)

    # Mock the current price and delta calculation
    hedger.current_price = 100.0
    hedger.cur_delta = 0.0  # Already at target

    # Run the hedge check
    await hedger._execute_hedge_if_needed()

    # Verify no hedge was executed
    assert len(mock_portfolio.hedge_calls) == 0

@pytest.mark.asyncio
async def test_hedger_triggers_hedge(hedger_config, mock_portfolio, mock_client):
    """Test that the hedger triggers a hedge when delta exceeds threshold."""
    # Setup
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_client)

    # Mock the current price and set delta above threshold
    hedger.current_price = 100.0
    hedger.cur_delta = 0.1  # Above threshold

    # Run the hedge check
    await hedger._execute_hedge_if_needed()

    # Verify hedge was executed
    assert len(mock_portfolio.hedge_calls) == 1
    usd_qty, price = mock_portfolio.hedge_calls[0]
    assert usd_qty < 0  # Should sell to reduce delta
    assert price == 100.0

@pytest.mark.asyncio
async def test_hedger_sign_and_direction(hedger_config, mock_portfolio, mock_client):
    """Test that the hedger correctly handles both positive and negative deltas."""
    # Setup
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_client)
    hedger.current_price = 100.0

    # Test SELL direction (cur_delta > 0)
    hedger.cur_delta = 0.3
    hedger.target_delta = 0.0
    await hedger._execute_hedge_if_needed()

    # Verify SELL order was placed
    assert len(mock_portfolio.hedge_calls) == 1
    usd_qty, price = mock_portfolio.hedge_calls[0]
    assert usd_qty < 0  # Negative for SELL
    assert price == 100.0

    # Reset for next test
    mock_portfolio.hedge_calls.clear()
    mock_portfolio._futures_position = 0.0

    # Test BUY direction (cur_delta < 0)
    hedger.cur_delta = -0.5
    hedger.target_delta = 0.0
    await hedger._execute_hedge_if_needed()

    # Verify BUY order was placed
    assert len(mock_portfolio.hedge_calls) == 1
    usd_qty, price = mock_portfolio.hedge_calls[0]
    assert usd_qty > 0  # Positive for BUY
    assert price == 100.0

@pytest.mark.asyncio
async def test_hedge_direction_after_fix(hedger_config, mock_portfolio, mock_client, caplog):
    """Regression test for hedge direction fix.

    Verifies that a positive delta results in a SELL order (negative usd_qty) and
    a negative delta results in a BUY order (positive usd_qty).
    """
    # Enable debug logging
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # Setup test with a portfolio that has a positive delta from options
    mock_portfolio._total_delta = 1.0  # 1 BTC positive delta
    mock_portfolio._futures_position = 0.0  # No futures position

    # Add a test option to the portfolio to ensure delta calculation works
    from datetime import datetime, timezone, timedelta
    from dneutral_sniper.models import VanillaOption, OptionType, ContractType

    # Create a test option with a delta of 0.5 (for testing)
    test_option = VanillaOption(
        instrument_name="BTC-31DEC25-100000-C",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime.now(timezone.utc) + timedelta(days=365),
        quantity=2.0,  # 2 contracts with 0.5 delta each = 1.0 delta
        underlying="BTC",
        usd_value=5000.0,
        mark_price=0.05,
        contract_type=ContractType.INVERSE
    )

    # Add the option to the portfolio
    mock_portfolio._options = {test_option.instrument_name: test_option}

    # Create a copy of the config with a lower min_hedge_usd to ensure the hedge is executed
    from copy import deepcopy
    config = deepcopy(hedger_config)
    config.min_hedge_usd = 1.0  # Lower the minimum hedge amount
    config.ddh_min_trigger_delta = 0.001  # Lower the minimum trigger delta

    # Create and start hedger with the updated config
    hedger = DynamicDeltaHedger(config, mock_portfolio, mock_client)
    await hedger.start()

    # Verify the hedger started correctly
    assert hedger.ddh_enabled, "Hedger should be enabled after start"
    assert hasattr(hedger, '_hedging_task'), "Hedger should have a _hedging_task"
    assert not hedger._hedging_task.done(), "Hedging task should be running"

    # Set current price
    current_price = 50000.0
    await hedger._update_price(current_price)

    # Verify price was updated
    assert hedger.current_price == current_price, "Current price should be updated"

    # Process hedging cycle
    await hedger._process_hedging_cycle()

    # Debug: Print all log messages
    for record in caplog.records:
        print(f"{record.levelname}: {record.message}")

    # Verify hedge direction - with long calls, we should SELL to hedge
    assert len(mock_portfolio.hedge_calls) > 0, "No hedge calls were made"
    usd_qty, price = mock_portfolio.hedge_calls[0]
    # For long calls (positive delta), we need to SELL (negative usd_qty) to hedge
    assert usd_qty < 0, f"Expected negative usd_qty for SELL to hedge long calls, got {usd_qty}"
    assert price == current_price, f"Expected price {current_price}, got {price}"

    # Create a fresh portfolio for negative delta test
    mock_portfolio = DummyPortfolio()

    # Create a short call option (negative quantity) with delta of -0.5
    short_call = VanillaOption(
        instrument_name="BTC-31DEC25-100000-C",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime.now(timezone.utc) + timedelta(days=365),
        quantity=-2.0,  # Negative quantity for short position
        underlying="BTC",
        usd_value=-5000.0,  # Negative value for short position
        mark_price=0.05,
        delta=0.5,  # Delta will be multiplied by quantity (-2.0 * 0.5 = -1.0)
        contract_type=ContractType.INVERSE
    )

    # Add the short option to the portfolio
    mock_portfolio._options = {short_call.instrument_name: short_call}

    # Reset futures position
    mock_portfolio._futures_position = 0.0
    mock_portfolio._initial_usd_hedge_position = 0.0

    # Create a new hedger instance with the fresh portfolio
    hedger = DynamicDeltaHedger(config, mock_portfolio, mock_client)
    await hedger.start()

    # Update the price to ensure the hedge is processed
    current_price += 1.0
    await hedger._update_price(current_price)

    # Process hedging cycle again
    await hedger._process_hedging_cycle()

    # Verify hedge direction - with short calls, we should BUY to hedge
    assert len(mock_portfolio.hedge_calls) > 0, "No hedge calls were made for negative delta"
    usd_qty, price = mock_portfolio.hedge_calls[0]
    # For short calls (negative delta), we need to BUY (positive usd_qty) to hedge
    assert usd_qty > 0, f"Expected positive usd_qty for BUY to hedge short calls, got {usd_qty}"
    assert price == current_price, f"Expected price {current_price}, got {price}"

    # Clean up
    await hedger.stop()
    assert not hasattr(hedger, '_hedging_task'), "Hedging task should be cleaned up"

@pytest.mark.asyncio
async def test_negative_premium_hedge(hedger_config, mock_client):
    """Test that selling an option with premium creates a negative hedge position.

    When an option is sold (negative quantity) with a premium, we should record a negative
    initial_option_usd_value and hedge by selling futures (negative hedge).
    """
    # Create a fresh portfolio for this test
    portfolio = DummyPortfolio()

    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('test_negative_premium_hedge')

    # Create a sold option (negative quantity) with premium
    option = VanillaOption(
        instrument_name="BTC-31DEC25-100000-C",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=2000.0,  # Premium received for selling the option
        mark_price=0.05,
        iv=0.8,  # Implied volatility
        delta=0.5,
        contract_type=ContractType.INVERSE  # Use INVERSE for this test
    )

    # Add option to portfolio with premium (sold option)
    logger.debug("Adding option to portfolio")
    portfolio._options[option.instrument_name] = option

    # For inverse options, we should set up the initial_option_usd_value
    # For a sold option, we store negative premium to indicate hedge needed
    portfolio.initial_option_usd_value = {
        option.instrument_name: [-2000.0, 0.0]  # (needed_hedge, actual_hedge)
    }
    logger.debug(f"Set initial_option_usd_value: {portfolio.initial_option_usd_value}")

    # Create and start hedger
    logger.debug("Creating and starting hedger")
    hedger = DynamicDeltaHedger(hedger_config, portfolio, mock_client)
    await hedger.start()

    # Set current price
    current_price = 90000.0
    logger.debug(f"Setting current price to {current_price}")
    await hedger._update_price(current_price)

    # Process initial hedge - ensure portfolio is not hedged yet
    logger.debug("Executing initial option premium hedge if needed")
    portfolio.initial_usd_hedged = False
    portfolio.initial_usd_hedge_position = 0.0
    logger.debug(f"Reset portfolio - initial_usd_hedged: {portfolio.initial_usd_hedged}, initial_usd_hedge_position: {portfolio.initial_usd_hedge_position}")

    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)

    # Debug: Print portfolio state
    logger.debug(f"Portfolio state: {portfolio.__dict__}")
    logger.debug(f"Initial option USD value: {portfolio.initial_option_usd_value}")

    # For inverse options with a static hedge, we should see the initial_usd_hedge_position updated
    # to reflect the hedge amount (negative for sold options)
    expected_hedge = -2000.0  # Negative for sold options

    # Check if the initial_usd_hedge_position was updated
    assert abs(portfolio.initial_usd_hedge_position - expected_hedge) <= hedger_config.min_hedge_usd, \
        f"Initial USD hedge position {portfolio.initial_usd_hedge_position:.2f} should be close to {expected_hedge:.2f}"

    # Verify the portfolio is marked as hedged
    assert portfolio.initial_usd_hedged, "Portfolio should be marked as hedged"

    # Check that the hedge was recorded in the portfolio
    assert option.instrument_name in portfolio.initial_option_usd_value
    needed, actual = portfolio.initial_option_usd_value[option.instrument_name]
    assert needed == -2000.0, "Needed hedge should be negative for sold inverse option"
    assert actual < 0, "Actual hedge should be negative (futures sold)"

    # Now test with a standard contract type - should not use initial USD hedge
    portfolio.hedge_calls.clear()  # Clear previous hedge calls
    portfolio.initial_option_usd_value.clear()  # Clear previous hedge values

    # Create a standard contract option
    std_option = VanillaOption(
        instrument_name="BTC-31DEC24-100000-C-STD",
        option_type=OptionType.CALL,
        strike=100000.0,
        expiry=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        quantity=-1.0,  # Negative quantity indicates sold option
        underlying="BTC",
        usd_value=2000.0,  # Premium received for selling the option
        mark_price=2000.0,
        iv=0.8,
        delta=0.3,
        contract_type=ContractType.STANDARD
    )

    # Add standard option to portfolio
    portfolio._options[std_option.instrument_name] = std_option

    # For standard contracts, we should NOT set up initial_option_usd_value
    # But we'll set it to verify it's not used
    portfolio.initial_option_usd_value = {
        std_option.instrument_name: [0.0, 0.0]  # Should be ignored for standard contracts
    }

    # Process initial hedge - should not create any hedge for standard contracts
    await hedger._execute_initial_option_premium_hedge_if_needed(current_price)

    # Verify no hedge was executed for standard contract
    assert len(portfolio.hedge_calls) == 0, \
        "Standard contract should not trigger initial USD hedge"

    # Clean up
    await hedger.stop()
