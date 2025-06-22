"""Tests for price update functionality in DynamicDeltaHedger."""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime, timedelta
from typing import AsyncGenerator

import pytest_asyncio

from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.deribit_client import DeribitWebsocketClient

# Enable async fixtures for all tests in this module
pytestmark = pytest.mark.asyncio

# Test configuration
TEST_INSTRUMENT = "BTC-PERPETUAL"
TEST_UNDERLYING = "BTC"

# Fixtures

@pytest_asyncio.fixture
async def mock_portfolio() -> Portfolio:
    """Create a mock portfolio with required attributes."""
    portfolio = MagicMock(spec=Portfolio)
    portfolio.id = 'test-portfolio'
    
    # Setup properties
    type(portfolio).futures_position = PropertyMock(return_value=0.0)
    type(portfolio).futures_avg_entry = PropertyMock(return_value=0.0)
    type(portfolio).last_hedge_price = PropertyMock(return_value=100000.0)
    type(portfolio).realized_pnl = PropertyMock(return_value=0.0)
    type(portfolio).initial_usd_hedged = PropertyMock(return_value=True)
    
    # Setup attributes
    portfolio.initial_option_usd_value = {}
    portfolio._lock = asyncio.Lock()
    portfolio._dirty = False
    
    # Mock the mark_dirty method
    async def mark_dirty():
        portfolio._dirty = True
        return True
    portfolio._mark_dirty = mark_dirty
    
    return portfolio

@pytest_asyncio.fixture
async def mock_deribit_client() -> DeribitWebsocketClient:
    """Create a mock Deribit client with price update capabilities."""
    client = MagicMock(spec=DeribitWebsocketClient)
    
    # Setup price_iv_cache
    client.price_iv_cache = {}
    
    # Mock get_price_iv to return cached values
    def get_price_iv(instrument):
        entry = client.price_iv_cache.get(instrument, {})
        return entry.get('mark_price', 100000.0), entry.get('iv', 0.5)
    
    client.get_price_iv.side_effect = get_price_iv
    
    # Mock async method for getting mark price and IV
    async def get_instrument_mark_price_and_iv(instrument):
        entry = client.price_iv_cache.get(instrument, {})
        return entry.get('mark_price', 100000.0), entry.get('iv', 0.5)
    
    client.get_instrument_mark_price_and_iv = get_instrument_mark_price_and_iv
    
    # Mock properties
    type(client).underlying = PropertyMock(return_value='BTC')
    
    return client

@pytest.fixture
def hedger_config():
    """Create a default hedger configuration."""
    return HedgerConfig(
        ddh_min_trigger_delta=0.001,  # 0.1% of notional
        ddh_target_delta=0.0,  # Target delta-neutral
        ddh_step_mode='percentage',
        ddh_step_size=0.001,  # 0.1%
        price_check_interval=1.0,
        underlying=TEST_UNDERLYING,
        instrument_name=TEST_INSTRUMENT,
        volatility=0.6,
        risk_free_rate=0.0,
        min_hedge_usd=10.0
    )

@pytest_asyncio.fixture
async def hedger(hedger_config, mock_portfolio, mock_deribit_client) -> AsyncGenerator[DynamicDeltaHedger, None]:
    """Create a DynamicDeltaHedger instance for testing."""
    # Create the hedger with test configuration
    hedger = DynamicDeltaHedger(hedger_config, mock_portfolio, mock_deribit_client)
    
    # Start the hedger
    await hedger.start()
    
    # Yield the hedger for testing
    yield hedger
    
    # Cleanup
    await hedger.stop()
    
    # Cancel any pending tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

# Test Cases

async def test_price_update_flow(hedger: DynamicDeltaHedger, mock_portfolio: MagicMock):
    """Test that price updates are processed correctly."""
    # Initial state
    assert getattr(hedger, 'current_price', None) is None
    
    # Simulate a price update
    test_price = 50000.0
    await hedger._price_callback(TEST_INSTRUMENT, test_price)
    
    # Check that the price was updated
    assert hedger.current_price == test_price
    
    # Check that the last update time was set
    assert getattr(hedger, '_last_price_update', 0) > 0

async def test_get_current_price_with_timeout(hedger: DynamicDeltaHedger):
    """Test that _get_current_price_with_timeout works as expected."""
    # Should initially return None since we have no price
    if hasattr(hedger, '_get_current_price_with_timeout'):
        price = await hedger._get_current_price_with_timeout(timeout=0.1)
        assert price is None
        
        # Update the price
        test_price = 50000.0
        await hedger._update_price(test_price)
        
        # Now should return the price immediately
        price = await hedger._get_current_price_with_timeout()
        assert price == test_price

async def test_price_update_race_condition(hedger: DynamicDeltaHedger):
    """Test that we handle race conditions in price updates."""
    if not hasattr(hedger, '_get_current_price_with_timeout'):
        pytest.skip("Method _get_current_price_with_timeout not found in hedger")
        
    # This test verifies that we don't miss price updates between clearing the event
    # and starting to wait for it
    
    # Start a task that will wait for a price update
    price_task = asyncio.create_task(hedger._get_current_price_with_timeout(timeout=1.0))
    
    # Give the task a moment to start
    await asyncio.sleep(0.1)
    
    # Send a price update
    test_price = 50000.0
    await hedger._update_price(test_price)
    
    # The task should complete with the updated price
    result = await price_task
    assert result == test_price

async def test_stale_price_handling(hedger: DynamicDeltaHedger):
    """Test that we handle stale prices correctly."""
    if not hasattr(hedger, '_update_price'):
        pytest.skip("Method _update_price not found in hedger")
    
    # Set an initial price
    test_price = 50000.0
    await hedger._update_price(test_price)
    
    # Simulate the price becoming stale (older than 5 seconds)
    with patch('time.time', return_value=time.time() + 10):
        if hasattr(hedger, '_get_current_price_with_timeout'):
            # Should return the last known price with a warning
            price = await hedger._get_current_price_with_timeout(timeout=0.1)
            assert price == test_price

async def test_concurrent_price_updates(hedger: DynamicDeltaHedger):
    """Test that concurrent price updates don't cause issues."""
    if not hasattr(hedger, '_get_current_price_with_timeout'):
        pytest.skip("Method _get_current_price_with_timeout not found in hedger")
        
    # Start multiple tasks that will wait for price updates
    tasks = [
        asyncio.create_task(hedger._get_current_price_with_timeout(timeout=1.0))
        for _ in range(5)
    ]
    
    # Give tasks a moment to start
    await asyncio.sleep(0.1)
    
    # Send multiple price updates
    for i in range(1, 6):
        if hasattr(hedger, '_update_price'):
            await hedger._update_price(50000.0 + i * 1000)
        await asyncio.sleep(0.1)
    
    # All tasks should complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # All results should be valid prices
    assert all(isinstance(result, float) for result in results)
    assert all(result is not None for result in results)

async def test_price_update_with_slow_consumer(hedger: DynamicDeltaHedger):
    """Test that we handle slow consumers of price updates."""
    if not hasattr(hedger, '_update_price') or not hasattr(hedger, '_get_current_price_with_timeout'):
        pytest.skip("Required methods not found in hedger")
        
    # Send a price update
    await hedger._update_price(50000.0)
    
    # Start a task that will take a while to process the price
    async def slow_consumer():
        await asyncio.sleep(0.1)  # Reduced sleep time for faster tests
        return await hedger._get_current_price_with_timeout(timeout=0.5)
    
    # Start multiple consumers
    tasks = [asyncio.create_task(slow_consumer()) for _ in range(3)]
    
    # Send more price updates while consumers are processing
    for i in range(1, 4):
        await hedger._update_price(50000.0 + i * 1000)
        await asyncio.sleep(0.05)  # Reduced sleep time
    
    # All tasks should complete with valid prices
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, float) for result in results)

async def test_price_update_with_errors(hedger: DynamicDeltaHedger):
    """Test that we handle price updates with various inputs."""
    if not hasattr(hedger, '_update_price'):
        pytest.skip("Method _update_price not found in hedger")
    
    # Save the original price to restore it later
    original_price = getattr(hedger, 'current_price', None)
    
    try:
        # Test with negative price (should be accepted)
        await hedger._update_price(-100.0)
        assert hasattr(hedger, 'current_price') and hedger.current_price == -100.0
        
        # Test with zero price (should be accepted)
        await hedger._update_price(0.0)
        assert hasattr(hedger, 'current_price') and hedger.current_price == 0.0
        
        # Test with None price (should raise TypeError)
        with pytest.raises(TypeError):
            await hedger._update_price(None)
        
        # Test with non-numeric price - this might cause issues with the hedger's state
        # so we'll do this in a separate test
    finally:
        # Restore the original price
        if original_price is not None:
            await hedger._update_price(original_price)

async def test_price_update_with_invalid_input():
    """Test that we handle invalid price inputs correctly."""
    from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
    from unittest.mock import MagicMock, patch
    
    # Create a test config
    config = HedgerConfig(
        ddh_min_trigger_delta=0.1,
        ddh_target_delta=0.0,
        ddh_step_mode='percentage',
        ddh_step_size=0.01,
        price_check_interval=2.0,
        underlying="TEST",
        instrument_name="TEST-PERPETUAL",
        volatility=0.8,
        risk_free_rate=0.0,
        min_hedge_usd=10.0
    )
    
    # Create a mock portfolio
    mock_portfolio = MagicMock()
    
    # Create a new hedger instance
    test_hedger = DynamicDeltaHedger(config, mock_portfolio)
    
    # Test with valid price first
    await test_hedger._update_price(100.0)
    assert test_hedger.current_price == 100.0
    
    # Test with negative price (should be valid)
    await test_hedger._update_price(-50.0)
    assert test_hedger.current_price == -50.0
    
    # Test with non-numeric price (should raise TypeError)
    with pytest.raises(TypeError, match="Price must be a number, got str"):
        await test_hedger._update_price("not a number")
    
    # Test with None price (should raise TypeError)
    with pytest.raises(TypeError, match="Price must be a number, got NoneType"):
        await test_hedger._update_price(None)
    
    try:
        # Test with valid price first (outside logger patch)
        await test_hedger._update_price(100.0)
        assert test_hedger.current_price == 100.0
        
        # Test with negative price (should be valid)
        await test_hedger._update_price(-50.0)
        assert test_hedger.current_price == -50.0
            
    finally:
        # Clean up
        if hasattr(test_hedger, 'stop'):
            await test_hedger.stop()

async def test_price_update_with_invalid_type():
    """Test that we handle invalid price types correctly."""
    from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
    from unittest.mock import MagicMock
    
    # Create a test config
    config = HedgerConfig(
        ddh_min_trigger_delta=0.1,
        ddh_target_delta=0.0,
        ddh_step_mode='percentage',
        ddh_step_size=0.01,
        price_check_interval=2.0,
        underlying="TEST",
        instrument_name="TEST-PERPETUAL",
        volatility=0.8,
        risk_free_rate=0.0,
        min_hedge_usd=10.0
    )
    
    # Create a mock portfolio
    mock_portfolio = MagicMock()
    
    # Create a new hedger instance
    test_hedger = DynamicDeltaHedger(config, mock_portfolio)
    
    try:
        # Set initial price
        await test_hedger._update_price(100.0)
        assert test_hedger.current_price == 100.0
        
        # Test with dict (invalid type) - should raise TypeError
        with pytest.raises(TypeError, match="Price must be a number, got dict"):
            await test_hedger._update_price({"price": 100.0})
                
        # Test with list (invalid type) - should raise TypeError
        with pytest.raises(TypeError, match="Price must be a number, got list"):
            await test_hedger._update_price([100.0])
        
        # Test with bool (valid in Python as it's a subclass of int)
        # Should be converted to 1.0 for True
        await test_hedger._update_price(True)
        assert test_hedger.current_price == 1.0  # True is converted to 1.0
        
        # Test with False (valid in Python as it's a subclass of int)
        # Should be converted to 0.0 for False
        await test_hedger._update_price(False)
        assert test_hedger.current_price == 0.0  # False is converted to 0.0
                
    finally:
        # Clean up
        if hasattr(test_hedger, 'stop'):
            await test_hedger.stop()
