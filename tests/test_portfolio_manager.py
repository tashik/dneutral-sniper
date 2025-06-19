"""Tests for the PortfolioManager class."""

import asyncio
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.models import OptionType, ContractType, VanillaOption
from dneutral_sniper.portfolio import Portfolio

# Enable async test support
pytestmark = pytest.mark.asyncio

# Test fixtures

@pytest.fixture
def temp_portfolios_dir():
    """Create a temporary directory for testing portfolio files."""
    temp_dir = tempfile.mkdtemp(prefix="test_data_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
async def portfolio_manager(temp_portfolios_dir):
    """Create a PortfolioManager instance with a temporary directory."""
    manager = PortfolioManager(data_dir=temp_portfolios_dir)
    await manager.initialize()
    return manager

@pytest.fixture
def sample_option():
    """Create a sample option for testing."""
    return {
        "instrument_name": "BTC-31DEC25-100000-C",
        "quantity": 1.0,
        "strike": 100000.0,
        "expiry": "2025-12-31T08:00:00+00:00",
        "option_type": "call",
        "contract_type": "inverse",
        "mark_price": 0.05,
        "iv": 0.8,
        "usd_value": 5000.0,
        "delta": 0.5
    }

# Test cases

class TestPortfolioManager:
    """Test cases for the PortfolioManager class."""

    @pytest_asyncio.fixture
    async def portfolio_manager(self, temp_portfolios_dir):
        """Create a PortfolioManager instance with a temporary directory."""
        manager = PortfolioManager(data_dir=temp_portfolios_dir)
        await manager.initialize()
        return manager

    async def test_initialize_creates_default_portfolio(self, portfolio_manager, temp_portfolios_dir):
        """Test that initializing creates a default portfolio if none exist."""
        # Should have created a default portfolio
        portfolio_ids = await portfolio_manager.list_portfolios()
        assert len(portfolio_ids) == 1
        assert portfolio_ids[0] == "default"

        # Get the portfolio to verify its properties
        portfolio = await portfolio_manager.get_portfolio("default")
        assert portfolio is not None
        assert portfolio.underlying == "BTC"
        assert getattr(portfolio, 'initial_balance', 0.0) == 0.0

        # Verify the portfolio file was created
        portfolio_file = Path(temp_portfolios_dir) / "portfolio_default.json"
        assert portfolio_file.exists()

    async def test_create_portfolio(self, portfolio_manager, temp_portfolios_dir):
        """Test creating a new portfolio with a custom ID."""
        # Create a new portfolio
        portfolio_id = "test_portfolio"
        underlying = "ETH"
        initial_balance = 10000.0

        new_id, portfolio = await portfolio_manager.create_portfolio(
            portfolio_id=portfolio_id,
            underlying=underlying,
            initial_balance=initial_balance
        )

        assert new_id == portfolio_id
        assert portfolio.underlying == underlying
        assert getattr(portfolio, 'initial_balance', 0.0) == initial_balance

        # Verify the portfolio file was created
        portfolio_file = Path(temp_portfolios_dir) / f"portfolio_{portfolio_id}.json"
        assert portfolio_file.exists()

        # Verify the portfolio is in the list
        portfolio_ids = await portfolio_manager.list_portfolios()
        assert portfolio_id in portfolio_ids

    async def test_get_portfolio(self, portfolio_manager):
        """Test getting a portfolio by ID."""
        # Create a test portfolio
        portfolio_id = "test_get"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id)

        # Get the portfolio
        portfolio = await portfolio_manager.get_portfolio(portfolio_id)
        assert portfolio is not None
        assert isinstance(portfolio, Portfolio)

        # Test getting non-existent portfolio
        non_existent = await portfolio_manager.get_portfolio("non_existent")
        assert non_existent is None

    async def test_delete_portfolio(self, portfolio_manager, temp_portfolios_dir):
        """Test deleting a portfolio."""
        # Create a test portfolio
        portfolio_id = "test_delete"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id)

        # Verify it exists
        portfolio_file = Path(temp_portfolios_dir) / f"portfolio_{portfolio_id}.json"
        assert portfolio_file.exists()

        # Delete the portfolio
        result = await portfolio_manager.delete_portfolio(portfolio_id)
        assert result is True

        # Verify it was deleted
        assert not portfolio_file.exists()
        portfolio = await portfolio_manager.get_portfolio(portfolio_id)
        assert portfolio is None

        # Test deleting non-existent portfolio
        result = await portfolio_manager.delete_portfolio("non_existent")
        assert result is False

    async def test_add_option_to_portfolio(self, portfolio_manager):
        """Test adding an option to a portfolio."""
        # Create a test portfolio
        portfolio_id = "test_options"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id, underlying="BTC")

        # Add an option with individual parameters
        result = await portfolio_manager.add_option_to_portfolio(
            portfolio_id=portfolio_id,
            option_instrument="BTC-31DEC25-100000-C",
            quantity=1.0,
            strike=100000.0,
            expiry="2025-12-31T08:00:00+00:00",
            option_type="call",
            contract_type="inverse",
            mark_price=0.05,
            iv=0.8,
            usd_value=5000.0,
            delta=0.5
        )
        assert result is True

        # Verify the option was added
        portfolio = await portfolio_manager.get_portfolio(portfolio_id)
        assert portfolio is not None
        assert hasattr(portfolio, 'options')
        assert "BTC-31DEC25-100000-C" in portfolio.options

        option = portfolio.options["BTC-31DEC25-100000-C"]
        assert option.quantity == 1.0
        assert option.strike == 100000.0
        assert option.mark_price == 0.05
        assert option.iv == 0.8
        assert option.usd_value == 5000.0
        assert option.delta == 0.5
        assert option.option_type == OptionType("call")
        assert option.contract_type == ContractType("inverse")

    async def test_get_subscribed_instruments(self, portfolio_manager):
        """Test getting subscribed instruments for a portfolio."""
        # Create a test portfolio with options
        portfolio_id = "test_subscriptions"
        underlying = "BTC"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id, underlying=underlying)

        # Add some options with individual parameters
        await portfolio_manager.add_option_to_portfolio(
            portfolio_id=portfolio_id,
            option_instrument="BTC-31DEC25-100000-C",
            quantity=1.0,
            strike=100000.0,
            expiry="2025-12-31T08:00:00+00:00",
            option_type="call",
            contract_type="inverse"
        )

        await portfolio_manager.add_option_to_portfolio(
            portfolio_id=portfolio_id,
            option_instrument="BTC-31DEC25-90000-P",
            quantity=1.0,
            strike=90000.0,
            expiry="2025-12-31T08:00:00+00:00",
            option_type="put",
            contract_type="inverse"
        )

        # Get subscribed instruments
        instruments = await portfolio_manager.get_subscribed_instruments(portfolio_id)

        # Should include the perpetual and both options
        assert len(instruments) == 3
        assert f"{underlying.upper()}-PERPETUAL" in instruments
        assert "BTC-31DEC25-100000-C" in instruments
        assert "BTC-31DEC25-90000-P" in instruments

    async def test_save_portfolio(self, portfolio_manager, temp_portfolios_dir):
        """Test saving a portfolio to disk."""
        # Create a test portfolio
        portfolio_id = "test_save"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id)

        # Modify the portfolio
        portfolio = await portfolio_manager.get_portfolio(portfolio_id)
        portfolio.futures_position = 1000.0

        # Save the portfolio
        result = await portfolio_manager.save_portfolio(portfolio_id)
        assert result is True

        # Verify the file was updated
        portfolio_file = Path(temp_portfolios_dir) / f"portfolio_{portfolio_id}.json"
        with open(portfolio_file, 'r') as f:
            data = json.load(f)
            assert data['futures_position'] == 1000.0

    async def test_concurrent_access(self, portfolio_manager):
        """Test that the portfolio manager handles concurrent access safely."""
        portfolio_id = "concurrent_test"
        await portfolio_manager.create_portfolio(portfolio_id=portfolio_id)

        # Number of concurrent operations
        num_operations = 10

        async def modify_portfolio(i):
            # Add a small delay to ensure operations overlap
            await asyncio.sleep(0.01)
            portfolio = await portfolio_manager.get_portfolio(portfolio_id)
            if not hasattr(portfolio, 'counter'):
                portfolio.counter = 0
            portfolio.counter += 1
            await portfolio_manager.save_portfolio(portfolio_id)

        # Run concurrent modifications
        await asyncio.gather(*[modify_portfolio(i) for i in range(num_operations)])

        # Verify all operations were applied
        portfolio = await portfolio_manager.get_portfolio(portfolio_id)
        assert hasattr(portfolio, 'counter')
        assert portfolio.counter == num_operations
