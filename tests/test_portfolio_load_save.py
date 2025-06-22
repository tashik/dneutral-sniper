import asyncio
import logging
import os
import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pytest_asyncio

from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.models import VanillaOption, OptionType, ContractType

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_dir = Path(tempfile.mkdtemp(prefix="test_portfolio_"))
    yield temp_dir
    # Use ignore_errors=True instead of ignore_ok for Python < 3.12 compatibility
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest_asyncio.fixture
async def portfolio_manager(temp_dir):
    """Create a portfolio manager with a temporary directory."""
    manager = PortfolioManager(data_dir=str(temp_dir))
    yield manager
    # Clean up
    await manager.close()

@pytest_asyncio.fixture
async def sample_portfolio(portfolio_manager):
    """Create a sample portfolio with test data."""
    portfolio_id, portfolio = await portfolio_manager.create_portfolio(
        portfolio_id="test_portfolio",
        underlying="BTC"
    )
    
    expiry = datetime.now(timezone.utc) + timedelta(days=30)
    
    # Add a call option
    call_option = VanillaOption(
        instrument_name="BTC-30JUL25-120000-C",
        option_type=OptionType.CALL,
        strike=120000.0,
        expiry=expiry,
        quantity=2.0,
        underlying="BTC",
        contract_type=ContractType.INVERSE,
        mark_price=0.05,
        iv=0.75,
        usd_value=1000.0,
        delta=0.45
    )
    await portfolio.add_option(call_option, premium_usd=1000.0)
    
    # Add a put option
    put_option = VanillaOption(
        instrument_name="BTC-30JUL25-100000-P",
        option_type=OptionType.PUT,
        strike=100000.0,
        expiry=expiry,
        quantity=1.0,
        underlying="BTC",
        contract_type=ContractType.INVERSE,
        mark_price=0.03,
        iv=0.8,
        usd_value=600.0,
        delta=-0.35
    )
    await portfolio.add_option(put_option, premium_usd=600.0)
    
    # Save the portfolio
    await portfolio_manager.save_portfolio(portfolio_id)
    return portfolio

@pytest.mark.asyncio
async def test_portfolio_save_and_load(portfolio_manager, sample_portfolio):
    """Test saving a portfolio to a file and loading it back."""
    # Save the portfolio
    await portfolio_manager.save_portfolio(sample_portfolio.id)
    
    # Verify the file was created
    portfolio_file = Path(portfolio_manager.data_dir) / f"portfolio_{sample_portfolio.id}.json"
    assert portfolio_file.exists(), "Portfolio file was not created"
    
    # Load the portfolio back using the manager
    loaded_portfolio = await portfolio_manager.get_portfolio(sample_portfolio.id)
    
    # Verify basic properties
    assert loaded_portfolio.id == sample_portfolio.id
    assert loaded_portfolio.underlying == sample_portfolio.underlying
    
    # Verify options were loaded correctly
    assert len(loaded_portfolio.options) == len(sample_portfolio.options)
    
    for opt_name, option in sample_portfolio.options.items():
        assert opt_name in loaded_portfolio.options
        loaded_opt = loaded_portfolio.options[opt_name]
        assert loaded_opt.quantity == option.quantity
        assert loaded_opt.strike == option.strike
        assert loaded_opt.expiry == option.expiry
        assert loaded_opt.option_type == option.option_type
        assert loaded_opt.contract_type == option.contract_type
        assert loaded_opt.mark_price == option.mark_price
        assert loaded_opt.iv == option.iv
        assert loaded_opt.usd_value == option.usd_value
        assert loaded_opt.delta == option.delta

@pytest.mark.asyncio
async def test_portfolio_load_existing(portfolio_manager, temp_dir):
    """Test loading an existing portfolio from the data directory."""
    # Create a test portfolio
    portfolio_id, portfolio = await portfolio_manager.create_portfolio(
        portfolio_id="test_load_existing",
        underlying="BTC"
    )
    
    # Add an option
    expiry = datetime.now(timezone.utc) + timedelta(days=30)
    call_option = VanillaOption(
        instrument_name="BTC-30AUG25-125000-C",
        option_type=OptionType.CALL,
        strike=125000.0,
        expiry=expiry,
        quantity=1.0,
        underlying="BTC",
        contract_type=ContractType.INVERSE,
        mark_price=0.04,
        iv=0.7,
        usd_value=800.0,
        delta=0.4
    )
    await portfolio.add_option(call_option, premium_usd=800.0)
    
    # Save the portfolio
    await portfolio_manager.save_portfolio(portfolio_id)
    
    # Create a new portfolio manager that will load the saved portfolio
    new_manager = PortfolioManager(data_dir=str(temp_dir))
    try:
        # Load all portfolios (should load our saved one)
        await new_manager.load_all_portfolios()
        
        # Get the loaded portfolio
        loaded_portfolio = await new_manager.get_portfolio(portfolio_id)
        
        # Verify basic properties
        assert loaded_portfolio is not None
        assert loaded_portfolio.id == portfolio_id
        assert loaded_portfolio.underlying == "BTC"
        assert len(loaded_portfolio.options) == 1
        assert "BTC-30AUG25-125000-C" in loaded_portfolio.options
    finally:
        await new_manager.close()
