"""
Tests for HedgingManager initialization and basic functionality.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dneutral_sniper.hedging_manager import HedgingManager, HedgerConfig
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager
from dneutral_sniper.deribit_client import DeribitWebsocketClient


@pytest.fixture
def mock_managers():
    """Create mock manager instances for testing."""
    portfolio_manager = MagicMock(spec=PortfolioManager)
    subscription_manager = MagicMock(spec=SubscriptionManager)
    deribit_client = MagicMock(spec=DeribitWebsocketClient)
    
    return portfolio_manager, subscription_manager, deribit_client


@pytest.mark.asyncio
async def test_hedging_manager_init_with_default_config(mock_managers):
    """Test HedgingManager initialization with default configuration."""
    portfolio_manager, subscription_manager, deribit_client = mock_managers
    
    # Initialize with default config
    hedging_manager = HedgingManager(
        portfolio_manager=portfolio_manager,
        subscription_manager=subscription_manager,
        deribit_client=deribit_client
    )
    
    assert hedging_manager is not None
    assert isinstance(hedging_manager, HedgingManager)
    assert hedging_manager.default_hedger_config == {}


@pytest.mark.asyncio
async def test_hedging_manager_init_with_hedger_config(mock_managers):
    """Test HedgingManager initialization with HedgerConfig object."""
    portfolio_manager, subscription_manager, deribit_client = mock_managers
    
    # Create a HedgerConfig object
    config = HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="percentage",
        ddh_step_size=0.01,
        price_check_interval=2.0,
        min_hedge_usd=10.0
    )
    
    # Initialize with HedgerConfig
    hedging_manager = HedgingManager(
        portfolio_manager=portfolio_manager,
        subscription_manager=subscription_manager,
        deribit_client=deribit_client,
        default_hedger_config=config
    )
    
    assert hedging_manager is not None
    assert isinstance(hedging_manager, HedgingManager)
    assert isinstance(hedging_manager.default_hedger_config, dict)
    assert hedging_manager.default_hedger_config["ddh_min_trigger_delta"] == 0.01
    assert hedging_manager.default_hedger_config["ddh_target_delta"] == 0.0
    assert hedging_manager.default_hedger_config["ddh_step_mode"] == "percentage"
    assert hedging_manager.default_hedger_config["ddh_step_size"] == 0.01
    assert hedging_manager.default_hedger_config["price_check_interval"] == 2.0
    assert hedging_manager.default_hedger_config["min_hedge_usd"] == 10.0


@pytest.mark.asyncio
async def test_hedging_manager_init_with_dict_config(mock_managers):
    """Test HedgingManager initialization with dictionary config."""
    portfolio_manager, subscription_manager, deribit_client = mock_managers
    
    # Create a dictionary config
    config = {
        "ddh_min_trigger_delta": 0.02,
        "ddh_target_delta": 0.0,
        "ddh_step_mode": "fixed",
        "ddh_step_size": 0.02,
        "price_check_interval": 1.0,
        "min_hedge_usd": 5.0
    }
    
    # Initialize with dict config
    hedging_manager = HedgingManager(
        portfolio_manager=portfolio_manager,
        subscription_manager=subscription_manager,
        deribit_client=deribit_client,
        default_hedger_config=config
    )
    
    assert hedging_manager is not None
    assert isinstance(hedging_manager, HedgingManager)
    assert isinstance(hedging_manager.default_hedger_config, dict)
    assert hedging_manager.default_hedger_config["ddh_min_trigger_delta"] == 0.02
    assert hedging_manager.default_hedger_config["ddh_target_delta"] == 0.0
    assert hedging_manager.default_hedger_config["ddh_step_mode"] == "fixed"
    assert hedging_manager.default_hedger_config["ddh_step_size"] == 0.02
    assert hedging_manager.default_hedger_config["price_check_interval"] == 1.0
    assert hedging_manager.default_hedger_config["min_hedge_usd"] == 5.0
