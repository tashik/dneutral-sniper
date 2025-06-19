import asyncio
import logging
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from decimal import Decimal
from datetime import datetime, timedelta
import pytest_asyncio

from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.hedging_manager import HedgingManager, DynamicDeltaHedger
from dneutral_sniper.deribit_client import DeribitWebsocketClient
from dneutral_sniper.models import VanillaOption, OptionType, ContractType
from dneutral_sniper.subscription_manager import SubscriptionManager

# Configure test logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Test configuration
TEST_CONFIG = {
    "ddh_min_trigger_delta": 0.01,  # 0.01 BTC
    "ddh_target_delta": 0.0,        # Target delta neutral
    "ddh_step_mode": "percentage",
    "ddh_step_size": 1.0,           # 1% price move
    "price_check_interval": 1.0,     # Check prices every second
    "underlying": "BTC",
    "instrument_name": "BTC-PERPETUAL",
    "volatility": 0.8,              # 80% annualized vol
    "risk_free_rate": 0.0,
    "min_hedge_usd": 10.0            # $10 minimum hedge amount
}

# Test option specifications
OPTION_SPECS = [
    {
        "instrument_name": "BTC-27JUN25-110000-C",
        "quantity": 0.1,       # 0.1 BTC notional
        "trade_price": 0.1,     # 0.1 BTC per option (10% of notional)
        "trade_iv": 0.8,        # 80% IV
        "is_buy": True
    },
    {
        "instrument_name": "BTC-27JUN25-115000-C",
        "quantity": 0.1,       # 0.1 BTC notional
        "trade_price": 0.08,    # 0.08 BTC per option (8% of notional)
        "trade_iv": 0.8,        # 80% IV
        "is_buy": True
    }
]

class TestDynamicHedgingIntegration:
    """Integration test for dynamic hedging with live connection."""

    @pytest.fixture
    def tmp_path():
        """Create a temporary directory for testing portfolio files."""
        temp_dir = tempfile.mkdtemp(prefix="test_data_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture(autouse=True)
    def event_loop(self, request):
        """Create an instance of the default event loop for each test case.

        This fixture is set to autouse=True to ensure it runs for every test.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
        # Cleanup
        loop.close()
        asyncio.set_event_loop(None)

    @pytest_asyncio.fixture
    async def test_portfolio(self, tmp_path):
        """Create a test portfolio with BTC underlying and options."""
        # Create a portfolio manager with a temporary directory
        portfolio_manager = PortfolioManager(data_dir=str(tmp_path))

        # Create a new portfolio
        portfolio_id = "test_hedging_portfolio"
        _, portfolio = await portfolio_manager.create_portfolio(
            portfolio_id=portfolio_id,
            underlying="BTC"
        )

        # Set initial balances using properties
        portfolio.initial_balance = 100000.0  # $100,000 initial balance
        portfolio.btc_balance = 1.0  # 1 BTC initial balance

        # Add options to the portfolio
        for spec in OPTION_SPECS:
            # Parse instrument name (format: BTC-DDMMMYY-STRIKE-C/P)
            parts = spec["instrument_name"].split('-')
            expiry = datetime.strptime(parts[1], '%d%b%y')
            strike = float(parts[2])
            option_type = OptionType.CALL if parts[3] == 'C' else OptionType.PUT

            option = VanillaOption(
                instrument_name=spec["instrument_name"],
                option_type=option_type,
                strike=strike,
                expiry=expiry,
                quantity=spec["quantity"],
                underlying="BTC",
                contract_type=ContractType.INVERSE,
                mark_price=spec["trade_price"],
                iv=spec["trade_iv"],
                usd_value=spec["trade_price"] * spec["quantity"]
            )
            # Pass premium_usd to trigger trade recording
            premium_usd = option.mark_price * option.quantity
            await portfolio.add_option(option, premium_usd=premium_usd)

        # Save the portfolio
        await portfolio_manager._save_portfolio(portfolio_id, portfolio)

        # Store the portfolio manager for later use in tests
        self._portfolio_manager = portfolio_manager

        # Ensure the portfolio is saved before returning
        await portfolio_manager._save_portfolio(portfolio_id, portfolio)

        # Return the portfolio directly instead of yielding
        return portfolio

    @pytest.fixture
    def deribit_client(self):
        """Create a mock Deribit client with price updates."""
        client = MagicMock(spec=DeribitWebsocketClient)

        # Mock the subscribe_to_instruments method
        client.subscribe_to_instruments = AsyncMock(return_value=True)
        client.is_connected = AsyncMock(return_value=True)
        client.connect = AsyncMock(return_value=True)
        client.disconnect = AsyncMock()
        client.add_subscription_handler = MagicMock()
        client.remove_subscription_handler = MagicMock()

        # Track subscriptions
        client.subscriptions = set()

        # Mock the get_instrument method to return instrument details
        def mock_get_instrument(instrument_name):
            if instrument_name == "BTC-PERPETUAL":
                return {"min_trade_amount": 0.001, "tick_size": 0.5, "kind": "future"}
            return {"min_trade_amount": 0.1, "tick_size": 0.5, "kind": "option"}

        client.get_instrument = MagicMock(side_effect=mock_get_instrument)

        # Track subscriptions and callbacks
        client.subscriptions = set()
        client._price_callbacks = []

        # Mock price updates
        async def mock_subscribe_to_instruments(instruments):
            for instrument in instruments:
                client.subscriptions.add(instrument)
            return True

        client.subscribe_to_instruments = AsyncMock(side_effect=mock_subscribe_to_instruments)

        # Mock set_price_callback to store the callback
        def mock_set_price_callback(callback):
            client._price_callbacks.append(callback)

        client.set_price_callback = MagicMock(side_effect=mock_set_price_callback)

        return client

    @pytest.mark.asyncio
    async def test_dynamic_hedging_flow(self, test_portfolio, deribit_client, tmp_path):
        """Test the complete dynamic hedging flow with live updates."""
        # Ensure we have a proper portfolio object
        if asyncio.iscoroutine(test_portfolio):
            portfolio = await test_portfolio
        else:
            portfolio = test_portfolio

        # Use the same portfolio manager that was used to create the portfolio
        portfolio_manager = self._portfolio_manager

        # Ensure the portfolio is saved and loaded
        await portfolio_manager._save_portfolio(portfolio.id, portfolio)

        # Create a subscription manager
        subscription_manager = SubscriptionManager(deribit_client=deribit_client)

        # Create a hedging manager
        hedging_manager = HedgingManager(
            portfolio_manager=portfolio_manager,
            subscription_manager=subscription_manager,
            deribit_client=deribit_client,
            default_hedger_config=TEST_CONFIG
        )

        try:
            # Start the hedging manager
            await hedging_manager.start()

            # Add the test portfolio to the hedging manager
            await hedging_manager.start_hedging(
                portfolio_id=portfolio.id
            )

            # Verify the hedger was created
            assert portfolio.id in hedging_manager.hedgers
            hedger_info = hedging_manager.hedgers[portfolio.id]
            hedger = hedger_info.hedger
            assert isinstance(hedger, DynamicDeltaHedger)

            # Simulate initial price update (BTC at $60,000)
            initial_price = 60000.0

            # Get the price callback that was registered with the client
            price_callback = deribit_client.set_price_callback.call_args[0][0]

            # Call the price callback with the update
            if asyncio.iscoroutinefunction(price_callback):
                await price_callback("BTC-PERPETUAL", initial_price)
            else:
                price_callback("BTC-PERPETUAL", initial_price)

            # Wait for the initial price to be processed
            await asyncio.sleep(0.5)

            # Verify the initial price was received
            assert hedger.current_price == initial_price

            # Simulate price increase (5% move to $63,000)
            new_price = 63000.0

            # Call all registered price callbacks with the update
            for callback in deribit_client._price_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback("BTC-PERPETUAL", new_price)
                else:
                    callback("BTC-PERPETUAL", new_price)

            # Wait for the price update to be processed and delta to be calculated
            max_attempts = 10
            for attempt in range(max_attempts):
                if hedger.cur_delta is not None:
                    break
                await asyncio.sleep(0.5)
                logger.info(f"Waiting for delta calculation (attempt {attempt + 1}/{max_attempts})...")
            else:
                logger.error("Timed out waiting for delta calculation")

            # Verify the price update was received
            assert abs(hedger.current_price - new_price) < 1e-6  # Allow for floating point precision

            # Verify the delta was calculated
            assert hedger.cur_delta is not None, "Delta was not calculated after price update"
            logger.info(f"Delta calculated successfully: {hedger.cur_delta}")

            # Verify the price was updated
            assert hedger.current_price == new_price

            # Simulate another price update (back to $60,000) using the price callback
            for callback in deribit_client._price_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback("BTC-PERPETUAL", initial_price)
                else:
                    callback("BTC-PERPETUAL", initial_price)

            # Wait for the price update to be processed
            await asyncio.sleep(0.5)


            # Verify the price was updated again
            assert hedger.current_price == initial_price

            # Check that the hedging manager is running and processing updates
            assert hedging_manager._monitor_task is not None and not hedging_manager._monitor_task.done()

        finally:
            # Stop the hedging manager
            await hedging_manager.stop()

            # Cleanup
            await portfolio_manager.close()

            # Verify the hedging manager was stopped
            assert hedging_manager._monitor_task is None or hedging_manager._monitor_task.done()
