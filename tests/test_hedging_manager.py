"""
Tests for the HedgingManager class.
"""
import pytest
import pytest_asyncio
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

from dneutral_sniper.hedging_manager import HedgingManager, HedgerInfo
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager
from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.deribit_client import DeribitWebsocketClient

class TestHedgingManager:
    """Test cases for HedgingManager."""

    @pytest_asyncio.fixture
    async def portfolio_manager(self, tmp_path):
        """Create a PortfolioManager instance for testing with a test portfolio."""
        # Create a temporary directory for portfolio files
        portfolios_dir = tmp_path / "portfolios"

        # Create and initialize the PortfolioManager
        pm = PortfolioManager(data_dir=portfolios_dir)
        await pm.initialize()

        # Create a test portfolio using the PortfolioManager
        portfolio_id = "test_portfolio"
        underlying = "BTC"
        initial_balance = 10000.0

        # Create portfolio using the manager's method
        await pm.create_portfolio(portfolio_id, underlying, initial_balance)

        # Get the created portfolio to set additional attributes if needed
        portfolio = await pm.get_portfolio(portfolio_id)
        # Set any additional attributes needed for testing
        portfolio.initial_balance = initial_balance

        # Save the portfolio to persist any changes
        await pm.save_portfolio(portfolio_id)

        return pm

    @pytest_asyncio.fixture
    async def subscription_manager(self):
        """Create a SubscriptionManager instance for testing."""
        return SubscriptionManager()

    @pytest.fixture
    def deribit_client(self):
        """Create a mock Deribit client."""
        client = AsyncMock(spec=DeribitWebsocketClient)
        client.set_price_callback = MagicMock()
        return client

    @pytest_asyncio.fixture
    async def hedging_manager(self, portfolio_manager, subscription_manager, deribit_client):
        """Create a HedgingManager instance for testing."""
        # Create a function to wrap the DynamicDeltaHedger constructor
        original_init = DynamicDeltaHedger.__init__

        def patched_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            # Set test mode to prevent hanging in tests
            self._test_mode = True

        # Apply the patch
        with patch('dneutral_sniper.hedging_manager.DynamicDeltaHedger.__init__', patched_init):
            manager = HedgingManager(
                portfolio_manager=portfolio_manager,
                subscription_manager=subscription_manager,
                deribit_client=deribit_client,
                default_hedger_config={
                    'ddh_min_trigger_delta': 0.01,
                    'ddh_target_delta': 0.0,
                    'ddh_step_mode': 'percentage',
                    'ddh_step_size': 0.01,
                }
            )
            # Initialize _price_handlers if it doesn't exist
            if not hasattr(manager, '_price_handlers'):
                manager._price_handlers = {}
            return manager

    @pytest.mark.asyncio
    async def test_start_stop(self, hedging_manager):
        """Test starting and stopping the hedging manager."""
        try:
            # Start the manager
            await hedging_manager.start()
            assert hedging_manager._monitor_task is not None
            assert not hedging_manager._shutdown_event.is_set()
            assert not hedging_manager._monitor_task.done()

            # Stop the manager
            await hedging_manager.stop()
            assert hedging_manager._monitor_task is None
            assert hedging_manager._shutdown_event.is_set()
        except Exception as e:
            # Ensure cleanup in case of test failure
            if hasattr(hedging_manager, '_monitor_task') and hedging_manager._monitor_task:
                hedging_manager._monitor_task.cancel()
                try:
                    await asyncio.wait_for(hedging_manager._monitor_task, timeout=0.1)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            raise

    @pytest.mark.asyncio
    async def test_add_remove_hedger(self, hedging_manager, portfolio_manager):
        """Test adding and removing a hedger."""
        portfolio_id = "test_portfolio"

        # Create a mock portfolio
        mock_portfolio = AsyncMock()
        mock_portfolio.underlying = 'BTC'
        mock_portfolio.options = {}

        # Create a mock subscription manager
        mock_subscription_manager = AsyncMock()
        hedging_manager.subscription_manager = mock_subscription_manager

        # Create a mock hedger
        mock_hedger = AsyncMock()
        mock_hedger.start = AsyncMock(return_value=None)
        mock_hedger.stop = AsyncMock(return_value=None)

        # Patch the portfolio manager to return our mock portfolio
        with (
            patch.object(portfolio_manager, 'get_portfolio', return_value=mock_portfolio) as mock_get_portfolio,
            patch('dneutral_sniper.hedging_manager.DynamicDeltaHedger', return_value=mock_hedger) as mock_hedger_cls
        ):
            try:
                print("1. Starting test: Adding hedger...")
                # Add a hedger with timeout
                try:
                    await asyncio.wait_for(
                        hedging_manager._add_hedger(portfolio_id),
                        timeout=5.0
                    )
                    print("2. Successfully added hedger")
                except asyncio.TimeoutError:
                    pytest.fail("Timeout while adding hedger")

                # Verify the hedger was created and start was called
                mock_hedger_cls.assert_called_once()
                mock_hedger.start.assert_called_once()
                print("3. Verified hedger creation and start")

                # Verify the hedger is in the manager
                assert portfolio_id in hedging_manager.hedgers
                assert hedging_manager.hedgers[portfolio_id].hedger is mock_hedger
                print("4. Verified hedger in manager")

                # Get the monitoring task
                task = hedging_manager.hedgers[portfolio_id].task
                assert task is not None
                assert not task.done()
                print("5. Verified monitoring task")

                # Test adding the same hedger again (should be idempotent)
                print("6. Testing idempotent add...")
                initial_task = hedging_manager.hedgers[portfolio_id].task
                await asyncio.wait_for(
                    hedging_manager._add_hedger(portfolio_id),
                    timeout=5.0
                )
                assert hedging_manager.hedgers[portfolio_id].task is initial_task  # Same task, not replaced
                mock_hedger.start.assert_called_once()  # Still only called once
                print("7. Verified idempotent add")

                # Remove the hedger
                print("8. Removing hedger...")
                try:
                    await asyncio.wait_for(
                        hedging_manager._remove_hedger(portfolio_id),
                        timeout=5.0
                    )
                    print("9. Successfully removed hedger")
                except asyncio.TimeoutError:
                    pytest.fail("Timeout while removing hedger")
                
                assert portfolio_id not in hedging_manager.hedgers
                mock_hedger.stop.assert_called_once()
                print("10. Verified hedger removal")

                # Test removing non-existent hedger (should not raise)
                print("11. Testing removal of non-existent hedger...")
                await hedging_manager._remove_hedger("non_existent_portfolio")
                print("12. Successfully handled non-existent hedger removal")

            except Exception as e:
                print(f"Test failed with error: {str(e)}")
                raise
            finally:
                # Ensure cleanup in case of test failure
                if hasattr(hedging_manager, 'hedgers') and portfolio_id in hedging_manager.hedgers:
                    print("Cleaning up test...")
                    try:
                        await asyncio.wait_for(
                            hedging_manager._remove_hedger(portfolio_id),
                            timeout=5.0
                        )
                    except (asyncio.TimeoutError, Exception) as e:
                        print(f"Cleanup warning: {str(e)}")

    @pytest.mark.asyncio
    async def test_monitor_portfolios(self, hedging_manager, portfolio_manager):
        """Test monitoring for portfolio changes."""
        # Create a test portfolio
        portfolio_id = "test_portfolio"
        mock_portfolio = AsyncMock()
        mock_portfolio.underlying = 'BTC'
        mock_portfolio.options = {}

        # Mock the subscription manager
        mock_subscription_manager = AsyncMock()
        hedging_manager.subscription_manager = mock_subscription_manager

        # Mock the portfolio manager to return our test portfolio
        with patch.object(portfolio_manager, 'get_portfolio', return_value=mock_portfolio), \
             patch.object(portfolio_manager, 'list_portfolios', return_value=[portfolio_id]), \
             patch.object(hedging_manager, '_add_hedger') as mock_add_hedger, \
             patch.object(hedging_manager, '_remove_hedger') as mock_remove_hedger:

            # Create a task that will be cancelled after a timeout
            monitor_task = asyncio.create_task(hedging_manager._monitor_portfolios())

            # Wait for the monitor to run at least once
            await asyncio.sleep(0.1)

            # Verify the monitor tried to add the hedger
            mock_add_hedger.assert_called_once_with(portfolio_id)

            # Cancel the monitor task
            monitor_task.cancel()
            try:
                await asyncio.wait_for(monitor_task, timeout=0.1)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

    @pytest.mark.asyncio
    async def test_get_hedger(self, hedging_manager):
        """Test getting a hedger by portfolio ID."""
        portfolio_id = "test_portfolio"

        # Add a hedger
        await hedging_manager._add_hedger(portfolio_id)

        # Get the hedger
        hedger = await hedging_manager.get_hedger(portfolio_id)
        assert hedger is not None
        assert isinstance(hedger, DynamicDeltaHedger)

    @pytest.mark.asyncio
    async def test_get_hedger_stats(self, hedging_manager):
        """Test getting statistics for a hedger."""
        portfolio_id = "test_portfolio"

        # Add a hedger
        await hedging_manager._add_hedger(portfolio_id)

        # Get stats
        stats = await hedging_manager.get_hedger_stats(portfolio_id)
        assert stats is not None
        assert stats['portfolio_id'] == portfolio_id
        assert 'current_delta' in stats
        assert 'hedge_count' in stats

        # Test getting stats for non-existent portfolio
        stats = await hedging_manager.get_hedger_stats("non_existent_portfolio")
        assert stats is None

    # Test removed - covered in integration tests

    @pytest.mark.asyncio
    async def test_get_all_hedger_stats(self, hedging_manager, portfolio_manager):
        """Test getting statistics for all hedgers."""
        # Create test portfolios first
        portfolio_ids = ["test_portfolio1", "test_portfolio2"]
        underlying = "BTC"
        initial_balance = 10000.0

        for pid in portfolio_ids:
            await portfolio_manager.create_portfolio(pid, underlying, initial_balance)

        # Add hedgers for each portfolio
        for pid in portfolio_ids:
            await hedging_manager._add_hedger(pid)

        # Get all stats
        all_stats = await hedging_manager.get_all_hedger_stats()

        # Check that we have stats for all portfolios
        assert set(all_stats.keys()) == set(portfolio_ids)
        for pid in portfolio_ids:
            assert pid in all_stats
            assert 'current_delta' in all_stats[pid]
            assert 'hedge_count' in all_stats[pid]

    @pytest.mark.asyncio
    async def test_get_hedger_nonexistent(self, hedging_manager):
        """Test getting a non-existent hedger."""
        # Test with non-existent portfolio
        assert await hedging_manager.get_hedger("nonexistent") is None
