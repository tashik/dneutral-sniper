"""
Tests for the SubscriptionManager class.
"""
import pytest
import asyncio
import pytest_asyncio
from dneutral_sniper.subscription_manager import SubscriptionManager

class TestSubscriptionManager:
    """Test cases for SubscriptionManager."""
    
    @pytest_asyncio.fixture
    async def subscription_manager(self):
        """Create a SubscriptionManager instance for testing."""
        return SubscriptionManager()

    @pytest.mark.asyncio
    async def test_add_remove_subscription(self, subscription_manager):
        """Test adding and removing subscriptions."""
        # Add subscription
        added = await subscription_manager.add_subscription("portfolio1", "BTC-PERPETUAL")
        assert added is True

        # Add same subscription again (should be idempotent)
        added_again = await subscription_manager.add_subscription("portfolio1", "BTC-PERPETUAL")
        assert added_again is False

        # Verify subscription exists
        portfolios = await subscription_manager.get_subscribed_portfolios("BTC-PERPETUAL")
        assert "portfolio1" in portfolios

        # Remove subscription
        removed = await subscription_manager.remove_subscription("portfolio1", "BTC-PERPETUAL")
        assert removed is True

        # Verify subscription is removed
        portfolios = await subscription_manager.get_subscribed_portfolios("BTC-PERPETUAL")
        assert "portfolio1" not in portfolios

        # Remove non-existent subscription
        removed_again = await subscription_manager.remove_subscription("portfolio1", "BTC-PERPETUAL")
        assert removed_again is False

    @pytest.mark.asyncio
    async def test_get_portfolio_subscriptions(self, subscription_manager):
        """Test getting all subscriptions for a portfolio."""
        # Add multiple subscriptions
        await subscription_manager.add_subscription("portfolio1", "BTC-PERPETUAL")
        await subscription_manager.add_subscription("portfolio1", "BTC-31DEC25-100000-C")
        await subscription_manager.add_subscription("portfolio2", "BTC-PERPETUAL")

        # Test getting subscriptions for portfolio1
        portfolio1_subs = await subscription_manager.get_portfolio_subscriptions("portfolio1")
        assert len(portfolio1_subs) == 2
        assert "BTC-PERPETUAL" in portfolio1_subs
        assert "BTC-31DEC25-100000-C" in portfolio1_subs

        # Test non-existent portfolio
        empty_subs = await subscription_manager.get_portfolio_subscriptions("nonexistent")
        assert len(empty_subs) == 0

    @pytest.mark.asyncio
    async def test_update_portfolio_subscriptions(self, subscription_manager):
        """Test updating all subscriptions for a portfolio at once."""
        # Initial subscriptions
        await subscription_manager.add_subscription("portfolio1", "BTC-PERPETUAL")
        await subscription_manager.add_subscription("portfolio1", "BTC-31DEC25-100000-C")

        # Update subscriptions
        new_instruments = {"BTC-PERPETUAL", "BTC-31DEC25-90000-P"}  # Removed 100000-C, added 90000-P
        await subscription_manager.update_portfolio_subscriptions("portfolio1", new_instruments)

        # Verify updates
        current_subs = await subscription_manager.get_portfolio_subscriptions("portfolio1")
        assert current_subs == new_instruments

        # Verify other portfolios not affected
        portfolios = await subscription_manager.get_subscribed_portfolios("BTC-31DEC25-100000-C")
        assert "portfolio1" not in portfolios

    @pytest.mark.asyncio
    async def test_clear_portfolio_subscriptions(self, subscription_manager):
        """Test clearing all subscriptions for a portfolio."""
        # Add some subscriptions
        await subscription_manager.add_subscription("portfolio1", "BTC-PERPETUAL")
        await subscription_manager.add_subscription("portfolio1", "BTC-31DEC25-100000-C")
        await subscription_manager.add_subscription("portfolio2", "BTC-PERPETUAL")

        # Clear portfolio1's subscriptions
        await subscription_manager.clear_portfolio_subscriptions("portfolio1")

        # Verify portfolio1 has no subscriptions
        portfolio1_subs = await subscription_manager.get_portfolio_subscriptions("portfolio1")
        assert len(portfolio1_subs) == 0

        # Verify portfolio2's subscriptions are unchanged
        portfolio2_subs = await subscription_manager.get_portfolio_subscriptions("portfolio2")
        assert "BTC-PERPETUAL" in portfolio2_subs

        # Verify instrument subscriptions were cleaned up
        btc_perp_subscribers = await subscription_manager.get_subscribed_portfolios("BTC-PERPETUAL")
        assert "portfolio1" not in btc_perp_subscribers
        assert "portfolio2" in btc_perp_subscribers

    @pytest.mark.asyncio
    async def test_concurrent_updates(self, subscription_manager):
        """Test that concurrent updates don't cause race conditions."""
        num_portfolios = 10
        num_instruments = 10

        async def update_subscriptions(portfolio_id):
            for i in range(num_instruments):
                instrument = f"INSTR-{i}"
                await subscription_manager.add_subscription(portfolio_id, instrument)
                await asyncio.sleep(0)  # Yield control to allow context switches

        # Create and run concurrent updates
        tasks = []
        for i in range(num_portfolios):
            task = asyncio.create_task(update_subscriptions(f"portfolio-{i}"))
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Verify all subscriptions were created
        for i in range(num_portfolios):
            portfolio_id = f"portfolio-{i}"
            subs = await subscription_manager.get_portfolio_subscriptions(portfolio_id)
            assert len(subs) == num_instruments

        for i in range(num_instruments):
            instrument = f"INSTR-{i}"
            portfolios = await subscription_manager.get_subscribed_portfolios(instrument)
            assert len(portfolios) == num_portfolios
