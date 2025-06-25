"""Tests for portfolio event system and persistence."""

import asyncio
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from dneutral_sniper.portfolio import (
    Portfolio,
    PortfolioEvent,
    PortfolioEventType,
    VanillaOption,
)
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.models import OptionType, ContractType

# Enable async test support
pytestmark = pytest.mark.asyncio

# Test fixtures

@pytest.fixture
def temp_portfolios_dir():
    """Create a temporary directory for testing portfolio files."""
    temp_dir = tempfile.mkdtemp(prefix="test_portfolios_")
    yield temp_dir
    # Cleanup handled by OS

@pytest_asyncio.fixture
async def portfolio_manager(temp_portfolios_dir):
    """Create a PortfolioManager with a temporary directory."""
    manager = PortfolioManager(portfolios_dir=temp_portfolios_dir)
    await manager.initialize()
    try:
        yield manager
    finally:
        # Cancel any pending save tasks
        if hasattr(manager, '_save_tasks') and manager._save_tasks:
            # Create a list of tasks to avoid modifying the dictionary during iteration
            tasks = list(manager._save_tasks.values())
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.warning(f"Error cancelling save task: {e}")
        # Ensure all portfolios are properly closed
        for portfolio_id in list(manager.portfolios.keys()):
            portfolio = manager.portfolios[portfolio_id]
            if hasattr(portfolio, 'close'):
                await portfolio.close()

@pytest_asyncio.fixture
async def test_portfolio(portfolio_manager):
    """Create a test portfolio with event tracking."""
    portfolio_id = "test_events"
    underlying = "BTC"
    initial_balance = 10000.0

    # Create a new portfolio
    new_id, portfolio = await portfolio_manager.create_portfolio(
        portfolio_id=portfolio_id,
        underlying=underlying,
        initial_balance=initial_balance
    )

    # Add a mock event handler
    mock_handler = AsyncMock()
    await portfolio.add_listener(PortfolioEventType.STATE_CHANGED, mock_handler)

    return portfolio, mock_handler, portfolio_manager

# Test cases

class TestPortfolioEventSystem:
    """Test the portfolio event system and persistence."""

    async def test_portfolio_emits_events_on_changes(self, test_portfolio):
        """Test that the portfolio emits events when its state changes."""
        portfolio, mock_handler, _ = test_portfolio

        # Clear any initial calls
        mock_handler.reset_mock()

        # Add a dedicated mock for FUTURES_POSITION_UPDATED
        position_handler = AsyncMock()
        await portfolio.add_listener(PortfolioEventType.FUTURES_POSITION_UPDATED, position_handler)

        # Modify the portfolio
        portfolio.futures_position = 1.0

        # Allow any pending events to be processed
        await asyncio.sleep(0.1)

        # Check that the futures position event was emitted
        position_handler.assert_called_once()
        event = position_handler.call_args[0][0]

        assert event.event_type == PortfolioEventType.FUTURES_POSITION_UPDATED
        assert event.portfolio is portfolio
        assert event.data['old_value'] == 0.0
        assert event.data['new_value'] == 1.0
        assert event.data['delta'] == 1.0

        # Check that the dirty state was marked
        assert portfolio.is_dirty(), "Portfolio should be marked as dirty"

    async def test_portfolio_sets_dirty_flag_on_change(self, test_portfolio):
        """Test that the portfolio sets the dirty flag when modified."""
        portfolio, _, _ = test_portfolio

        # Ensure clean state
        portfolio.mark_clean()
        assert not portfolio.is_dirty()

        # Create an event to wait for
        event_future = asyncio.Future()

        async def on_state_changed(event: PortfolioEvent):
            if not event_future.done():
                event_future.set_result(True)

        # Add a listener for state changes
        await portfolio.add_listener(PortfolioEventType.STATE_CHANGED, on_state_changed)

        # Modify the portfolio
        portfolio.futures_position = 1.0

        # Wait for the state change event
        try:
            await asyncio.wait_for(event_future, timeout=1.0)
            assert portfolio.is_dirty(), "Portfolio should be marked as dirty"
        except asyncio.TimeoutError:
            assert False, "Timed out waiting for state change event"

        # Mark as clean
        portfolio.mark_clean()
        assert not portfolio.is_dirty()

    async def test_portfolio_manager_debounced_save(self, test_portfolio, temp_portfolios_dir):
        """Test that the portfolio manager debounces save operations."""
        portfolio, _, manager = test_portfolio
        portfolio_id = portfolio.id

        # Mock the save_portfolio method
        with patch.object(manager, '_save_portfolio', new_callable=AsyncMock) as mock_save:
            # Make multiple rapid changes
            portfolio.futures_position = 1.0
            portfolio.futures_position = 2.0
            portfolio.futures_position = 3.0

            # Wait for debounce time (1 second) plus a small buffer
            await asyncio.sleep(1.5)

            # Should only call save_portfolio once
            assert mock_save.call_count == 1, "Expected exactly one call to _save_portfolio"

            # The saved value should be the last one set
            saved_portfolio = mock_save.call_args[0][1]
            assert saved_portfolio.futures_position == 3.0, "Unexpected futures position value"

    async def test_portfolio_persistence_roundtrip(self, test_portfolio, temp_portfolios_dir):
        """Test that a portfolio can be saved and loaded with all attributes."""
        portfolio, _, manager = test_portfolio
        portfolio_id = portfolio.id

        # Set various attributes
        portfolio.futures_position = 1.0
        portfolio.futures_avg_entry = 50000.0
        portfolio.initial_balance = 10000.0

        # Add an option
        option = VanillaOption(
            instrument_name="BTC-31DEC25-100000-C",
            quantity=1.0,
            strike=100000.0,
            expiry=datetime(2025, 12, 31, tzinfo=timezone.utc),
            option_type=OptionType.CALL,
            underlying="BTC",
            contract_type=ContractType.INVERSE,
            mark_price=0.05,
            iv=0.8,
            usd_value=5000.0,
            delta=0.5
        )
        await portfolio.add_option(option)

        # Save the portfolio
        await manager._save_portfolio(portfolio_id, portfolio)

        # Clear the in-memory portfolio
        manager.portfolios.pop(portfolio_id, None)

        # Load the portfolio
        await manager.load_all_portfolios()
        loaded_portfolio = await manager.get_portfolio(portfolio_id)

        # Check that all attributes were preserved
        assert loaded_portfolio is not None
        assert loaded_portfolio.futures_position == 1.0
        assert loaded_portfolio.futures_avg_entry == 50000.0
        assert loaded_portfolio.initial_balance == 10000.0

        # Check the option was preserved
        assert "BTC-31DEC25-100000-C" in loaded_portfolio.options
        loaded_option = loaded_portfolio.options["BTC-31DEC25-100000-C"]
        assert loaded_option.quantity == 1.0
        assert loaded_option.strike == 100000.0
        assert loaded_option.option_type == OptionType.CALL
        assert loaded_option.contract_type == ContractType.INVERSE

    async def test_event_emission_on_option_operations(self, test_portfolio, caplog):
        """Test that events are emitted for option operations."""
        portfolio, _, _ = test_portfolio

        # Enable debug logging for this test
        import logging
        logger = logging.getLogger('dneutral_sniper.portfolio')
        logger.setLevel(logging.DEBUG)

        # Track all events for debugging
        received_events = []

        def event_collector(event):
            received_events.append(event.event_type)
            logger.debug(f"Event received: {event.event_type}")
            return None

        # Setup dedicated mocks for each event type
        added_handler = AsyncMock(side_effect=lambda e: event_collector(e) or None)
        updated_handler = AsyncMock(side_effect=lambda e: event_collector(e) or None)
        removed_handler = AsyncMock(side_effect=lambda e: event_collector(e) or None)

        # Add listeners for each event type
        logger.debug("Adding event listeners...")
        await asyncio.gather(
            portfolio.add_listener(PortfolioEventType.OPTION_ADDED, added_handler),
            portfolio.add_listener(PortfolioEventType.OPTION_UPDATED, updated_handler),
            portfolio.add_listener(PortfolioEventType.OPTION_REMOVED, removed_handler)
        )
        logger.debug("Event listeners added")

        # Add an option
        option = VanillaOption(
            instrument_name="BTC-31DEC25-100000-C",
            quantity=1.0,
            strike=100000.0,
            expiry=datetime(2025, 12, 31, tzinfo=timezone.utc),
            option_type=OptionType.CALL,
            underlying="BTC",
            contract_type=ContractType.INVERSE
        )

        # Add the option and wait for the event
        logger.debug("Adding option...")
        await portfolio.add_option(option)
        logger.debug("Option added, waiting for events...")
        await asyncio.sleep(0.1)

        # Debug: Log all received events
        logger.debug(f"All received events: {received_events}")

        # Check that the OPTION_ADDED event was emitted
        try:
            added_handler.assert_called_once()
            added_event = added_handler.call_args[0][0]
            assert added_event.event_type == PortfolioEventType.OPTION_ADDED
            assert added_event.data["instrument_name"] == "BTC-31DEC25-100000-C"
        except AssertionError as e:
            logger.error(f"Assertion failed: {e}")
            logger.error(f"Added handler call count: {added_handler.call_count}")
            raise

        # Update the option
        added_handler.reset_mock()
        option.quantity = 2.0

        # Trigger an update by adding the option again with a different quantity
        logger.debug("Updating option...")
        option.quantity = 2.0
        await portfolio.add_option(option)
        logger.debug("Option updated, waiting for events...")
        await asyncio.sleep(0.1)

        # Debug: Log all received events
        logger.debug(f"All received events after update: {received_events}")

        # Check that the OPTION_UPDATED event was emitted
        try:
            updated_handler.assert_called_once()
            updated_event = updated_handler.call_args[0][0]
            assert updated_event.event_type == PortfolioEventType.OPTION_UPDATED
            assert updated_event.data["instrument_name"] == "BTC-31DEC25-100000-C"
        except AssertionError as e:
            logger.error(f"Update assertion failed: {e}")
            logger.error(f"Updated handler call count: {updated_handler.call_count}")
            raise

        # Remove the option
        logger.debug("Removing option...")
        updated_handler.reset_mock()
        await portfolio.remove_option("BTC-31DEC25-100000-C")
        logger.debug("Option removed, waiting for events...")
        await asyncio.sleep(0.1)

        # Debug: Log all received events
        logger.debug(f"All received events after removal: {received_events}")

        # Check that the OPTION_REMOVED event was emitted
        try:
            removed_handler.assert_called_once()
            removed_event = removed_handler.call_args[0][0]
            assert removed_event.event_type == PortfolioEventType.OPTION_REMOVED
            assert removed_event.data["instrument_name"] == "BTC-31DEC25-100000-C"
        except AssertionError as e:
            logger.error(f"Removal assertion failed: {e}")
            logger.error(f"Removed handler call count: {removed_handler.call_count}")
            raise
