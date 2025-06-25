"""
Tests for the PortfolioService with PortfolioManager integration.
"""

import pytest
import os
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException

from web_api.services.portfolio_service_v2 import PortfolioService
from web_api.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    OptionPositionCreate
)

# Test data
TEST_DATA_DIR = "test_data/portfolios"

# Fixtures
@pytest.fixture
def portfolio_service():
    """Create a clean PortfolioService instance for each test."""
    # Ensure clean test directory
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)

    service = PortfolioService(data_dir=TEST_DATA_DIR)
    yield service

    # Clean up after test
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)

# Test cases
class TestPortfolioService:
    """Test cases for PortfolioService with PortfolioManager integration."""

    @pytest.mark.asyncio
    async def test_create_and_get_portfolio(self, portfolio_service):
        """Test creating and retrieving a portfolio."""
        # Create a portfolio
        create_data = PortfolioCreate(
            name="Test Portfolio",
            description="Test description"
        )
        created = await portfolio_service.create_portfolio(create_data)

        # Verify the portfolio was created
        assert created["name"] == "Test Portfolio"
        assert created["description"] == "Test description"
        assert "id" in created

        # Retrieve the portfolio
        retrieved = await portfolio_service.get_portfolio(created["id"])
        assert retrieved == created

    @pytest.mark.asyncio
    async def test_list_portfolios(self, portfolio_service):
        """Test listing multiple portfolios."""
        # Create test portfolios
        for i in range(3):
            await portfolio_service.create_portfolio(
                PortfolioCreate(name=f"Portfolio {i}")
            )

        # List portfolios
        portfolios = await portfolio_service.list_portfolios()
        assert len(portfolios) == 3
        assert all("Portfolio " in p["name"] for p in portfolios)

    @pytest.mark.asyncio
    async def test_update_portfolio(self, portfolio_service):
        """Test updating a portfolio."""
        # Create a portfolio
        portfolio = await portfolio_service.create_portfolio(
            PortfolioCreate(name="Original Name")
        )

        # Update the portfolio
        updated = await portfolio_service.update_portfolio(
            portfolio["id"],
            PortfolioUpdate(name="Updated Name", description="New description")
        )

        # Verify the update
        assert updated["name"] == "Updated Name"
        assert updated["description"] == "New description"

        # Verify the update is persisted
        retrieved = await portfolio_service.get_portfolio(portfolio["id"])
        assert retrieved["name"] == "Updated Name"

    @pytest.mark.asyncio
    async def test_delete_portfolio(self, portfolio_service):
        """Test deleting a portfolio."""
        # Create a portfolio
        portfolio = await portfolio_service.create_portfolio(
            PortfolioCreate(name="To be deleted")
        )

        # Delete the portfolio
        await portfolio_service.delete_portfolio(portfolio["id"])

        # Verify it's gone
        with pytest.raises(HTTPException) as exc_info:
            await portfolio_service.get_portfolio(portfolio["id"])
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_add_option_to_portfolio(self, portfolio_service):
        """Test adding an option to a portfolio."""
        # Create a portfolio
        portfolio = await portfolio_service.create_portfolio(
            PortfolioCreate(name="Options Test")
        )

        # Add an option
        option_data = OptionPositionCreate(
            symbol="BTC-30JUN23-30000-C",
            option_type="call",
            strike=30000,
            expiration=datetime.now(timezone.utc) + timedelta(days=30),
            quantity=1.0,
            premium=0.05,
            underlying="BTC"
        )
        option = await portfolio_service.add_option(portfolio["id"], option_data)

        # Verify the option was added
        assert option["symbol"] == "BTC-30JUN23-30000-C"
        assert option["option_type"] == "call"

        # Verify it appears in the portfolio
        portfolio = await portfolio_service.get_portfolio(portfolio["id"])
        assert len(portfolio["options"]) == 1
        assert portfolio["options"][0]["id"] == option["id"]

    @pytest.mark.asyncio
    async def test_remove_option_from_portfolio(self, portfolio_service):
        """Test removing an option from a portfolio."""
        # Create a portfolio with an option
        portfolio = await portfolio_service.create_portfolio(
            PortfolioCreate(name="Options Test")
        )

        option_data = OptionPositionCreate(
            symbol="BTC-30JUN23-30000-C",
            option_type="call",
            strike=30000,
            expiration=datetime.now(timezone.utc) + timedelta(days=30),
            quantity=1.0,
            premium=0.05,
            underlying="BTC"
        )
        option = await portfolio_service.add_option(portfolio["id"], option_data)

        # Remove the option
        await portfolio_service.remove_option(portfolio["id"], option["id"])

        # Verify it's gone
        portfolio = await portfolio_service.get_portfolio(portfolio["id"])
        assert len(portfolio["options"]) == 0

    @pytest.mark.asyncio
    async def test_update_option_in_portfolio(self, portfolio_service):
        """Test updating an option in a portfolio."""
        # Create a portfolio with an option
        portfolio = await portfolio_service.create_portfolio(
            PortfolioCreate(name="Options Test")
        )

        option_data = OptionPositionCreate(
            symbol="BTC-30JUN23-30000-C",
            option_type="call",
            strike=30000,
            expiration=datetime.now(timezone.utc) + timedelta(days=30),
            quantity=1.0,
            premium=0.05,
            underlying="BTC"
        )
        option = await portfolio_service.add_option(portfolio["id"], option_data)

        # Update the option
        updated_data = OptionPositionCreate(
            symbol="BTC-30JUN23-31000-C",  # Updated strike
            option_type="call",
            strike=31000,
            expiration=datetime.now(timezone.utc) + timedelta(days=30),
            quantity=2.0,  # Updated quantity
            premium=0.06,  # Updated premium
            underlying="BTC"
        )

        # Since we don't have a direct update, we'll use remove and add
        await portfolio_service.remove_option(portfolio["id"], option["id"])
        updated_option = await portfolio_service.add_option(portfolio["id"], updated_data)

        # Verify the update
        assert updated_option["symbol"] == "BTC-30JUN23-31000-C"
        assert updated_option["strike"] == 31000.0
        assert updated_option["quantity"] == 2.0

        # Verify it's the only option in the portfolio
        portfolio = await portfolio_service.get_portfolio(portfolio["id"])
        assert len(portfolio["options"]) == 1
        assert portfolio["options"][0]["id"] == updated_option["id"]

    @pytest.mark.asyncio
    async def test_nonexistent_portfolio(self, portfolio_service):
        """Test operations on a non-existent portfolio."""
        with pytest.raises(HTTPException) as exc_info:
            await portfolio_service.get_portfolio("nonexistent")
        assert exc_info.value.status_code == 404

        with pytest.raises(HTTPException) as exc_info:
            await portfolio_service.update_portfolio(
                "nonexistent", PortfolioUpdate(name="New Name")
            )
        assert exc_info.value.status_code == 404

        with pytest.raises(HTTPException) as exc_info:
            await portfolio_service.delete_portfolio("nonexistent")
        assert exc_info.value.status_code == 404
