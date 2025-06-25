#!/usr/bin/env python3
"""
Test script to verify HedgingManager subscription and price update functionality.
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.subscription_manager import SubscriptionManager
from dneutral_sniper.hedging_manager import HedgingManager
from dneutral_sniper.dynamic_delta_hedger import HedgerConfig
from dneutral_sniper.models import VanillaOption, OptionType, ContractType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
        # logging.FileHandler('hedging_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
TESTNET = False
UNDERLYING = 'BTC'
TEST_PORTFOLIO_ID = 'test_hedging_portfolio'
TEST_OPTION_INSTRUMENT = 'BTC-27JUN25-100000-C'  # Example option
TEST_PERPETUAL_INSTRUMENT = 'BTC-PERPETUAL'  # Perpetual futures

# Test credentials (replace with actual test credentials or use environment variables)
TEST_CREDENTIALS = DeribitCredentials(
    client_id=os.getenv('DERIBIT_TEST_CLIENT_ID', ''),
    client_secret=os.getenv('DERIBIT_TEST_CLIENT_SECRET', ''),
    test=True
)

class HedgingManagerTestHarness:
    """Test class for HedgingManager subscription and price update functionality."""

    def __init__(self):
        """Initialize the test harness."""
        self.deribit_client: Optional[DeribitWebsocketClient] = None
        self.subscription_manager: Optional[SubscriptionManager] = None
        self.portfolio_manager: Optional[PortfolioManager] = None
        self.hedging_manager: Optional[HedgingManager] = None
        self.test_portfolio: Optional[Portfolio] = None
        self.running = False

        # Track received price updates
        self.price_updates: Dict[str, float] = {}
        self.price_update_events: Dict[str, asyncio.Event] = {}

    async def setup(self):
        """Set up the test environment."""
        logger.info("Setting up test environment...")

        # Initialize Deribit client
        self.deribit_client = DeribitWebsocketClient(credentials=TEST_CREDENTIALS)

        # Initialize subscription manager
        self.subscription_manager = SubscriptionManager(deribit_client=self.deribit_client)

        # Initialize portfolio manager
        self.portfolio_manager = PortfolioManager(portfolios_dir='test_data')

        # Create a test portfolio
        portfolio_id, self.test_portfolio = await self.portfolio_manager.create_portfolio(
            portfolio_id=TEST_PORTFOLIO_ID,
            underlying=UNDERLYING,
            initial_balance=10000.0
        )

        # Store config separately since it's not part of the portfolio creation
        self.test_portfolio.config = {
            'underlying': UNDERLYING,
            'ddh_min_trigger_delta': 0.01,
            'ddh_target_delta': 0.0,
            'ddh_step_mode': 'absolute',
            'ddh_step_size': 100.0,
            'volatility': 0.8,
            'risk_free_rate': 0.0,
            'min_hedge_usd': 10.0,
            'price_check_interval': 1.0
        }

        # Add a test option to the portfolio
        expiry = datetime.now() + timedelta(days=30)
        option = VanillaOption(
            instrument_name=TEST_OPTION_INSTRUMENT,
            option_type=OptionType.CALL,
            strike=30000.0,
            expiry=expiry,
            quantity=1.0,
            underlying=UNDERLYING,
            contract_type=ContractType.INVERSE,
            mark_price=1500.0,
            iv=0.8,
            usd_value=1500.0,
            delta=0.5
        )

        # Add the option to the portfolio
        await self.test_portfolio.add_option(option)

        # Save the portfolio
        await self.portfolio_manager.save_portfolio(self.test_portfolio)

        # Initialize hedging manager
        self.hedging_manager = HedgingManager(
            portfolio_manager=self.portfolio_manager,
            subscription_manager=self.subscription_manager,
            deribit_client=self.deribit_client,
            default_hedger_config={
                'ddh_min_trigger_delta': 0.01,
                'ddh_target_delta': 0.0,
                'ddh_step_mode': 'absolute',
                'ddh_step_size': 100.0,
                'price_check_interval': 1.0,
                'underlying': UNDERLYING,
                'instrument_name': TEST_PERPETUAL_INSTRUMENT,
                'volatility': 0.8,
                'risk_free_rate': 0.0,
                'min_hedge_usd': 10.0
            }
        )

        # Register a price update handler for testing
        self.deribit_client.set_price_callback(self._on_price_update)

        logger.info("Test environment setup complete")

    async def _on_price_update(self, instrument: str, price: float) -> None:
        """Handle price updates for testing."""
        logger.info(f"Received price update - Instrument: {instrument}, Price: {price}")
        self.price_updates[instrument] = price

        # Set the event for this instrument if we're waiting for it
        if instrument in self.price_update_events:
            self.price_update_events[instrument].set()

    async def wait_for_price_update(self, instrument: str, timeout: float = 10.0) -> Optional[float]:
        """Wait for a price update for the given instrument."""
        if instrument not in self.price_update_events:
            self.price_update_events[instrument] = asyncio.Event()

        try:
            await asyncio.wait_for(self.price_update_events[instrument].wait(), timeout=timeout)
            return self.price_updates.get(instrument)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for price update for {instrument}")
            return None
        finally:
            self.price_update_events[instrument].clear()

    async def run_test(self):
        """Run the test with proper timeouts and cleanup."""
        logger.info("Starting test...")

        try:
            # Start the Deribit client with timeout
            logger.info("Connecting to Deribit...")
            try:
                await asyncio.wait_for(self.deribit_client.connect(), timeout=10.0)
                logger.info("Successfully connected to Deribit")
            except asyncio.TimeoutError:
                logger.error("Timed out connecting to Deribit")
                return False
            except Exception as e:
                logger.error(f"Failed to connect to Deribit: {e}")
                return False

            # Start the hedging manager with timeout
            logger.info("Starting hedging manager...")
            try:
                await asyncio.wait_for(self.hedging_manager.start(), timeout=10.0)
                logger.info("Hedging manager started successfully")
            except asyncio.TimeoutError:
                logger.error("Timed out starting hedging manager")
                return False
            except Exception as e:
                logger.error(f"Failed to start hedging manager: {e}")
                return False

            # Start hedging for the test portfolio with timeout
            logger.info(f"Starting hedging for portfolio {TEST_PORTFOLIO_ID}...")
            try:
                # Increase timeout to 30 seconds and add more detailed logging
                logger.info("Calling start_hedging...")
                start_time = time.time()

                # Call start_hedging with a 30 second timeout
                success = await asyncio.wait_for(
                    self.hedging_manager.start_hedging(TEST_PORTFOLIO_ID, timeout=30.0),
                    timeout=35.0  # Slightly longer than the internal timeout
                )

                elapsed = time.time() - start_time
                if success:
                    logger.info(f"Hedging started successfully in {elapsed:.2f} seconds")
                else:
                    logger.error(f"Failed to start hedging (took {elapsed:.2f} seconds)")
                    return False

            except asyncio.TimeoutError:
                elapsed = time.time() - start_time
                logger.error(f"Timed out after {elapsed:.2f} seconds while starting hedging")
                return False
            except Exception as e:
                logger.error(f"Failed to start hedging: {e}", exc_info=True)
                return False

            # Wait for price updates for both the option and perpetual
            logger.info("Waiting for price updates...")

            # Wait for the perpetual price update with timeout
            logger.info(f"Waiting for {TEST_PERPETUAL_INSTRUMENT} price update...")
            perpetual_price = await self.wait_for_price_update(TEST_PERPETUAL_INSTRUMENT, timeout=30.0)
            if perpetual_price is not None:
                logger.info(f"Successfully received {TEST_PERPETUAL_INSTRUMENT} price update: {perpetual_price}")
            else:
                logger.error(f"Failed to receive {TEST_PERPETUAL_INSTRUMENT} price update")
                return False

            # Wait for the option price update with timeout
            logger.info(f"Waiting for {TEST_OPTION_INSTRUMENT} price update...")
            option_price = await self.wait_for_price_update(TEST_OPTION_INSTRUMENT, timeout=30.0)
            if option_price is not None:
                logger.info(f"Successfully received {TEST_OPTION_INSTRUMENT} price update: {option_price}")
            else:
                logger.error(f"Failed to receive {TEST_OPTION_INSTRUMENT} price update")
                return False

            # Verify that the price cache was updated
            logger.info("Verifying price cache...")
            try:
                perpetual_price_cached, _ = self.deribit_client.get_price_iv(TEST_PERPETUAL_INSTRUMENT)
                option_price_cached, _ = self.deribit_client.get_price_iv(TEST_OPTION_INSTRUMENT)
                logger.info(f"Cached prices - {TEST_PERPETUAL_INSTRUMENT}: {perpetual_price_cached}, {TEST_OPTION_INSTRUMENT}: {option_price_cached}")
                return True
            except Exception as e:
                logger.error(f"Failed to get cached prices: {e}")
                return False

        except asyncio.CancelledError:
            logger.info("Test was cancelled")
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}", exc_info=True)
            return False
        finally:
            logger.info("Cleaning up test...")
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources with proper error handling."""
        logger.info("Starting cleanup...")

        # Stop the hedging manager if it exists
        if hasattr(self, 'hedging_manager') and self.hedging_manager:
            try:
                logger.info("Stopping hedging manager...")
                await asyncio.wait_for(self.hedging_manager.stop(), timeout=10.0)
                logger.info("Hedging manager stopped")
            except asyncio.TimeoutError:
                logger.warning("Timed out stopping hedging manager")
            except Exception as e:
                logger.error(f"Error stopping hedging manager: {e}", exc_info=True)

        # Close the Deribit client if it exists
        if hasattr(self, 'deribit_client') and self.deribit_client:
            try:
                logger.info("Closing Deribit client...")
                await asyncio.wait_for(self.deribit_client.close(), timeout=10.0)
                logger.info("Deribit client closed")
            except asyncio.TimeoutError:
                logger.warning("Timed out closing Deribit client")
            except Exception as e:
                logger.error(f"Error closing Deribit client: {e}", exc_info=True)

        # Cancel any pending tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"Error cancelling task {task.get_name()}: {e}")

        logger.info("Cleanup complete")


async def main():
    # Create and run the test
    test = HedgingManagerTestHarness()
    await test.setup()
    success = await test.run_test()

    if success:
        logger.info("Test completed successfully!")
    else:
        logger.error("Test failed!")

    # Keep the script running to receive updates
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await test.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
