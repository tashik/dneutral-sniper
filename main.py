"""
DNEUTRAL SNIPER - Multi-Portfolio Delta Hedging System

This is the main entry point for the DNEUTRAL SNIPER application, which provides
automated delta hedging for multiple option portfolios using the Deribit exchange.
"""
import asyncio
import logging
import sys
from datetime import datetime
from typing import Tuple

from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.hedging_manager import HedgingManager, HedgerConfig
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager

# Default configuration
DEFAULT_CONFIG = {
    "portfolios_dir": "portfolios",  # Directory to store portfolio files
    "underlying": "BTC",  # Default underlying asset
    "deribit_testnet": False,  # Use Deribit testnet
    "deribit_credentials": {
        "key": "YOUR_API_KEY",
        "secret": "YOUR_API_SECRET",
        "testnet": False
    },
    # Default hedger configuration
    "hedger_config": {
        "ddh_min_trigger_delta": 0.01,  # 0.01 BTC
        "ddh_target_delta": 0.0,  # Target delta-neutral
        "ddh_step_mode": "percentage",
        "ddh_step_size": 0.01,  # 1%
        "price_check_interval": 2.0,  # seconds
        "min_hedge_usd": 10.0  # Minimum USD notional for a hedge order
    }
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
        # logging.FileHandler('dneutral_sniper.log')
    ]
)
logger = logging.getLogger(__name__)

# Global flag for shutdown signal
shutdown_event = asyncio.Event()


def handle_shutdown(sig, frame):
    """Handle shutdown signal from OS."""
    logger.info("Shutdown signal received, shutting down...")
    shutdown_event.set()
    sys.exit(0)


async def initialize_managers(
    config: dict
) -> Tuple[PortfolioManager, SubscriptionManager, HedgingManager, DeribitWebsocketClient]:
    """Initialize and return all manager instances."""
    try:
        # Initialize Deribit client with credentials if provided
        deribit_credentials = config.get("deribit_credentials")
        credentials_obj = None

        if deribit_credentials and deribit_credentials.get("key") and deribit_credentials.get("secret"):
            credentials_obj = DeribitCredentials(
                client_id=deribit_credentials["key"],
                client_secret=deribit_credentials["secret"],
                test=deribit_credentials.get("testnet", True)
            )

        is_test = deribit_credentials.get("testnet", True) if deribit_credentials else False
        deribit_client = DeribitWebsocketClient(
            credentials=credentials_obj,
            is_test=is_test
        )

        # Connect to Deribit
        await deribit_client.connect()

        # Initialize managers
        portfolio_manager = PortfolioManager(portfolios_dir=config["portfolios_dir"])
        await portfolio_manager.initialize()

        subscription_manager = SubscriptionManager()

        # Initialize HedgingManager with default configuration
        hc = config["hedger_config"]
        hedger_config = HedgerConfig(
            ddh_min_trigger_delta=hc["ddh_min_trigger_delta"],
            ddh_target_delta=hc["ddh_target_delta"],
            ddh_step_mode=hc["ddh_step_mode"],
            ddh_step_size=hc["ddh_step_size"],
            price_check_interval=hc["price_check_interval"],
            min_hedge_usd=hc["min_hedge_usd"]
        )

        hedging_manager = HedgingManager(
            portfolio_manager=portfolio_manager,
            subscription_manager=subscription_manager,
            deribit_client=deribit_client,
            default_hedger_config=hedger_config
        )

        return portfolio_manager, subscription_manager, hedging_manager, deribit_client

    except Exception as e:
        logger.error(f"Error initializing managers: {e}")
        raise


async def monitor_hedgers(hedging_manager: HedgingManager) -> None:
    """Monitor and log hedger status."""
    while not shutdown_event.is_set():
        try:
            # Get stats for all hedgers
            stats = await hedging_manager.get_all_hedger_stats()

            # Log status for each hedger
            for portfolio_id, stat in stats.items():
                if not stat:  # Skip if no stats
                    continue

                # Safely get values with defaults
                current_delta = stat.get('current_delta', 0) or 0
                hedge_count = stat.get('hedge_count', 0) or 0
                last_hedge = stat.get('last_hedge_time')

                # Format the last hedge time
                last_hedge_str = 'Never'
                if last_hedge and last_hedge != 'None':
                    try:
                        # If it's a timestamp, format it nicely
                        if isinstance(last_hedge, (int, float)) and last_hedge > 0:
                            last_hedge_str = datetime.fromtimestamp(last_hedge).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            last_hedge_str = str(last_hedge)
                    except (TypeError, ValueError):
                        last_hedge_str = str(last_hedge)

                logger.info(
                    f"Portfolio {portfolio_id}: "
                    f"Delta={current_delta:.4f} BTC, "
                    f"Hedges={hedge_count}, "
                    f"Last Hedge={last_hedge_str}"
                )

            # Wait before next update
            await asyncio.sleep(10)  # Log every 10 seconds

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in monitor_hedgers: {e}")
            await asyncio.sleep(5)  # Wait a bit before retrying


async def shutdown(
    hedging_manager: HedgingManager,
    deribit_client: DeribitWebsocketClient
) -> None:
    """Gracefully shutdown all components."""
    logger.info("Shutting down...")

    try:
        # Stop the hedging manager
        if hedging_manager:
            await hedging_manager.stop()
    except Exception as e:
        logger.error(f"Error stopping hedging manager: {e}")

    try:
        # Close the Deribit client
        if deribit_client:
            await deribit_client.close()
    except Exception as e:
        logger.error(f"Error closing Deribit client: {e}")

    logger.info("Shutdown complete")


async def main():
    """Main entry point for the DNEUTRAL SNIPER application."""
    logger.info("DNEUTRAL SNIPER started")

    # Initialize configuration
    # Ensure underlying is uppercase for consistency
    DEFAULT_CONFIG["underlying"] = DEFAULT_CONFIG["underlying"].upper()

    # Initialize managers
    portfolio_manager = None
    subscription_manager = None
    hedging_manager = None
    deribit_client = None

    try:
        # Initialize all managers
        portfolio_manager, subscription_manager, hedging_manager, deribit_client = \
            await initialize_managers(DEFAULT_CONFIG)

        # Start the hedging manager
        await hedging_manager.start()

        # Start monitoring task
        monitor_task = asyncio.create_task(monitor_hedgers(hedging_manager))

        logger.info("DNEUTRAL SNIPER is running. Press Ctrl+C to stop.")

        # Wait for shutdown signal
        while not shutdown_event.is_set():
            await asyncio.sleep(1)

        # Cancel monitoring task
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

    except asyncio.CancelledError:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    finally:
        # Graceful shutdown
        await shutdown(hedging_manager, deribit_client)
        logger.info("DNEUTRAL SNIPER stopped")


async def create_new_portfolio(
    portfolio_manager: PortfolioManager,
    deribit_client: DeribitWebsocketClient,
    underlying: str = "BTC",
    initial_balance: float = 10000.0
) -> str:
    """Create a new portfolio with the specified parameters.

    Args:
        portfolio_manager: Initialized PortfolioManager instance
        deribit_client: Initialized Deribit client
        underlying: The underlying asset (e.g., "BTC" or "ETH")
        initial_balance: Initial balance in USD

    Returns:
        str: ID of the created portfolio
    """
    # Generate a unique portfolio ID
    portfolio_id = f"{underlying.lower()}_{int(datetime.utcnow().timestamp())}"

    try:
        # Create the portfolio
        await portfolio_manager.create_portfolio(
            portfolio_id=portfolio_id,
            underlying=underlying,
            initial_balance=initial_balance
        )

        logger.info(f"Created new portfolio: {portfolio_id}")
        return portfolio_id

    except Exception as e:
        logger.error(f"Error creating portfolio: {e}")
        raise


if __name__ == "__main__":
    try:
        # Set up event loop policy for Windows compatibility
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        # Run the main application
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Application terminated")
