"""
DNEUTRAL SNIPER - Multi-Portfolio Delta Hedging System

This is the main entry point for the DNEUTRAL SNIPER application, which provides
automated delta hedging for multiple option portfolios using the Deribit exchange.
"""
import asyncio
import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any

from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.hedging_manager import HedgingManager, HedgerConfig
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager
from dneutral_sniper.models import ContractType, OptionType, VanillaOption

# Default configuration
DEFAULT_CONFIG = {
    "portfolios_dir": "data/portfolios",  # Directory to store portfolio files
    "underlying": "BTC",  # Default underlying asset
    "deribit_testnet": False,  # Use Deribit testnet
    # Default hedger configuration
    "hedger_config": {
        "volatility": 0.4,  # Default volatility
        "risk_free_rate": 0.0,  # Default risk-free rate
        "ddh_min_trigger_delta": 0.01,  # 0.01 BTC
        "ddh_target_delta": 0.0,  # Target delta-neutral
        "ddh_step_mode": "absolute",
        "ddh_step_size": 100.00,  # 100 USD
        "price_check_interval": 2.0,  # seconds
        "min_hedge_usd": 10.0  # Minimum USD notional for a hedge order
    }
}

def setup_logging(debug: bool = False) -> None:
    """Configure logging for the application with both console and file output.

    Args:
        debug: If True, enable DEBUG level logging and verbose output
    """
    import os
    from logging.handlers import RotatingFileHandler

    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Create a timestamped log file name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f'dneutral_sniper_{timestamp}.log')

    log_level = logging.DEBUG if debug else logging.INFO

    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatters
    debug_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    info_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = debug_formatter if debug else info_formatter
    console_handler.setFormatter(console_formatter)

    # Create file handler with rotation (10MB per file, keep 5 backup files)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # Always log everything to file
    file_handler.setFormatter(debug_formatter)

    # Configure root logger
    root_logger.setLevel(logging.DEBUG)  # Set to lowest level, let handlers filter
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Log file location
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to file: {os.path.abspath(log_file)}")

    # Set log levels for specific modules
    if debug:
        logging.getLogger('dneutral_sniper').setLevel(logging.DEBUG)
        logging.getLogger('websockets').setLevel(logging.DEBUG)
        logging.getLogger('asyncio').setLevel(logging.DEBUG)
    else:
        logging.getLogger('websockets').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)

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

        portfolios = await portfolio_manager.list_portfolios()

        if (len(portfolios) == 0):
            await create_new_portfolio(portfolio_manager, deribit_client, "BTC", 10000.0)

        subscription_manager = SubscriptionManager(deribit_client=deribit_client)

        # Initialize HedgingManager with default configuration
        hc = config["hedger_config"]
        hedger_config = HedgerConfig(
            ddh_min_trigger_delta=hc["ddh_min_trigger_delta"],
            ddh_target_delta=hc["ddh_target_delta"],
            ddh_step_mode=hc["ddh_step_mode"],
            ddh_step_size=hc["ddh_step_size"],
            price_check_interval=hc["price_check_interval"],
            min_hedge_usd=hc["min_hedge_usd"],
            underlying=config["underlying"],
            volatility=hc["volatility"],
            risk_free_rate=hc["risk_free_rate"],
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


def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='DNEUTRAL SNIPER - Multi-Portfolio Delta Hedging System')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging and verbose output'
    )
    return vars(parser.parse_args())


async def main():
    """Main entry point for the DNEUTRAL SNIPER application."""
    # Parse command line arguments
    args = parse_args()

    # Setup logging based on debug flag
    setup_logging(debug=args.get('debug', False))

    logger.info("DNEUTRAL SNIPER started")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Debug logging enabled")

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
    portfolio_id = f"{underlying.lower()}_{int(datetime.now().timestamp())}"

    try:
        # Create the portfolio
        await portfolio_manager.create_portfolio(
            portfolio_id=portfolio_id,
            underlying=underlying,
            initial_balance=initial_balance
        )

        logger.info(f"Created new portfolio: {portfolio_id}")

        portfolio = await portfolio_manager.get_portfolio(portfolio_id)

        futures_instrument = f"{underlying}-PERPETUAL"

        # Get current futures price if available
        futures_mark_price, _ = await deribit_client.get_instrument_mark_price_and_iv(futures_instrument)
        if futures_mark_price is not None:
            logger.info(f"Using current price of {futures_instrument}: ${futures_mark_price:,.2f}")

        strike = 110000
        # Set expiry to June 27, 2025, 08:00 UTC
        expiry_date = datetime(2025, 6, 27, 8, 0, tzinfo=timezone.utc)  # 2025-06-27 08:00 UTC
        call_option = create_option(
            OptionType.CALL,
            strike,
            expiry_date,
            2.0,
            underlying,
            ContractType.INVERSE
        )

        put_option = create_option(
            OptionType.PUT,
            strike - 10000,
            expiry_date,
            -2.0,
            underlying,
            ContractType.INVERSE
        )

        if call_option:
            # Verify the option exists and get mark price
            mark_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(call_option.instrument_name)
            if mark_price_btc is None:
                logger.warning(f"Could not fetch mark price for {call_option.instrument_name}")
                return None
            if portfolio:
                await portfolio.add_option(call_option, mark_price_btc, mark_price_btc * futures_mark_price)
                logger.info(f"Added call option to portfolio: {portfolio_id}")

        if put_option:
            # Verify the option exists and get mark price
            mark_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(put_option.instrument_name)
            if mark_price_btc is None:
                logger.warning(f"Could not fetch mark price for {put_option.instrument_name}")
                return None
            if portfolio:
                await portfolio.add_option(put_option, mark_price_btc, mark_price_btc * futures_mark_price)
                logger.info(f"Added put option to portfolio: {portfolio_id}")

        return portfolio_id

    except Exception as e:
        logger.error(f"Error creating portfolio: {e}")
        raise

def create_option(
    option_type: OptionType,
    strike: float,
    expiry: datetime,
    quantity: float,
    underlying: str = "BTC",
    contract_type: ContractType = ContractType.INVERSE
) -> Optional[VanillaOption]:
    """Create and validate an option with proper error handling.

    Args:
        option_type: Type of option (CALL or PUT)
        strike: Strike price
        expiry: Expiry date as a datetime object
        quantity: Number of contracts (positive for long, negative for short)
        underlying: Underlying asset symbol (e.g., 'BTC' or 'ETH')
        contract_type: Type of contract (INVERSE or QUANTO)

    Returns:
        VanillaOption instance if successful, None otherwise
    """
    try:
        if not isinstance(expiry, datetime):
            raise ValueError("expiry must be a datetime object")

        # Format expiry as string for instrument name
        expiry_str = expiry.strftime('%d%b%y').upper()

        instrument_name = f"{underlying}-{expiry_str}-{int(strike)}-{option_type.value[0].upper()}"

        option = VanillaOption(
            instrument_name=instrument_name,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            quantity=quantity,
            underlying=underlying,
            contract_type=contract_type
        )

        return option

    except Exception as e:
        logger.error(f"Error creating {option_type.value} option: {e}", exc_info=True)
        return None

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
