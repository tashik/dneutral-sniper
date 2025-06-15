import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.deribit_client import DeribitWebsocketClient
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.models import OptionType, VanillaOption, ContractType

# Configuration
CONFIG = {
    "portfolio_file": "portfolio.json",
    "underlying": "BTC",
    "expiry_date": "2025-06-27T08:00:00",  # June 27, 2025, 08:00:00 UTC
    "call_strike": 110000,
    "put_strike": 100000,
    "option_quantity": 2.0,
    "contract_type": "inverse",  # or "standard"
    "deribit_testnet": False,
}

def setup_logging():
    """Configure logging with a more detailed format."""
    log_format = (
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(message)s [%(filename)s:%(lineno)d]"
    )
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('dneutral_sniper.log')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

async def create_option(
    deribit_client: DeribitWebsocketClient,
    option_type: OptionType,
    strike: float,
    expiry: datetime,
    quantity: float,
    contract_type: ContractType = ContractType.INVERSE,
) -> Optional[VanillaOption]:
    """Create and validate an option with proper error handling."""
    try:
        expiry_str = expiry.strftime('%d%b%y').upper()
        underlying = f"BTC-{expiry_str}"
        instrument_name = f"BTC-{expiry_str}-{int(strike)}-{option_type.value[0].upper()}"

        option = VanillaOption(
            instrument_name=instrument_name,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            quantity=quantity,
            underlying=underlying,
            contract_type=contract_type
        )

        # Verify the option exists and get mark price
        mark_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(instrument_name)
        if mark_price_btc is None:
            logger.warning(f"Could not fetch mark price for {instrument_name}")
            return None

        return option

    except Exception as e:
        logger.error(f"Error creating {option_type.value} option: {e}", exc_info=True)
        return None


async def initialize_usd_value_hedged_strangle(
    deribit_client: DeribitWebsocketClient,
    expiry: datetime,
    current_price: float,
    call_strike: float,
    put_strike: float,
    quantity: float = 2.0,
    contract_type: ContractType = ContractType.INVERSE,
) -> Portfolio:
    """Initialize a portfolio with a strangle strategy.

    Args:
        deribit_client: Initialized Deribit client
        expiry: Option expiry datetime
        current_price: Current price of the underlying
        call_strike: Strike price for call option
        put_strike: Strike price for put option
        quantity: Number of contracts for each option
        contract_type: Type of options contract (inverse or standard)

    Returns:
        Portfolio: Initialized portfolio with options
    """
    portfolio = Portfolio()

    # Get current futures price if available
    futures_mark_price, _ = await deribit_client.get_instrument_mark_price_and_iv("BTC-PERPETUAL")
    if futures_mark_price is not None:
        current_price = futures_mark_price
    logger.info(f"Using current price: ${current_price:,.2f}")

    # Create and add call option
    call_option = await create_option(
        deribit_client=deribit_client,
        option_type=OptionType.CALL,
        strike=call_strike,
        expiry=expiry,
        quantity=quantity,
        contract_type=contract_type
    )

    if call_option:
        call_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(call_option.instrument_name)
        if call_price_btc is not None:
            call_usd_value = call_option.quantity * call_price_btc * current_price
            portfolio.add_option(call_option, premium_usd=call_usd_value)
            logger.info(f"Added call option: {call_option}")
            logger.info(
                f"Hedging USD value of call: {'Buy' if call_option.quantity > 0 else 'Sell'} "
                f"${call_usd_value:,.2f} notional of BTC-PERPETUAL "
                f"(price: {call_price_btc:.8f} BTC)"
            )

    # Create and add put option
    put_option = await create_option(
        deribit_client=deribit_client,
        option_type=OptionType.PUT,
        strike=put_strike,
        expiry=expiry,
        quantity=quantity,
        contract_type=contract_type
    )

    if put_option:
        put_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(put_option.instrument_name)
        if put_price_btc is not None:
            put_usd_value = put_option.quantity * put_price_btc * current_price
            portfolio.add_option(put_option, premium_usd=put_usd_value)
            logger.info(f"Added put option: {put_option}")
            logger.info(
                f"Hedging USD value of put: {'Buy' if put_option.quantity > 0 else 'Sell'} "
                f"${put_usd_value:,.2f} notional of BTC-PERPETUAL "
                f"(price: {put_price_btc:.8f} BTC)"
            )

    # Save portfolio after all options are added
    portfolio.initial_usd_hedged = True
    portfolio.last_hedge_price = current_price
    return portfolio


async def save_portfolio(portfolio: Portfolio, filename: str) -> None:
    """Save portfolio to file with error handling."""
    try:
        portfolio.save_to_file(filename)
        logger.info(f"Portfolio saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving portfolio: {e}", exc_info=True)


async def main():
    """Main entry point for the DNEUTRAL SNIPER application."""
    logger.info("DNEUTRAL SNIPER started")

    # Initialize configuration
    portfolio_file = CONFIG["portfolio_file"]
    contract_type = ContractType.INVERSE if CONFIG["contract_type"].lower() == "inverse" else ContractType.STANDARD

    # Ensure underlying is uppercase for consistency
    CONFIG["underlying"] = CONFIG["underlying"].upper()

    # Initialize Deribit client
    credentials = None  # Add your credentials here if needed
    deribit_client = DeribitWebsocketClient(credentials, is_test=CONFIG["deribit_testnet"])

    try:
        await deribit_client.connect()
        logger.info("Connected to Deribit API")

        # Load or create portfolio
        portfolio_path = Path(portfolio_file)
        if portfolio_path.exists():
            try:
                portfolio = Portfolio.load_from_file(portfolio_file)
                logger.info(f"Loaded portfolio from {portfolio_file}")
            except Exception as e:
                logger.error(f"Error loading portfolio: {e}")
                logger.info("Creating new portfolio...")
                portfolio = await create_new_portfolio(deribit_client, contract_type)
        else:
            portfolio = await create_new_portfolio(deribit_client, contract_type)

        # Generate perpetual contract name based on underlying
        def get_perpetual_contract(underlying: str) -> str:
            """Generate perpetual contract name from underlying.

            Args:
                underlying: The underlying asset (e.g., 'BTC', 'ETH')

            Returns:
                str: Formatted perpetual contract name (e.g., 'BTC-PERPETUAL')
            """
            # Extract base asset (e.g., 'BTC' from 'BTC-27JUN25')
            base_asset = underlying.split('-')[0].upper()
            return f"{base_asset}-PERPETUAL"

        # Get unique underlyings from portfolio options
        underlyings = set()
        if hasattr(portfolio, 'options') and portfolio.options:
            for option in portfolio.options.values():
                underlyings.add(option.underlying.split('-')[0].upper())

        # Subscribe to perpetual contracts for all underlyings and all option instruments
        instrument_names = set()
        for underlying in underlyings or [CONFIG["underlying"]]:  # Fallback to config if no options
            instrument_names.add(get_perpetual_contract(underlying))

        # Add all option instruments
        if hasattr(portfolio, 'options') and portfolio.options:
            instrument_names.update(option.instrument_name for option in portfolio.options.values())

        logger.debug(f"Generated instrument names for subscription: {instrument_names}")

        logger.info(f"Subscribing to instruments: {', '.join(instrument_names)}")
        await deribit_client.subscribe_to_instruments(instrument_names)

        # Configure and start hedger
        config = HedgerConfig(
            ddh_min_trigger_delta=0.01,  # 1% of BTC delta
            ddh_target_delta=0.0,       # Target neutral delta
            ddh_step_mode="absolute",   # Use absolute price movement for triggers
            ddh_step_size=100,          # $100 price movement to trigger check
            underlying=CONFIG["underlying"],
            instrument_name="BTC-PERPETUAL",
            volatility=0.4,             # 40% annualized volatility (fallback IV)
            risk_free_rate=0.0,         # 0% risk-free rate
            min_hedge_usd=10.0,         # $10 minimum hedge size
            price_check_interval=2.0    # Check every 2 seconds
        )

        hedger = DynamicDeltaHedger(config, portfolio, deribit_client)

        try:
            logger.info("Starting dynamic delta hedger...")
            await hedger.start()
        except asyncio.CancelledError:
            logger.info("Received shutdown signal, stopping...")
            raise
        except Exception as e:
            logger.error(f"Error in hedger: {e}", exc_info=True)
            raise

    except asyncio.CancelledError:
        logger.info("Shutdown requested...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        # Ensure clean shutdown
        try:
            if 'hedger' in locals() and hasattr(hedger, 'stop'):
                await hedger.stop()
            if 'portfolio' in locals():
                await save_portfolio(portfolio, portfolio_file)
            if 'deribit_client' in locals() and hasattr(deribit_client, 'close'):
                await deribit_client.close()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)

        logger.info("DNEUTRAL SNIPER stopped")


async def create_new_portfolio(
    deribit_client: DeribitWebsocketClient,
    contract_type: ContractType
) -> Portfolio:
    """Create a new portfolio with default options."""
    expiry = datetime.fromisoformat(CONFIG["expiry_date"])
    current_price = 105800  # Will be updated from market data

    portfolio = await initialize_usd_value_hedged_strangle(
        deribit_client=deribit_client,
        expiry=expiry,
        current_price=current_price,
        call_strike=CONFIG["call_strike"],
        put_strike=CONFIG["put_strike"],
        quantity=CONFIG["option_quantity"],
        contract_type=contract_type
    )

    return portfolio

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        raise
    finally:
        logger.info("Application shutdown complete")
