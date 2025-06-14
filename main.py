import asyncio
from datetime import datetime, timedelta
from dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from deribit_client import DeribitCredentials
from options import VanillaOption, OptionType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Optional: for authenticated access
    credentials = DeribitCredentials(
        client_id="your_client_id",
        client_secret="your_client_secret",
        test=True  # Use testnet
    )

    # Create hedger configuration
    config = HedgerConfig(
        ddh_min_trigger_delta=0.1,  # Minimum delta difference to trigger hedge
        ddh_target_delta=0.0,      # Target neutral delta
        ddh_step_mode="percentage",
        ddh_step_size=0.01,        # 1% price movement to trigger check
        instrument_name="BTC-PERPETUAL",
        volatility=0.8,            # 80% annualized volatility
        price_check_interval=2.0   # Check every 2 seconds
    )

    # Initialize hedger
    hedger = DynamicDeltaHedger(config, credentials)

    # Add sample options to the portfolio
    # Example: Long strangle strategy with 30-day expiry
    expiry = datetime.now() + timedelta(days=30)
    current_price = 30000  # Example BTC price

    # Long 1 OTM call option
    otm_call = VanillaOption(
        instrument_name="BTC-35000-C",
        option_type=OptionType.CALL,
        strike=35000.0,
        expiry=expiry,
        quantity=1.0
    )
    hedger.portfolio.add_option(otm_call)
    logger.info(f"Added OTM call option: {otm_call}")

    # Long 1 OTM put option
    otm_put = VanillaOption(
        instrument_name="BTC-25000-P",
        option_type=OptionType.PUT,
        strike=25000.0,
        expiry=expiry,
        quantity=1.0
    )
    hedger.portfolio.add_option(otm_put)
    logger.info(f"Added OTM put option: {otm_put}")

    try:
        logger.info("Starting dynamic delta hedger...")
        await hedger.start()
    except KeyboardInterrupt:
        logger.info("Shutting down hedger...")
        await hedger.stop()
    except Exception as e:
        logger.error(f"Error in hedger: {e}")
        await hedger.stop()
        raise

if __name__ == "__main__":
    asyncio.run(main())
