import asyncio
from datetime import datetime, timedelta
from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.deribit_client import DeribitCredentials, DeribitWebsocketClient
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.models import OptionType, VanillaOption
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def initialize_usd_value_hedged_strangle(deribit_client: DeribitWebsocketClient, expiry: datetime, current_price: float, call_strike: float, put_strike: float) -> Portfolio:
    # Add sample options to the portfolio
    # Example: Long strangle strategy
    portfolio = Portfolio()

    futures_mark_price, _ = await deribit_client.get_instrument_mark_price_and_iv("BTC-PERPETUAL")
    if futures_mark_price is None:
        logger.warning("Could not fetch mark price for BTC-PERPETUAL, skipping futures hedge.")
    else:
        current_price = futures_mark_price

    # Long 1 call option
    call_expiry_str = expiry.strftime('%d%b%y').upper()
    otm_call = VanillaOption(
        instrument_name=f"BTC-{call_expiry_str}-{call_strike}-C",
        option_type=OptionType.CALL,
        strike=call_strike,
        expiry=expiry,
        quantity=2.0,
        underlying="BTC-{call_expiry_str}"
    )
    otm_call_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(otm_call.instrument_name)
    otm_call_usd_value = None
    if otm_call_price_btc is not None:
        otm_call_usd_value = otm_call.quantity * otm_call_price_btc * current_price
        premium_usd = otm_call.quantity * otm_call_price_btc * current_price
        portfolio.add_option(otm_call, premium_usd=premium_usd)
        logger.info(f"Added call option: {otm_call}")
        logger.info(f"Hedging USD value of call: {'Buy' if otm_call.quantity > 0 else 'Sell'} {otm_call_usd_value:.2f} USD notional of BTC-PERPETUAL (option price: {otm_call_price_btc} BTC)")
        portfolio.initial_usd_hedge_position += otm_call_usd_value
        portfolio.initial_usd_hedge_avg_entry = current_price
        portfolio.last_hedge_price = current_price
        # Mark initial USD hedge as done
        portfolio.initial_usd_hedged = True
        portfolio.save_to_file('portfolio.json')
    else:
        portfolio.add_option(otm_call)
        logger.warning(f"Could not fetch mark price for {otm_call.instrument_name}, skipping call hedge.")

    # Long 1 put option
    put_expiry_str = expiry.strftime('%d%b%y').upper()
    otm_put = VanillaOption(
        instrument_name=f"BTC-{put_expiry_str}-{put_strike}-P",
        option_type=OptionType.PUT,
        strike=put_strike,
        expiry=expiry,
        quantity=2.0,
        underlying="BTC-{put_expiry_str}"
    )
    otm_put_price_btc, _ = await deribit_client.get_instrument_mark_price_and_iv(otm_put.instrument_name)
    otm_put_usd_value = None
    if otm_put_price_btc is not None:
        otm_put_usd_value = otm_put.quantity * otm_put_price_btc * current_price
        premium_usd = otm_put.quantity * otm_put_price_btc * current_price
        portfolio.add_option(otm_put, premium_usd=premium_usd)
        logger.info(f"Added put option: {otm_put}")
        logger.info(f"Hedging USD value of put: {'Buy' if otm_put.quantity > 0 else 'Sell'} {otm_put_usd_value:.2f} USD notional of BTC-PERPETUAL (option price: {otm_put_price_btc} BTC)")
        portfolio.initial_usd_hedge_position += otm_put_usd_value
        portfolio.initial_usd_hedge_avg_entry = current_price
        portfolio.last_hedge_price = current_price
        # Mark initial USD hedge as done
        portfolio.initial_usd_hedged = True
        portfolio.save_to_file('portfolio.json')
    else:
        portfolio.add_option(otm_put)
        logger.warning(f"Could not fetch mark price for {otm_put.instrument_name}, skipping put hedge.")
    return portfolio


async def main():
    logger.info("DNEUTRAL SNIPER started")
    # Optional: for authenticated access
    # credentials = DeribitCredentials(
    #     client_id="your_client_id",
    #     client_secret="your_client_secret",
    #     test=True  # Use testnet
    # )
    credentials = None
    # Initialize hedger
    deribit_client = DeribitWebsocketClient(credentials)
    await deribit_client.connect()
    # Load portfolio from file if it exists
    portfolio_file = 'portfolio.json'
    if os.path.exists(portfolio_file):
        portfolio = Portfolio.load_from_file(portfolio_file)
        logger.info(f"Loaded portfolio from {portfolio_file}")
    else:
        expiry = datetime(2025, 6, 27, 8, 0, 0)  # June 27, 2025, 08:00:00 UTC
        current_price = 105800
        call_strike = 110000
        put_strike = 100000
        portfolio = await initialize_usd_value_hedged_strangle(deribit_client, expiry, current_price, call_strike, put_strike)

    # Subscribe to all option instruments and BTC-PERPETUAL
    instrument_names = set(["BTC-PERPETUAL"]) | set(o.instrument_name for o in getattr(portfolio, 'options', {}).values())
    await deribit_client.subscribe_to_instruments(instrument_names)

    # Create hedger configuration
    config = HedgerConfig(
        ddh_min_trigger_delta=0.01,  # Minimum delta difference to trigger hedge
        ddh_target_delta=0.0,       # Target neutral delta
        ddh_step_mode="absolute",   # Use absolute price movement to trigger check
        ddh_step_size=100,          # 100 USD price movement to trigger check
        instrument_name="BTC-PERPETUAL",
        volatility=0.4,             # 40% annualized volatility
        price_check_interval=2.0    # Check every 2 seconds
    )

    hedger = DynamicDeltaHedger(config, portfolio, deribit_client)

    try:
        logger.info("Starting dynamic delta hedger...")
        await hedger.start()
    except KeyboardInterrupt:
        logger.info("Shutting down hedger...")
        await hedger.stop()
        if hasattr(hedger, 'portfolio'):
            hedger.portfolio.save_to_file('portfolio.json')
            print("Portfolio state saved on shutdown.")
    except Exception as e:
        logger.error(f"Error in hedger: {e}")
        await hedger.stop()
    finally:
        logger.info("main() completed")

if __name__ == "__main__":
    asyncio.run(main())
