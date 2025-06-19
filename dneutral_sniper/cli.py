"""
DNEUTRAL SNIPER - Command Line Interface

This module provides a command-line interface for managing the DNEUTRAL SNIPER
delta hedging system, including portfolio management and monitoring.
"""
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import signal

import click
from tabulate import tabulate

from dneutral_sniper.deribit_client import DeribitWebsocketClient, DeribitCredentials
from dneutral_sniper.hedging_manager import HedgingManager, HedgerConfig
from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.subscription_manager import SubscriptionManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
        # logging.FileHandler('dneutral_sniper.log')
    ]
)
logger = logging.getLogger(__name__)

# Global state
managers = {
    'portfolio_manager': None,
    'subscription_manager': None,
    'hedging_manager': None,
    'deribit_client': None
}

# Shutdown event for graceful shutdown
shutdown_event = asyncio.Event()

# Default configuration
DEFAULT_CONFIG = {
    "portfolios_dir": "portfolios",
    "underlying": "BTC",
    "deribit_testnet": False,
    "deribit_credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "test": True
    },
    "hedger_config": {
        "ddh_min_trigger_delta": 0.01,
        "ddh_target_delta": 0.0,
        "ddh_step_mode": "percentage",
        "ddh_step_size": 0.01,
        "price_check_interval": 2.0,
        "min_hedge_usd": 10.0
    }
}


def load_config(config_file: str = None) -> Dict[str, Any]:
    """Load configuration from file or use defaults."""
    if config_file and Path(config_file).exists():
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {config_file}")
                return config
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            sys.exit(1)
    return DEFAULT_CONFIG


async def initialize_managers(config: Dict[str, Any]) -> None:
    """Initialize all manager instances."""
    try:
        # Initialize Deribit client with credentials if provided
        deribit_credentials = config.get("deribit_credentials")
        credentials_obj = None

        if deribit_credentials and deribit_credentials.get("client_id") and deribit_credentials.get("client_secret"):
            credentials_obj = DeribitCredentials(
                client_id=deribit_credentials["client_id"],
                client_secret=deribit_credentials["client_secret"],
                test=deribit_credentials.get("test", True)
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

        # Initialize HedgingManager with configuration
        hedger_config = HedgerConfig(
            ddh_min_trigger_delta=config["hedger_config"]["ddh_min_trigger_delta"],
            ddh_target_delta=config["hedger_config"]["ddh_target_delta"],
            ddh_step_mode=config["hedger_config"]["ddh_step_mode"],
            ddh_step_size=config["hedger_config"]["ddh_step_size"],
            price_check_interval=config["hedger_config"]["price_check_interval"],
            min_hedge_usd=config["hedger_config"]["min_hedge_usd"]
        )

        hedging_manager = HedgingManager(
            portfolio_manager=portfolio_manager,
            subscription_manager=subscription_manager,
            deribit_client=deribit_client,
            default_hedger_config=hedger_config
        )

        # Update global state
        managers.update({
            'portfolio_manager': portfolio_manager,
            'subscription_manager': subscription_manager,
            'hedging_manager': hedging_manager,
            'deribit_client': deribit_client
        })

    except Exception as e:
        logger.error(f"Error initializing managers: {e}")
        await shutdown()
        sys.exit(1)


async def shutdown() -> None:
    """Gracefully shutdown all components."""
    logger.info("Shutting down...")

    try:
        # Stop the hedging manager
        if managers['hedging_manager']:
            await managers['hedging_manager'].stop()
    except Exception as e:
        logger.error(f"Error stopping hedging manager: {e}")

    try:
        # Close the Deribit client
        if managers['deribit_client']:
            await managers['deribit_client'].close()
    except Exception as e:
        logger.error(f"Error closing Deribit client: {e}")

    logger.info("Shutdown complete")


# CLI Commands

@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Path to configuration file')
@click.pass_context
def cli(ctx, config):
    """DNEUTRAL SNIPER - Multi-Portfolio Delta Hedging System"""
    # Load configuration
    ctx.ensure_object(dict)
    ctx.obj['config'] = load_config(config)

    # Initialize managers
    asyncio.run(initialize_managers(ctx.obj['config']))

    # Register shutdown handler
    def handle_shutdown(sig, frame):
        logger.info("Shutdown signal received")
        asyncio.create_task(shutdown())
        sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_shutdown)


@cli.command()
@click.argument('portfolio_id')
@click.option('--underlying', '-u', default='BTC', help='Underlying asset (e.g., BTC, ETH)')
@click.option('--balance', '-b', type=float, default=10000.0, help='Initial balance in USD')
def create_portfolio(portfolio_id, underlying, balance):
    """Create a new portfolio."""
    async def _create_portfolio():
        try:
            await managers['portfolio_manager'].create_portfolio(
                portfolio_id=portfolio_id,
                underlying=underlying.upper(),
                initial_balance=balance
            )
            click.echo(f"Created portfolio '{portfolio_id}' with {underlying} and ${balance:,.2f} balance")
        except Exception as e:
            click.echo(f"Error creating portfolio: {e}", err=True)

    asyncio.run(_create_portfolio())


@cli.command()
@click.argument('portfolio_id')
def delete_portfolio(portfolio_id):
    """Delete a portfolio."""
    async def _delete_portfolio():
        try:
            success = await managers['portfolio_manager'].delete_portfolio(portfolio_id)
            if success:
                click.echo(f"Deleted portfolio '{portfolio_id}'")
            else:
                click.echo(f"Portfolio '{portfolio_id}' not found")
        except Exception as e:
            click.echo(f"Error deleting portfolio: {e}", err=True)

    asyncio.run(_delete_portfolio())


@cli.command()
@click.option('--detailed', is_flag=True, help='Show detailed portfolio information')
def list_portfolios(detailed):
    """List all portfolios."""
    async def _list_portfolios():
        try:
            portfolios = await managers['portfolio_manager'].list_portfolios()

            if detailed:
                # Show detailed information for each portfolio
                table = []
                for pid in portfolios:
                    portfolio = await managers['portfolio_manager'].get_portfolio(pid)
                    hedger_stats = await managers['hedging_manager'].get_hedger_stats(pid)

                    table.append([
                        pid,
                        portfolio.underlying if hasattr(portfolio, 'underlying') else 'N/A',
                        f"${portfolio.initial_balance:,.2f}" if hasattr(portfolio, 'initial_balance') else 'N/A',
                        f"{hedger_stats.get('current_delta', 0):.4f} BTC" if hedger_stats else 'N/A',
                        hedger_stats.get('hedge_count', 0) if hedger_stats else 'N/A',
                        hedger_stats.get('last_hedge_time', 'Never') if hedger_stats else 'N/A'
                    ])

                headers = ["ID", "Underlying", "Balance", "Delta", "Hedges", "Last Hedge"]
                click.echo(tabulate(table, headers=headers, tablefmt="grid"))
            else:
                # Show simple list
                for pid in portfolios:
                    click.echo(pid)
        except Exception as e:
            click.echo(f"Error listing portfolios: {e}", err=True)

    asyncio.run(_list_portfolios())


@cli.command()
@click.argument('portfolio_id')
def start_hedging(portfolio_id):
    """Start delta hedging for a portfolio."""
    async def _start_hedging():
        try:
            # Start the hedging manager if not already running
            await managers['hedging_manager'].start()
            click.echo(f"Started delta hedging for portfolio '{portfolio_id}'")
        except Exception as e:
            click.echo(f"Error starting hedging: {e}", err=True)
            raise

    asyncio.run(_start_hedging())


@cli.command()
@click.argument('portfolio_id')
def stop_hedging(portfolio_id):
    """Stop delta hedging for a portfolio."""
    async def _stop_hedging():
        try:
            # Stop the hedging manager
            await managers['hedging_manager'].stop()
            click.echo(f"Stopped delta hedging for portfolio '{portfolio_id}'")
        except Exception as e:
            click.echo(f"Error stopping hedging: {e}", err=True)
            raise

    asyncio.run(_stop_hedging())


@cli.command()
@click.argument('portfolio_id')
@click.argument('instrument_name')
@click.option('--quantity', type=float, required=True,
              help='Number of contracts (positive for long, negative for short)')
@click.option('--strike', type=float, required=True, help='Strike price')
@click.option('--expiry', required=True, help='Expiration date (YYYY-MM-DD)')
@click.option('--option-type', type=click.Choice(['call', 'put'], case_sensitive=False),
              required=True, help='Option type (call/put)')
@click.option('--contract-type', type=click.Choice(['inverse', 'standard'], case_sensitive=False),
              default='inverse', show_default=True, help='Contract type (inverse/standard)')
@click.option('--mark-price', type=float, help='Mark price in USD')
@click.option('--iv', type=float, help='Implied volatility (0-1)')
@click.option('--usd-value', type=float, help='USD value of the position')
@click.option('--delta', type=float, help='Delta of the option')
@click.option('--current-price', type=float,
              help='Current price of the underlying (required for initial premium hedging)')
def add_option(
    portfolio_id,
    instrument_name,
    quantity,
    strike,
    expiry,
    option_type,
    contract_type,
    mark_price,
    iv,
    usd_value,
    delta,
    current_price
):
    """Add an option to a portfolio.

    If this is the first option being added to the portfolio, the initial premium will be
    hedged using the provided current price.
    """
    async def _add_option():
        nonlocal current_price
        try:
            portfolio_manager = managers['portfolio_manager']
            portfolio = portfolio_manager.portfolios.get(portfolio_id, {})
            # If no current price is provided but we need to hedge the initial premium,
            # try to get the current price from Deribit
            if current_price is None and not getattr(portfolio, 'initial_usd_hedged', True):
                try:
                    # Get the underlying from the portfolio or use the first part of the instrument name
                    underlying = getattr(
                        portfolio,
                        'underlying',
                        instrument_name.split('-')[0] if '-' in instrument_name else 'BTC'
                    )
                    ticker = f"{underlying.upper()}-PERPETUAL"
                    ticker_data = await managers['deribit_client'].get_ticker(ticker)
                    if ticker_data and 'last_price' in ticker_data and ticker_data['last_price']:
                        current_price = ticker_data['last_price']
                        click.echo(
                            f"Using current price from {ticker}: ${current_price:,.2f}"
                        )
                except Exception as e:
                    click.echo(f"Warning: Could not fetch current price: {e}")
                    click.echo(
                        "Please provide --current-price for initial premium hedging"
                    )
                    return

            success = await managers['portfolio_manager'].add_option_to_portfolio(
                portfolio_id=portfolio_id,
                option_instrument=instrument_name,
                quantity=quantity,
                strike=strike,
                expiry=expiry,
                option_type=option_type,
                contract_type=contract_type,
                mark_price=mark_price,
                iv=iv,
                usd_value=usd_value,
                delta=delta,
                current_price=current_price
            )
            if success:
                click.echo(
                    f"Added {quantity} of {instrument_name} to portfolio '{portfolio_id}'"
                )

                # Subscribe to the instrument
                await managers['subscription_manager'].add_subscription(
                    portfolio_id, instrument_name
                )
                click.echo(
                    f"Subscribed to {instrument_name} for portfolio '{portfolio_id}'"
                )

                # Show updated portfolio status
                await _show_portfolio_status(portfolio_id)
            else:
                click.echo(
                    f"Failed to add option to portfolio '{portfolio_id}'",
                    err=True
                )
        except Exception as e:
            click.echo(f"Error adding option: {e}", err=True)

    async def _show_portfolio_status(pid):
        """Helper to show portfolio status"""
        try:
            # Get portfolio
            portfolio = await managers['portfolio_manager'].get_portfolio(pid)
            if not portfolio:
                click.echo(f"Portfolio '{pid}' not found", err=True)
                return

            # Display portfolio info
            click.echo(f"\nPortfolio: {pid}")
            click.echo(f"Underlying: {getattr(portfolio, 'underlying', 'N/A')}")
            click.echo(
                f"Initial Balance: ${getattr(portfolio, 'initial_balance', 0):,.2f}"
            )

            # Display options if any
            if hasattr(portfolio, 'options') and portfolio.options:
                click.echo("\nOptions:")
                for opt in portfolio.options.values():
                    click.echo(f"  {opt.instrument_name}: {opt.quantity} contracts")
                    click.echo(
                        f"    Type: {opt.option_type.value.upper()}, "
                        f"Strike: {opt.strike}, Expiry: {opt.expiry.date()}"
                    )
                    if opt.mark_price is not None:
                        click.echo(f"    Mark: ${opt.mark_price:,.2f}", nl='')
                    if opt.delta is not None:
                        click.echo(f", Delta: {opt.delta:,.4f}")
                    else:
                        click.echo()
            else:
                click.echo("\nNo options in portfolio")

        except Exception as e:
            click.echo(f"Error showing portfolio status: {e}", err=True)

    asyncio.run(_add_option())


@cli.command()
@click.argument('portfolio_id')
def portfolio_status(portfolio_id):
    """Show status of a portfolio."""
    async def _portfolio_status():
        try:
            # Get portfolio
            portfolio = await managers['portfolio_manager'].get_portfolio(portfolio_id)
            if not portfolio:
                click.echo(f"Portfolio '{portfolio_id}' not found", err=True)
                return

            # Get hedger stats
            stats = await managers['hedging_manager'].get_hedger_stats(portfolio_id)

            # Display portfolio info
            click.echo(f"Portfolio: {portfolio_id}")
            click.echo(f"Underlying: {getattr(portfolio, 'underlying', 'N/A')}")
            click.echo(f"Initial Balance: ${getattr(portfolio, 'initial_balance', 0):,.2f}")

            # Display options if any
            if hasattr(portfolio, 'options') and portfolio.options:
                click.echo("\nOptions:")
                for opt in portfolio.options.values():
                    click.echo(f"  - {opt.instrument_name}: {opt.quantity} contracts")

            # Display hedger status
            if stats:
                click.echo("\nHedging Status:")
                click.echo(f"  Current Delta: {stats.get('current_delta', 0):.4f} BTC")
                click.echo(f"  Hedge Count: {stats.get('hedge_count', 0)}")
                click.echo(f"  Last Hedge: {stats.get('last_hedge_time', 'Never')}")
                click.echo(f"  Is Hedging: {'Yes' if stats.get('is_hedging', False) else 'No'}")
            else:
                click.echo("\nHedging not active for this portfolio")

        except Exception as e:
            click.echo(f"Error getting portfolio status: {e}", err=True)

    asyncio.run(_portfolio_status())


@cli.command()
def monitor():
    """Monitor all active hedgers in real-time."""

    async def _monitor():
        try:
            click.echo("Monitoring active hedgers (Ctrl+C to exit)...\n")

            while True:
                # Clear screen and move cursor to top
                click.clear()
                click.echo(f"DNEUTRAL SNIPER - Live Monitoring - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                click.echo("-" * 80)

                # Get all portfolios
                portfolios = managers['portfolio_manager'].list_portfolios()

                if not portfolios:
                    click.echo("No portfolios found")
                    await asyncio.sleep(5)
                    continue

                # Create table rows
                table = []
                for pid in portfolios:
                    stats = await managers['hedging_manager'].get_hedger_stats(pid)
                    if stats:
                        table.append([
                            pid,
                            f"{stats.get('current_delta', 0):.4f} BTC",
                            stats.get('hedge_count', 0),
                            stats.get('last_hedge_time', 'Never'),
                            'Active' if stats.get('is_hedging', False) else 'Inactive'
                        ])

                # Display table
                if table:
                    headers = ["Portfolio", "Delta", "Hedges", "Last Hedge", "Status"]
                    click.echo(tabulate(table, headers=headers, tablefmt="grid"))
                else:
                    click.echo("No active hedgers")

                # Wait before next update
                await asyncio.sleep(2)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            click.echo(f"Error in monitor: {e}", err=True)

    try:
        asyncio.run(_monitor())
    except KeyboardInterrupt:
        click.echo("\nMonitoring stopped")


if __name__ == "__main__":
    cli()
