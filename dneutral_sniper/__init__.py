"""
DNEUTRAL SNIPER - Automated Delta Hedging for Options Portfolios

This package provides tools for managing and delta-hedging options portfolios
on the Deribit exchange, with support for multiple portfolios and real-time
monitoring.
"""

# Import key components to make them available at the package level
from .portfolio import Portfolio
from .portfolio_manager import PortfolioManager
from .subscription_manager import SubscriptionManager
from .hedging_manager import HedgingManager, HedgerConfig
from .deribit_client import DeribitWebsocketClient
from .models import OptionType, ContractType, VanillaOption

# Version
__version__ = "0.1.0"

# Export the CLI as the main entry point
from .cli import cli as main

# Export commonly used components
__all__ = [
    'Portfolio',
    'PortfolioManager',
    'SubscriptionManager',
    'HedgingManager',
    'HedgerConfig',
    'DeribitWebsocketClient',
    'OptionType',
    'ContractType',
    'VanillaOption',
    'main'
]
