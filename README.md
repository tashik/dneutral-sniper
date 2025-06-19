# DNEUTRAL SNIPER

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

DNEUTRAL SNIPER is an automated delta hedging system for options portfolios on the Deribit exchange. It supports multiple independent portfolios with real-time monitoring and dynamic hedging capabilities.

## Features

- **Multi-Portfolio Management**: Manage multiple independent portfolios with different underlyings
- **Real-time Delta Hedging**: Automatically hedge delta exposure using futures contracts
- **Dynamic Portfolio Monitoring**: Monitor portfolio metrics and hedging status in real-time
- **Flexible Configuration**: Customize hedging parameters and risk management rules
- **Command-Line Interface**: Easy-to-use CLI for portfolio management and monitoring

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/dneutral-sniper.git
   cd dneutral-sniper
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

## Configuration

1. Copy the example configuration file:
   ```bash
   cp config.example.json config.json
   ```

2. Edit `config.json` with your Deribit API credentials and preferred settings.

## Usage

### Command-Line Interface

```bash
# Show help
python -m dneutral_sniper.cli --help

# Create a new portfolio
dneutral create-portfolio my_portfolio --underlying BTC --balance 10000

# List all portfolios
dneutral list-portfolios --detailed

# Start delta hedging for a portfolio
dneutral start-hedging my_portfolio

# Monitor active hedgers in real-time
dneutral monitor

# Get status of a specific portfolio
dneutral portfolio-status my_portfolio

# Stop delta hedging for a portfolio
dneutral stop-hedging my_portfolio

# Delete a portfolio
dneutral delete-portfolio my_portfolio
```

### Programmatic Usage

```python
import asyncio
from dneutral_sniper import (
    PortfolioManager, HedgingManager, SubscriptionManager, 
    DeribitWebsocketClient, HedgerConfig
)

async def main():
    # Initialize components
    deribit_client = DeribitWebsocketClient(
        key="your_api_key",
        secret="your_api_secret",
        testnet=True
    )
    
    portfolio_manager = PortfolioManager(portfolios_dir="portfolios")
    await portfolio_manager.initialize()
    
    subscription_manager = SubscriptionManager()
    
    hedger_config = HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="percentage",
        ddh_step_size=0.01,
        price_check_interval=2.0,
        min_hedge_usd=10.0
    )
    
    hedging_manager = HedgingManager(
        portfolio_manager=portfolio_manager,
        subscription_manager=subscription_manager,
        deribit_client=deribit_client,
        default_hedger_config=hedger_config
    )
    
    # Start the hedging manager
    await hedging_manager.start()
    
    # Create a new portfolio
    await portfolio_manager.create_portfolio(
        portfolio_id="my_portfolio",
        underlying="BTC",
        initial_balance=10000.0
    )
    
    # Start hedging for the portfolio
    await hedging_manager.start_hedger("my_portfolio")
    
    # Monitor status
    stats = await hedging_manager.get_hedger_stats("my_portfolio")
    print(f"Current delta: {stats.get('current_delta', 0):.4f} BTC")

if __name__ == "__main__":
    asyncio.run(main())
```

## Architecture

![Architecture Diagram](docs/architecture.png)

DNEUTRAL SNIPER is built with the following components:

1. **PortfolioManager**: Manages multiple portfolios and their persistence
2. **SubscriptionManager**: Handles instrument subscriptions and price updates
3. **HedgingManager**: Coordinates delta hedging across multiple portfolios
4. **DynamicDeltaHedger**: Implements the delta hedging logic for a single portfolio
5. **DeribitWebsocketClient**: Handles communication with the Deribit exchange

## Testing

To run the test suite:

```bash
pytest tests/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For support, please open an issue on the GitHub repository.

## Disclaimer

This software is provided for educational and informational purposes only. Use at your own risk. The authors are not responsible for any financial losses incurred while using this software.
