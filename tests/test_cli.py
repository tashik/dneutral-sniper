"""
Tests for the DNEUTRAL SNIPER CLI.
"""
import os
import json
import tempfile
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from unittest.mock import ANY

import pytest
from click.testing import CliRunner

# Import the CLI module
from dneutral_sniper.cli import cli, initialize_managers, shutdown, managers

# Test configuration
TEST_CONFIG = {
    "portfolios_dir": "test_portfolios",
    "underlying": "BTC",
    "deribit_testnet": True,
    "deribit_credentials": {
        "key": "test_key",
        "secret": "test_secret",
        "testnet": True
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

# Helper function to create async mocks
async def async_mock(*args, **kwargs):
    return None

# Fixtures

@pytest.fixture
def runner():
    """Click CLI runner for testing commands."""
    return CliRunner()

@pytest.fixture
def temp_config():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as f:
        json.dump(TEST_CONFIG, f)
        f.flush()
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)

@pytest.fixture
def mock_managers():
    """Create mock managers for testing with proper async/sync method support."""
    with patch('dneutral_sniper.cli.DeribitWebsocketClient') as mock_deribit, \
         patch('dneutral_sniper.cli.PortfolioManager') as mock_pm, \
         patch('dneutral_sniper.cli.SubscriptionManager') as mock_sm, \
         patch('dneutral_sniper.cli.HedgingManager') as mock_hm:

        # Create mock instances with AsyncMock for async methods
        mock_deribit_instance = AsyncMock()
        mock_pm_instance = MagicMock()
        mock_sm_instance = MagicMock()
        mock_hm_instance = AsyncMock()  # HedgingManager has async methods
        
        # Configure the mock constructors to return our instances
        mock_deribit.return_value = mock_deribit_instance
        mock_pm.return_value = mock_pm_instance
        mock_sm.return_value = mock_sm_instance
        mock_hm.return_value = mock_hm_instance

        # Set up mock return values
        mock_pm_instance.list_portfolios.return_value = ["test_portfolio"]
        
        # Mock portfolio object
        mock_portfolio = MagicMock()
        mock_portfolio.underlying = 'BTC'
        mock_portfolio.initial_balance = 10000.0
        mock_portfolio.options = {}
        mock_pm_instance.get_portfolio.return_value = mock_portfolio
        
        # Set up async methods that are awaited in the code
        # For PortfolioManager
        mock_pm_instance.initialize = AsyncMock(return_value=None)
        mock_pm_instance.create_portfolio = AsyncMock(return_value=mock_portfolio)
        mock_pm_instance.save_portfolio = AsyncMock(return_value=None)
        mock_pm_instance.get_portfolio = AsyncMock(return_value=mock_portfolio)
        
        # For SubscriptionManager
        mock_sm_instance.initialize = AsyncMock(return_value=None)
        
        # For HedgingManager
        mock_hm_instance.start_hedger = AsyncMock(return_value=None)
        mock_hm_instance.stop_hedger = AsyncMock(return_value=None)
        mock_hm_instance.stop = AsyncMock(return_value=None)
        
        # Create a mock hedger stats object
        mock_hedger_stats = {
            'current_delta': 0.1,
            'target_delta': 0.0,
            'delta_difference': 0.1,
            'hedge_amount': 0.1,
            'hedge_direction': 'buy',
            'hedge_count': 5,
            'last_hedge_time': '2023-01-01 12:00:00',
            'is_hedging': True,
            'portfolio_value': 10000.0,
            'portfolio_delta': 0.1,
            'portfolio_gamma': 0.0,
            'portfolio_theta': 0.0,
            'portfolio_vega': 0.0,
            'portfolio_rho': 0.0,
            'portfolio_iv': 0.0,
            'portfolio_underlying_price': 50000.0,
            'portfolio_underlying_asset': 'BTC',
            'portfolio_currency': 'BTC',
            'portfolio_initial_balance': 10000.0,
            'portfolio_current_balance': 10000.0,
            'portfolio_pnl': 0.0,
            'portfolio_pnl_pct': 0.0,
            'portfolio_margin_used': 0.0,
            'portfolio_margin_available': 10000.0,
            'portfolio_margin_ratio': 0.0,
            'portfolio_leverage': 1.0,
            'portfolio_risk_free_rate': 0.0,
            'portfolio_volatility': 0.0,
            'portfolio_risk': 0.0,
            'portfolio_sharpe_ratio': 0.0,
            'portfolio_sortino_ratio': 0.0,
            'portfolio_max_drawdown': 0.0,
            'portfolio_max_drawdown_pct': 0.0,
            'portfolio_calmar_ratio': 0.0,
            'portfolio_omega_ratio': 0.0,
            'portfolio_upside_potential_ratio': 0.0,
            'portfolio_downside_risk': 0.0,
            'portfolio_upside_risk': 0.0,
            'portfolio_tracking_error': 0.0,
            'portfolio_information_ratio': 0.0,
            'portfolio_jensens_alpha': 0.0,
            'portfolio_treynor_ratio': 0.0,
            'portfolio_m2_ratio': 0.0,
            'portfolio_m2_alpha': 0.0,
            'portfolio_treynor_black_ratio': 0.0,
            'portfolio_appraisal_ratio': 0.0,
            'portfolio_modigliani_ratio': 0.0,
            'portfolio_modigliani_mod_ratio': 0.0,
            'portfolio_burke_ratio': 0.0,
            'portfolio_sterling_ratio': 0.0,
            'portfolio_sterling_a_ratio': 0.0,
            'portfolio_burke_ratio_2': 0.0,
            'portfolio_sterling_ratio_2': 0.0,
            'portfolio_sterling_a_ratio_2': 0.0,
            'portfolio_burke_ratio_3': 0.0,
            'portfolio_sterling_ratio_3': 0.0,
            'portfolio_sterling_a_ratio_3': 0.0,
            'portfolio_burke_ratio_4': 0.0,
            'portfolio_sterling_ratio_4': 0.0,
            'portfolio_sterling_a_ratio_4': 0.0,
            'portfolio_burke_ratio_5': 0.0,
            'portfolio_sterling_ratio_5': 0.0,
            'portfolio_sterling_a_ratio_5': 0.0,
            'portfolio_burke_ratio_6': 0.0,
            'portfolio_sterling_ratio_6': 0.0,
            'portfolio_sterling_a_ratio_6': 0.0,
            'portfolio_burke_ratio_7': 0.0,
            'portfolio_sterling_ratio_7': 0.0,
            'portfolio_sterling_a_ratio_7': 0.0,
            'portfolio_burke_ratio_8': 0.0,
            'portfolio_sterling_ratio_8': 0.0,
            'portfolio_sterling_a_ratio_8': 0.0,
            'portfolio_burke_ratio_9': 0.0,
            'portfolio_sterling_ratio_9': 0.0,
            'portfolio_sterling_a_ratio_9': 0.0,
            'portfolio_burke_ratio_10': 0.0,
            'portfolio_sterling_ratio_10': 0.0,
            'portfolio_sterling_a_ratio_10': 0.0,
        }
        
        # Set up the get_hedger_stats method to return the mock hedger stats
        mock_hm_instance.get_hedger_stats = AsyncMock(return_value=mock_hedger_stats)
        
        # For DeribitWebsocketClient
        mock_deribit_instance.connect = AsyncMock(return_value=None)
        mock_deribit_instance.close = AsyncMock(return_value=None)
        mock_deribit_instance.get_instrument = AsyncMock(return_value={'tickSize': 0.1, 'tickSize': 0.1, 'minTradeAmount': 1, 'contractSize': 1})

        # Create the managers dictionary with both old and new keys for backward compatibility
        managers = {
            # Old keys (for backward compatibility)
            'deribit': mock_deribit.return_value,
            'portfolio': mock_pm.return_value,
            'hedging': mock_hm.return_value,
            'subscription': mock_sm.return_value,
            # New keys that the CLI expects
            'deribit_client': mock_deribit.return_value,
            'portfolio_manager': mock_pm.return_value,
            'hedging_manager': mock_hm.return_value,
            'subscription_manager': mock_sm.return_value,
        }
        
        # Store the original managers for cleanup
        original_managers = dict(managers)
        
        # Patch the global managers dict
        with patch('dneutral_sniper.cli.managers', managers):
            yield managers
            
        # Clean up by restoring the original managers
        managers.clear()
        managers.update(original_managers)

# Tests

def test_cli_help(runner):
    """Test that the CLI shows help."""
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert "DNEUTRAL SNIPER - Multi-Portfolio Delta Hedging System" in result.output

def test_create_portfolio(runner, mock_managers):
    """Test creating a new portfolio."""
    result = runner.invoke(cli, ['create-portfolio', 'test_port', '--underlying', 'BTC', '--balance', '10000'])
    assert result.exit_code == 0
    mock_managers['portfolio'].create_portfolio.assert_called_once_with(
        portfolio_id='test_port',
        underlying='BTC',
        initial_balance=10000.0
    )

def test_list_portfolios(runner, mock_managers):
    """Test listing portfolios."""
    # Set up the mocks
    mock_portfolio = MagicMock()
    mock_portfolio.underlying = 'BTC'
    mock_portfolio.initial_balance = 10000.0
    mock_portfolio.options = {}

    # Get the mock instances
    portfolio_mock = mock_managers['portfolio_manager']
    hedging_mock = mock_managers['hedging_manager']

    # Configure the return values for the simple list view
    portfolio_mock.list_portfolios = AsyncMock(return_value=["test_portfolio"])
    portfolio_mock.get_portfolio = AsyncMock(return_value=mock_portfolio)
    
    # Configure the hedging stats to return a dict directly (not a Future)
    hedging_stats = {
        'current_delta': 0.1,
        'hedge_count': 5,
        'last_hedge_time': '2023-01-01 12:00:00',
        'is_hedging': True
    }
    hedging_mock.get_hedger_stats = AsyncMock(return_value=hedging_stats)

    # Create a synchronous wrapper for asyncio.run that properly handles async functions
    def sync_run(coro):
        if asyncio.iscoroutine(coro):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()
        return coro

    # Create a function to run the test with the given command and assertions
    def run_test(cmd, expected_outputs, check_detailed=False):
        # Reset mocks for this test case
        portfolio_mock.list_portfolios.reset_mock()
        portfolio_mock.get_portfolio.reset_mock()
        hedging_mock.get_hedger_stats.reset_mock()

        with patch('dneutral_sniper.cli.managers', mock_managers), \
             patch('dneutral_sniper.cli.asyncio.run', side_effect=sync_run), \
             patch('dneutral_sniper.cli.initialize_managers') as mock_init:

            mock_init.return_value = None

            # Run the command
            result = runner.invoke(cli, cmd)


            # Verify the output
            assert result.exit_code == 0, f"Unexpected error: {result.output}"
            for expected in expected_outputs:
                assert expected in result.output, f"Expected '{expected}' in output: {result.output}"

            # Verify the mocks were called correctly
            portfolio_mock.list_portfolios.assert_called_once()
            
            if check_detailed:
                portfolio_mock.get_portfolio.assert_called_once_with('test_portfolio')
                hedging_mock.get_hedger_stats.assert_called_once_with('test_portfolio')
            else:
                portfolio_mock.get_portfolio.assert_not_called()
                hedging_mock.get_hedger_stats.assert_not_called()

    # Test the simple list-portfolios command
    run_test(
        ['list-portfolios'],
        expected_outputs=['test_portfolio']
    )
    
    # Test the detailed list-portfolios command
    run_test(
        ['list-portfolios', '--detailed'],
        expected_outputs=['test_portfolio', '0.1000 BTC'],
        check_detailed=True
    )

def test_start_hedging(runner, mock_managers):
    """Test starting hedging for a portfolio."""
    result = runner.invoke(cli, ['start-hedging', 'test_portfolio'])
    assert result.exit_code == 0
    mock_managers['hedging_manager'].start.assert_called_once()

def test_stop_hedging(runner, mock_managers):
    """Test stopping hedging for a portfolio."""
    # Get the mock hedging manager
    hedging_mock = mock_managers['hedging_manager']
    
    # Configure the mock to return None for stop
    hedging_mock.stop.return_value = None
    
    # Create a synchronous wrapper for asyncio.run
    def sync_run(coro):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    # Patch the managers and asyncio.run
    with patch('dneutral_sniper.cli.managers', mock_managers), \
         patch('dneutral_sniper.cli.asyncio.run', side_effect=sync_run), \
         patch('dneutral_sniper.cli.initialize_managers') as mock_init:
        
        mock_init.return_value = None
        
        # Test the stop-hedging command
        result = runner.invoke(cli, ['stop-hedging', 'test_portfolio'])
        
        # Verify the output and mocks
        assert result.exit_code == 0, f"Unexpected error: {result.output}"
        hedging_mock.stop.assert_called_once()
        assert "Stopped delta hedging for portfolio 'test_portfolio'" in result.output

def test_portfolio_status(runner, mock_managers):
    """Test getting status of a portfolio."""
    # Get the mock portfolio manager
    portfolio_mock = mock_managers['portfolio_manager']
    
    # Configure the mock portfolio
    mock_portfolio = MagicMock()
    mock_portfolio.underlying = 'BTC'
    mock_portfolio.initial_balance = 10000.0
    mock_portfolio.options = {}
    portfolio_mock.get_portfolio.return_value = mock_portfolio
    
    # Create a synchronous wrapper for asyncio.run
    def sync_run(coro):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    # Patch the managers and asyncio.run
    with patch('dneutral_sniper.cli.managers', mock_managers), \
         patch('dneutral_sniper.cli.asyncio.run', side_effect=sync_run), \
         patch('dneutral_sniper.cli.initialize_managers') as mock_init:
        
        mock_init.return_value = None
        
        # Test the portfolio-status command
        result = runner.invoke(cli, ['portfolio-status', 'test_portfolio'])
        
        # Verify the output and mocks
        assert result.exit_code == 0, f"Unexpected error: {result.output}"
        assert "Portfolio: test_portfolio" in result.output
        assert "Underlying: BTC" in result.output
        assert "Initial Balance: $10,000.00" in result.output
        portfolio_mock.get_portfolio.assert_called_once_with('test_portfolio')

def test_monitor(runner, mock_managers):
    """Test the monitor command with keyboard interrupt."""
    # Create a mock for asyncio.run that will be called by the CLI
    mock_run = MagicMock(side_effect=KeyboardInterrupt)
    
    # Create an async mock for the shutdown function
    mock_shutdown = AsyncMock()
    
    # Patch the necessary functions
    with patch('dneutral_sniper.cli.asyncio.run', mock_run), \
         patch('dneutral_sniper.cli.initialize_managers') as mock_init, \
         patch('dneutral_sniper.cli.shutdown', mock_shutdown):
        
        # Mock the managers to avoid actual initialization
        mock_init.return_value = None
        
        # Import the monitor function to patch it
        from dneutral_sniper.cli import monitor
        
        # Create a mock for the monitor function
        mock_monitor = AsyncMock(side_effect=KeyboardInterrupt)
        
        # Replace the monitor function with our mock
        with patch('dneutral_sniper.cli.monitor', mock_monitor):
            # Invoke the CLI command and expect it to exit with status 1
            result = runner.invoke(cli, ['monitor'], catch_exceptions=SystemExit)
            
            # Verify the results
            if isinstance(result, int):
                # If the result is an exit code
                assert result == 1, f"Expected exit code 1, got {result}"
            else:
                # If the result is a ClickResult object
                assert result.exit_code == 1, f"Expected exit code 1, got {result.exit_code}. Output: {result.output}"
            
            # Verify the functions were called
            mock_run.assert_called_once()
            mock_init.assert_called_once()

@pytest.mark.asyncio
async def test_shutdown_handling():
    """Test that shutdown cleans up resources properly."""
    # Create a mock logger to verify log messages
    mock_logger = MagicMock()
    
    # Create mock managers with proper async methods
    mock_deribit = AsyncMock()
    mock_portfolio = AsyncMock()
    mock_subscription = AsyncMock()
    mock_hedging = AsyncMock()
    
    # Configure the mocks to return awaitables
    mock_hedging.stop = AsyncMock(return_value=None)
    mock_deribit.close = AsyncMock(return_value=None)
    
    # Create the managers dictionary
    mock_managers = {
        'deribit_client': mock_deribit,
        'portfolio_manager': mock_portfolio,
        'subscription_manager': mock_subscription,
        'hedging_manager': mock_hedging,
        # Include backward compatibility keys
        'deribit': mock_deribit,
        'portfolio': mock_portfolio,
        'subscription': mock_subscription,
        'hedging': mock_hedging
    }
    
    # Patch the managers dictionary, logger, and call shutdown
    with patch('dneutral_sniper.cli.managers', mock_managers), \
         patch('dneutral_sniper.cli.logger', mock_logger), \
         patch('dneutral_sniper.cli.initialize_managers', AsyncMock(return_value=None)) as mock_init_managers:
        
        # Call shutdown
        from dneutral_sniper.cli import shutdown
        await shutdown()
        
        # Verify cleanup methods were called
        mock_hedging.stop.assert_awaited_once()
        mock_deribit.close.assert_awaited_once()
        
        # Verify log messages
        mock_logger.info.assert_any_call("Shutting down...")
        mock_logger.info.assert_any_call("Shutdown complete")
        mock_deribit.close.assert_awaited_once()

def test_config_loading(runner, temp_config, mock_managers):
    """Test loading configuration from a file."""
    test_config = {
        "deribit": {
            "api_key": "test_key",
            "api_secret": "test_secret"
        },
        "portfolios_dir": "/tmp/portfolios"
    }
    
    # Mock the list_portfolios to return a list synchronously
    def mock_list_portfolios():
        return []
    
    # Test with our temp config file
    with patch('dneutral_sniper.cli.managers', mock_managers), \
         patch('dneutral_sniper.cli.initialize_managers') as mock_init, \
         patch('dneutral_sniper.cli.list_portfolios', side_effect=mock_list_portfolios) as mock_list:
        
        # Configure the mocks
        mock_init.return_value = None
        
        # Mock the file opening to return our test config
        with patch('builtins.open', mock_open(read_data=json.dumps(test_config))):
            # Run the command with the temp config file
            result = runner.invoke(cli, ['--config', temp_config, 'list-portfolios'])
        
        # Verify the results
        assert result.exit_code == 0, f"Unexpected error: {result.output}"
        
        # Verify initialize_managers was called with our test config
        assert mock_init.called, "initialize_managers was not called with temp config"
        args, _ = mock_init.call_args
        
        # Verify the config passed to initialize_managers matches our test config
        assert args[0] == test_config, \
            f"Expected config {test_config}, got {args[0] if args else 'no args'}"
    
    # Test with our temp config file - second test with actual file read
    with patch('dneutral_sniper.cli.managers', mock_managers), \
         patch('dneutral_sniper.cli.initialize_managers') as mock_init, \
         patch('dneutral_sniper.cli.list_portfolios'):
        
        # Configure the mocks
        mock_init.return_value = None
        
        # Run the command with the temp config file
        result = runner.invoke(cli, ['--config', temp_config, 'list-portfolios'])
        
        # Verify the results
        assert result.exit_code == 0, f"Unexpected error: {result.output}"
        
        # Verify initialize_managers was called with our test config
        assert mock_init.called, "initialize_managers was not called with temp config"
        args, _ = mock_init.call_args
        
        # Load the expected config from the temp file
        with open(temp_config, 'r') as f:
            expected_config = json.load(f)
        
        # Verify the config passed to initialize_managers matches our test config
        assert args[0] == expected_config, \
            f"Expected config {expected_config}, got {args[0] if args else 'no args'}"
