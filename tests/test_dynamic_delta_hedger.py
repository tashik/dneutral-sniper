import pytest
from unittest.mock import AsyncMock, MagicMock
from dneutral_sniper.dynamic_delta_hedger import DynamicDeltaHedger, HedgerConfig
from dneutral_sniper.portfolio import Portfolio
from dneutral_sniper.deribit_client import DeribitWebsocketClient
from dneutral_sniper.options import OptionModel
from dneutral_sniper.models import OptionType, VanillaOption
import numpy as np
from datetime import datetime, timedelta

class DummyDeribitClient:
    def __init__(self):
        self.price_callback = None
        self.last_price = None
        # Simulate the price_iv_cache structure
        self.price_iv_cache = {}
    def set_price_callback(self, cb):
        self.price_callback = cb
    def get_price_iv(self, instrument_name):
        # Simulate cache lookup
        entry = self.price_iv_cache.get(instrument_name)
        if entry:
            return entry.get("mark_price"), entry.get("iv")
        return None, None
    async def get_instrument_mark_price_and_iv(self, instrument_name):
        # Simulate async fallback fetch
        return 100.0, 0.4

class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__()
        self.initial_usd_hedged = True
        self.futures_position = 0.0
        self.last_hedge_price = 100.0
        self.hedge_calls = []
        self._options = []
    def update_futures_position(self, usd_qty, price):
        self.hedge_calls.append((usd_qty, price))
        self.futures_position += usd_qty
        self.last_hedge_price = price
    def list_options(self):
        return self._options

@pytest.mark.asyncio
async def test_option_model_net_delta_various_portfolios():
    """
    Test OptionModel.calculate_portfolio_net_delta for:
    1. Only options
    2. Options + futures
    3. Only futures
    """
    # Setup
    portfolio = Portfolio()
    dummy_client = DummyDeribitClient()
    option_model = OptionModel(portfolio, deribit_client=dummy_client)
    price = 10000.0
    vol = 0.5
    # 1. Only options: 1 BTC call, ATM
    expiry = datetime.now() + timedelta(days=7)
    call = VanillaOption(
        instrument_name="BTC-30JUN24-10000-C",
        option_type=OptionType.CALL,
        strike=10000.0,
        expiry=expiry,
        quantity=1.0,
        underlying="BTC"
    )
    portfolio.add_option(call)
    # Populate the dummy cache with a realistic BTC-denominated option price
    dummy_client.price_iv_cache[call.instrument_name] = {"mark_price": 0.05, "iv": 0.5}
    net_delta = await option_model.calculate_portfolio_net_delta(price, vol)
    d1 = option_model.bs_model.calculate_d1(
        S=price,
        K=call.strike,
        T=(call.expiry - datetime.now()).total_seconds() / (365 * 24 * 3600),
        r=0.0,
        sigma=vol
    )
    call_delta = option_model.bs_model.calculate_delta(call.option_type, d1)
    # ATM call delta ~0.5 BTC, net delta = delta*qty - mark_price*qty = 0.5 - 0.05 = 0.45
    assert abs(net_delta - (call_delta - 0.05)) < 0.01

    # 2. Options + futures: add +10000 USD futures (1 BTC)
    portfolio.futures_position = 10000.0
    net_delta2 = await option_model.calculate_portfolio_net_delta(price, vol)
    # Should be call_delta - 0.05 + 1 = 1.45
    assert abs(net_delta2 - 1 - (call_delta - 0.05)) < 0.01
    # 3. Only futures: remove options
    portfolio.options = {}
    net_delta3 = await option_model.calculate_portfolio_net_delta(price, vol)
    assert abs(net_delta3 - 1.0) < 1e-6

@pytest.mark.asyncio
async def test_hedger_basic_delta_zero():
    config = HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="absolute",
        ddh_step_size=1,
        instrument_name="BTC-PERPETUAL",
        volatility=0.4,
        price_check_interval=0.1
    )
    portfolio = DummyPortfolio()
    client = DummyDeribitClient()
    hedger = DynamicDeltaHedger(config, portfolio, client)
    # Simulate price callback
    if client.price_callback:
        client.price_callback("BTC-PERPETUAL", 100.0)
    assert hedger.target_delta == 0.0
    # Should not hedge since cur_delta is None
    await hedger._execute_hedge_if_needed()
    assert portfolio.futures_position == 0.0

@pytest.mark.asyncio
async def test_hedger_triggers_hedge():
    config = HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="absolute",
        ddh_step_size=1,
        instrument_name="BTC-PERPETUAL",
        volatility=0.4,
        price_check_interval=0.1,
        min_contract_usd=10.0
    )
    portfolio = DummyPortfolio()
    client = DummyDeribitClient()
    hedger = DynamicDeltaHedger(config, portfolio, client)
    # Mock delta so that a hedge is needed
    hedger.cur_delta = -0.05  # Needs to buy (positive required_hedge)
    hedger.target_delta = 0.0
    # Patch _get_current_price to return 100
    hedger._get_current_price = AsyncMock(return_value=100.0)
    await hedger._execute_hedge_if_needed()
    # Should perform a hedge: required_hedge = 0.05, usd_qty = 5, rounded down to 0 (below min_contract_usd)
    assert portfolio.futures_position == 0.0
    # Now test with a larger required hedge
    hedger.cur_delta = -0.22  # Needs to buy 0.22 BTC
    await hedger._execute_hedge_if_needed()
    # required_hedge = 0.22, usd_qty = 22, rounded to 20
    assert portfolio.futures_position == 20.0
    assert portfolio.hedge_calls[-1] == (20.0, 100.0)

@pytest.mark.asyncio
async def test_hedger_sign_and_direction():
    config = HedgerConfig(
        ddh_min_trigger_delta=0.01,
        ddh_target_delta=0.0,
        ddh_step_mode="absolute",
        ddh_step_size=1,
        instrument_name="BTC-PERPETUAL",
        volatility=0.4,
        price_check_interval=0.1,
        min_contract_usd=10.0
    )
    portfolio = DummyPortfolio()
    client = DummyDeribitClient()
    hedger = DynamicDeltaHedger(config, portfolio, client)
    hedger._get_current_price = AsyncMock(return_value=100.0)
    # Test SELL direction (cur_delta > 0)
    hedger.cur_delta = 0.3
    hedger.target_delta = 0.0
    await hedger._execute_hedge_if_needed()
    # required_hedge = -0.3, usd_qty = -30, rounded to -30
    assert portfolio.futures_position == -30.0
    assert portfolio.hedge_calls[-1] == (-30.0, 100.0)
    # Test BUY direction (cur_delta < 0)
    hedger.cur_delta = -0.5
    await hedger._execute_hedge_if_needed()
    # required_hedge = 0.5, usd_qty = 50, rounded to 50
    assert portfolio.futures_position == 20.0  # -30 + 50
    assert portfolio.hedge_calls[-1] == (50.0, 100.0)
