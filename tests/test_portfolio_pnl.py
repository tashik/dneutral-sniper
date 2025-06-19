import pytest
import asyncio
from dneutral_sniper.portfolio import Portfolio

@pytest.mark.asyncio
async def test_realized_pnl_on_futures_reversal():
    """
    Test that realized PnL is calculated correctly when flipping/reducing a futures position,
    and that the USD notional is converted to BTC properly in the calculation.
    """
    portfolio = Portfolio()
    entry_price = 50000.0
    close_price = 51000.0
    
    # Helper function to wait for all pending tasks
    async def wait_for_updates():
        # Give any pending tasks a chance to complete
        await asyncio.sleep(0.01)
        # Run any pending callbacks
        await asyncio.sleep(0)
    
    # Open long 10000 USD notional at 50000
    await portfolio.update_futures_position(10000.0, entry_price)
    await wait_for_updates()
    assert portfolio.futures_position == 10000.0
    assert portfolio.futures_avg_entry == entry_price
    
    # Reduce/close 4000 USD notional at 51000
    await portfolio.update_futures_position(-4000.0, close_price)
    await wait_for_updates()
    
    # Realized PnL: (51000 - 50000) * (4000 / 50000) = 1000 * 0.08 = 80 USD
    expected_pnl = (close_price - entry_price) * (4000.0 / entry_price)
    assert abs(portfolio.realized_pnl - expected_pnl) < 1e-6, f"PnL mismatch: {portfolio.realized_pnl} vs {expected_pnl}"
    
    # Flip remaining position (-7000 USD notional at 52000)
    flip_price = 52000.0
    await portfolio.update_futures_position(-13000.0, flip_price)
    await wait_for_updates()
    
    # First, close out remaining +6000 at 52000: (52000-50000)*(6000/50000) = 2000*0.12=240
    expected_pnl += (flip_price - entry_price) * (6000.0 / entry_price)
    assert abs(portfolio.realized_pnl - expected_pnl) < 1e-6, f"PnL mismatch after flip: {portfolio.realized_pnl} vs {expected_pnl}"
    
    # Now, position is -7000 USD notional at 52000
    assert abs(portfolio.futures_position + 7000.0) < 1e-6
    assert abs(portfolio.futures_avg_entry - flip_price) < 1e-6
    
    # Reduce short by +2000 USD at 53000
    reduce_price = 53000.0
    await portfolio.update_futures_position(2000.0, reduce_price)
    await wait_for_updates()
    
    # Realized PnL: (53000-52000)*(2000/52000) * -1 (because closing short)
    expected_pnl += (reduce_price - flip_price) * (2000.0 / flip_price) * -1
    assert abs(portfolio.realized_pnl - expected_pnl) < 1e-6, f"PnL mismatch after short reduce: {portfolio.realized_pnl} vs {expected_pnl}"
    
    # Final checks on position
    assert abs(portfolio.futures_position + 5000.0) < 1e-6
    assert abs(portfolio.futures_avg_entry - flip_price) < 1e-6
