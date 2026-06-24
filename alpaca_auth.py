"""
Alpaca paper trading authentication.

Setup:
    pip install alpaca-py python-dotenv

Create a .env file (same folder) with:
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Get paper trading keys from: https://app.alpaca.markets/paper/dashboard/overview
"""

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Missing ALPACA_API_KEY or ALPACA_SECRET_KEY. Set them in your .env file."
    )

# paper=True routes all requests to the paper trading endpoint
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# Market data client - same keys work for paper accounts.
# (Historical data is not a separate "paper" endpoint; it's shared.)
data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)


def get_historical_bars(symbols, lookback_days=30, timeframe=TimeFrame.Day):
    """
    Fetch historical OHLCV bars for one or more symbols.

    symbols: str or list[str], e.g. "AAPL" or ["AAPL", "MSFT"]
    lookback_days: how many days back to fetch
    timeframe: TimeFrame.Day, TimeFrame.Hour, TimeFrame.Minute, etc.

    Returns a pandas DataFrame indexed by (symbol, timestamp).
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    end = datetime.now()
    start = end - timedelta(days=lookback_days)

    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
    )

    bars = data_client.get_stock_bars(request)
    return bars.df  # pandas DataFrame: open, high, low, close, volume, trade_count, vwap


def main():
    account = trading_client.get_account()
    print(f"Authenticated as account: {account.account_number}")
    print(f"Status: {account.status}")
    print(f"Buying power: ${account.buying_power}")
    print(f"Cash: ${account.cash}")
    print(f"Portfolio value: ${account.portfolio_value}")

    print("\nFetching historical OHLCV data for AAPL (last 30 days, daily bars)...")
    df = get_historical_bars("AAPL", lookback_days=30, timeframe=TimeFrame.Day)
    print(df)


if __name__ == "__main__":
    main()