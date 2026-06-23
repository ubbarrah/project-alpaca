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
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Missing ALPACA_API_KEY or ALPACA_SECRET_KEY. Set them in your .env file."
    )

# paper=True routes all requests to the paper trading endpoint
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)


def main():
    account = trading_client.get_account()
    print(f"Authenticated as account: {account.account_number}")
    print(f"Status: {account.status}")
    print(f"Buying power: ${account.buying_power}")
    print(f"Cash: ${account.cash}")
    print(f"Portfolio value: ${account.portfolio_value}")


if __name__ == "__main__":
    main()
