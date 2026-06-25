"""
Entry point that runs alpaca_quote_terminal.py and alpaca_minute_data.py
back-to-back for one ticker:

1. Pulls 30 days of 5-minute OHLCV bars and plots the matplotlib chart
   (reusing alpaca_quote_terminal.get_5min_bars / plot_historical_chart).
2. Once you close the chart window, it does NOT continue into
   alpaca_quote_terminal's live bid/ask panel (the "rectangular box" UI
   whose background websocket thread was prone to freezing on Ctrl+C).
   Instead it jumps straight to alpaca_minute_data's live OHLCV table,
   which streams new minute bars in the simpler, easy-to-Ctrl+C-quit style.

Setup:
    pip install alpaca-py python-dotenv rich matplotlib

.env (same folder as this file):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    python main.py AAPL
    python main.py        (you'll be prompted for a ticker)
"""

import sys

import alpaca_quote_terminal as qt
import alpaca_minute_data as md


def main():
    symbol = sys.argv[1].strip().upper() if len(sys.argv) > 1 else input("Enter ticker symbol: ").strip().upper()
    if not symbol:
        raise SystemExit("No ticker provided.")

    # --- Step 1: historical chart (alpaca_quote_terminal) ---
    qt.console.print(f"\nFetching 30 days of 5-minute OHLCV bars for [bold]{symbol}[/bold]...")
    df = qt.get_5min_bars(symbol, days=30)
    qt.console.print(f"Got {len(df)} bars. Plotting chart...")
    qt.plot_historical_chart(df, symbol)

    # --- Step 2: skip the bid/ask live panel entirely, go straight to ---
    # --- alpaca_minute_data's simpler live OHLCV table ---
    md.console.print(f"\nFetching last 100 one-minute OHLCV bars for [bold]{symbol}[/bold]...\n")
    minute_df = md.get_last_100_minute_bars(symbol)
    md.print_historical_table(minute_df, symbol)

    md.console.print(
        f"Streaming live minute bars for [bold]{symbol}[/bold] "
        f"(new candle every ~60s, Ctrl+C to stop)...\n"
    )
    md.stream_live_minute_bars(symbol)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
