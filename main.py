"""
Entry point that runs alpaca_quote_terminal.py, alpaca_minute_data.py, and
alpaca_quote_streamlit.py back-to-back for one ticker:

1. Pulls 30 days of 5-minute OHLCV bars and plots the matplotlib chart
   (reusing alpaca_quote_terminal.get_5min_bars / plot_historical_chart).
2. Once you close the chart window, prints the last 100 one-minute OHLCV
   bars to the terminal (alpaca_minute_data.get_last_100_minute_bars).
3. Launches a Streamlit web app showing live bid/ask/last-trade prices
   (alpaca_quote_streamlit.py) and opens it in your browser.

   Streamlit is used here instead of a Tkinter GUI because this machine's
   system Tcl/Tk build had a rendering bug where only native widgets like
   buttons would show up - text labels and entry boxes stayed invisible no
   matter what was tried. Streamlit renders as a normal web page instead,
   so that whole class of problem doesn't apply. It's launched as its own
   subprocess (`streamlit run ...`), which also conveniently sidesteps the
   separate macOS Tk/Cocoa NSApplication-singleton crash that came up when
   a Tkinter GUI was tried after the matplotlib chart in the same process.

Setup:
    pip install alpaca-py python-dotenv rich matplotlib streamlit

.env (same folder as this file):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    python main.py AAPL
    python main.py        (you'll be prompted for a ticker)

Stop with Ctrl+C in the terminal once the Streamlit app is running.
"""

import os
import sys
import subprocess

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

    # --- Step 2: historical 1-minute OHLCV table (alpaca_minute_data) ---
    md.console.print(f"\nFetching last 100 one-minute OHLCV bars for [bold]{symbol}[/bold]...\n")
    minute_df = md.get_last_100_minute_bars(symbol)
    md.print_historical_table(minute_df, symbol)

    # --- Step 3: live bid/ask/last-trade in a Streamlit web app ---
    # `streamlit run` starts a local web server and opens the app in your
    # default browser. This call blocks (like the old GUI window did) until
    # you Ctrl+C it in the terminal.
    md.console.print(
        f"Opening live bid/ask web app for [bold]{symbol}[/bold] - Ctrl+C in this "
        f"terminal to stop...\n"
    )
    streamlit_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alpaca_quote_streamlit.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", streamlit_script, "--", symbol])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
