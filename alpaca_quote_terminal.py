"""
Terminal UI for live Alpaca bid/ask/last-trade quotes (rich, not Tkinter).

Before streaming live quotes for a symbol, it first downloads at least 30
days of 5-minute OHLCV bars (most recent 1000 bars) and plots them with
matplotlib - price on the y-axis, time on the x-axis - so you see context
before the live feed kicks in. Close the chart window to continue to live
bid/ask/last-trade streaming.

Type a ticker, watch bid, ask, and last trade price update live. Type a new
ticker at any time and press Enter to switch symbols (re-runs the chart step
for the new symbol, then resumes live streaming).

Setup:
    pip install alpaca-py python-dotenv rich matplotlib

.env (same as other scripts):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    python alpaca_quote_terminal.py
    python alpaca_quote_terminal.py AAPL   (start streaming immediately)

Type "q" + Enter to quit. Note: stocks only update during market hours
(9:30am-4:00pm ET); outside that window the fields just won't change.
"""

import os
import sys
import queue
import signal
import threading
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

import matplotlib.pyplot as plt

from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Missing ALPACA_API_KEY or ALPACA_SECRET_KEY. Set them in your .env file."
    )

console = Console()
data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
update_queue = queue.Queue()
command_queue = queue.Queue()

state = {
    "symbol": None,
    "bid": None,
    "ask": None,
    "last_trade": None,
    "updated": "-",
    "status": "Enter a ticker to begin.",
}


class StreamWorker(threading.Thread):
    """Runs one StockDataStream for one symbol, pushing updates to update_queue."""

    def __init__(self, symbol):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.stream = StockDataStream(API_KEY, SECRET_KEY, feed=DataFeed.IEX)

        async def on_quote(data):
            update_queue.put({
                "type": "quote",
                "bid": data.bid_price,
                "ask": data.ask_price,
            })

        async def on_trade(data):
            update_queue.put({
                "type": "trade",
                "price": data.price,
            })

        self.stream.subscribe_quotes(on_quote, symbol)
        self.stream.subscribe_trades(on_trade, symbol)

    def run(self):
        try:
            self.stream.run()
        except Exception as e:
            update_queue.put({"type": "error", "message": str(e)})

    def stop(self):
        try:
            self.stream.stop()
        except Exception:
            pass


def get_5min_bars(symbol, days=30):
    """
    Fetch all 5-minute OHLCV bars over the last `days` calendar days,
    oldest-to-newest. No artificial cap on bar count - whatever the market
    actually produced in that window (roughly 2000-2500 bars for 30 days
    of regular trading hours, fewer around holidays).
    """
    now_utc = datetime.now(timezone.utc)
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=now_utc - timedelta(days=days),
        end=now_utc,
        # no `limit` - the SDK auto-paginates and returns everything in
        # the start/end window instead of capping at a fixed bar count
        feed=DataFeed.IEX,
    )
    bars = data_client.get_stock_bars(request)
    df = bars.df.xs(symbol, level=0)
    df = df.sort_index(ascending=True)  # chronological order for plotting
    return df


def plot_historical_chart(df, symbol):
    if df.empty:
        console.print(f"[yellow]No 5-minute bar data returned for {symbol} - skipping chart.[/yellow]")
        return

    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=(12, 7), sharex=True, gridspec_kw={"height_ratios": [3, 1]}
    )

    ax_price.plot(df.index, df["close"], color="tab:blue", linewidth=1.2, label="Close")
    ax_price.fill_between(df.index, df["low"], df["high"], color="tab:blue", alpha=0.15, label="High-Low range")
    ax_price.set_ylabel("Price ($)")
    ax_price.set_title(f"{symbol} - Last {len(df)} 5-Minute Bars (OHLCV)")
    ax_price.legend(loc="upper left")
    ax_price.grid(alpha=0.3)

    ax_vol.bar(df.index, df["volume"], width=0.003, color="gray")
    ax_vol.set_ylabel("Volume")
    ax_vol.set_xlabel("Date / Time")

    fig.autofmt_xdate()
    plt.tight_layout()
    console.print("[dim]Close the chart window to continue to live bid/ask streaming...[/dim]")
    plt.show()

    # matplotlib's interactive backends (TkAgg included) temporarily take
    # over SIGINT - and the signal "wakeup fd" used to interrupt blocking
    # event loops - while a chart window is open, so that Ctrl+C can close
    # the plot (see matplotlib.backend_bases._allow_interrupt). It's meant to
    # hand control back to whatever was there before once the window closes,
    # but that hand-back doesn't always leave things clean, which is the
    # likely reason Ctrl+C stops working in whatever runs after this chart
    # (e.g. the live data view in main.py) even though that same code closes
    # fine with Ctrl+C when run on its own, without a chart in front of it.
    # Explicitly reset both back to plain Python defaults here so downstream
    # code gets a clean slate to install its own handler on.
    try:
        signal.set_wakeup_fd(-1)
    except (ValueError, OSError):
        pass
    signal.signal(signal.SIGINT, signal.default_int_handler)
    plt.close("all")


def input_reader():
    """Reads tickers from stdin in a background thread so it doesn't block the live display."""
    while True:
        try:
            line = input()
        except EOFError:
            break
        line = line.strip()
        if line:
            command_queue.put(line)


def build_panel():
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()

    table.add_row("Symbol:", state["symbol"] or "-")
    table.add_row("Bid:", f"${state['bid']:.2f}" if state["bid"] is not None else "-")
    table.add_row("Ask:", f"${state['ask']:.2f}" if state["ask"] is not None else "-")
    table.add_row("Last Trade:", f"${state['last_trade']:.2f}" if state["last_trade"] is not None else "-")
    table.add_row("Updated:", state["updated"])

    return Panel(
        table,
        title="Alpaca Live Quotes",
        subtitle=state["status"],
        subtitle_align="left",
    )


def switch_symbol(symbol, worker_holder):
    if worker_holder["worker"] is not None:
        worker_holder["worker"].stop()

    console.print(f"\nFetching 30 days of 5-minute OHLCV bars for [bold]{symbol}[/bold]...")
    df = get_5min_bars(symbol, days=30)
    console.print(f"Got {len(df)} bars. Plotting chart...")
    plot_historical_chart(df, symbol)

    state["symbol"] = symbol
    state["bid"] = None
    state["ask"] = None
    state["last_trade"] = None
    state["updated"] = "-"
    state["status"] = f"Streaming {symbol}... (type another ticker to switch, 'q' to quit)"

    worker = StreamWorker(symbol)
    worker.start()
    worker_holder["worker"] = worker


def main():
    worker_holder = {"worker": None}

    threading.Thread(target=input_reader, daemon=True).start()

    initial_symbol = sys.argv[1].strip().upper() if len(sys.argv) > 1 else None
    if initial_symbol:
        switch_symbol(initial_symbol, worker_holder)
    else:
        console.print("[dim]Type a ticker and press Enter to start streaming (e.g. AAPL). Type 'q' to quit.[/dim]")

    with Live(build_panel(), refresh_per_second=4, console=console) as live:
        while True:
            # handle new ticker / quit commands
            try:
                while True:
                    cmd = command_queue.get_nowait()
                    if cmd.lower() == "q":
                        if worker_holder["worker"] is not None:
                            worker_holder["worker"].stop()
                        return
                    switch_symbol(cmd.upper(), worker_holder)
            except queue.Empty:
                pass

            # handle live data updates
            try:
                while True:
                    msg = update_queue.get_nowait()
                    if msg["type"] == "error":
                        state["status"] = f"Error: {msg['message']}"
                    elif msg["type"] == "quote":
                        if msg["bid"] is not None:
                            state["bid"] = msg["bid"]
                        if msg["ask"] is not None:
                            state["ask"] = msg["ask"]
                        state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
                    elif msg["type"] == "trade":
                        state["last_trade"] = msg["price"]
                        state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
            except queue.Empty:
                pass

            live.update(build_panel())
            threading.Event().wait(0.25)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass