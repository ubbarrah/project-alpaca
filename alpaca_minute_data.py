"""
Alpaca market data: 100 historical 1-minute OHLCV bars, then a live
bid/ask/last-trade panel (the "rectangle box" UI) instead of streaming new
minute bars into the table.

The historical section is still a full OHLCV candle per minute. The live
section switches to tick-level quotes/trades and renders them in a single
rich Panel that updates in place as new bid/ask/last-trade data arrives.

Setup:
    pip install alpaca-py python-dotenv rich

.env (same as other scripts):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    python alpaca_minute_data.py AAPL
    python alpaca_minute_data.py        (defaults to AAPL)

Ctrl+C to stop the live stream.
"""

import os
import sys
import signal
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.requests import Sort
from alpaca.data.timeframe import TimeFrame
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

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

SYMBOL = sys.argv[1].strip().upper() if len(sys.argv) > 1 else "AAPL"

# rolling buffer of recent minute bars, used by the historical table
recent_bars = []  # list of dicts: {timestamp, open, high, low, close, volume}

# live bid/ask/last-trade state, used by the rectangle panel
quote_state = {
    "bid": None,
    "ask": None,
    "last_trade": None,
    "updated": "-",
}


# ---------------------------------------------------------------------------
# 1. Historical OHLCV - last 100 one-minute bars
# ---------------------------------------------------------------------------

def get_last_100_minute_bars(symbol):
    # sort=Sort.DESC is the key fix: with a multi-day window and limit=100,
    # the default (ascending) sort returns the OLDEST 100 bars in the window,
    # not the most recent ones. DESC tells Alpaca to hand back the newest
    # bars first, capped at `limit`.
    # IMPORTANT: use timezone-aware UTC datetimes. A naive datetime.now()
    # gets sent without an offset and the API treats it as UTC, which
    # silently shifts the window by your local UTC offset (e.g. 4-5 hours
    # for US Eastern) and was why stale/wrong-looking bars showed up.
    now_utc = datetime.now(timezone.utc)
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=now_utc - timedelta(days=5),  # wide enough to cover 100 mins of trading
        end=now_utc,
        limit=100,
        sort=Sort.DESC,
        # Free/basic Alpaca accounts only have entitlement to the IEX feed.
        # The default feed is SIP, which throws a 403
        # ("subscription does not permit querying recent SIP data") on
        # accounts without a market data subscription.
        feed=DataFeed.IEX,
    )
    bars = data_client.get_stock_bars(request)
    df = bars.df.xs(symbol, level=0)
    # ensure newest-first regardless of how the SDK assembled the DataFrame
    df = df.sort_index(ascending=False).head(100)
    return df


def build_minute_table(symbol, bars, title_suffix=""):
    table = Table(title=f"{symbol} - Minute OHLCV{title_suffix}")
    table.add_column("Time", style="dim")
    table.add_column("Open", justify="right")
    table.add_column("High", justify="right")
    table.add_column("Low", justify="right")
    table.add_column("Close", justify="right", style="bold")
    table.add_column("Volume", justify="right")

    # bars is kept newest-first, so the first 15 entries are the most recent
    for b in bars[:15]:
        table.add_row(
            b["timestamp"].astimezone().strftime("%I:%M:%S %p"),
            f"{b['open']:.2f}",
            f"{b['high']:.2f}",
            f"{b['low']:.2f}",
            f"{b['close']:.2f}",
            f"{int(b['volume']):,}",
        )
    return table


def print_historical_table(df, symbol):
    # df is already sorted newest-first
    bars = [
        {
            "timestamp": ts.to_pydatetime(),  # plain datetime, so .astimezone() works
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
        }
        for ts, row in df.iterrows()
    ]
    recent_bars.extend(bars)  # newest-first order preserved

    console.print(build_minute_table(symbol, bars, title_suffix=f" - last {len(bars)} bars (showing 15)"))
    console.print(f"[dim](showing newest 15 of {len(bars)} one-minute bars, most recent on top)[/dim]\n")


# ---------------------------------------------------------------------------
# 2. Live - bid/ask/last-trade rectangle panel (replaces the minute-bar table)
# ---------------------------------------------------------------------------

def build_quote_panel(symbol, state, status):
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()

    table.add_row("Symbol:", symbol)
    table.add_row("Bid:", f"${state['bid']:.2f}" if state["bid"] is not None else "-")
    table.add_row("Ask:", f"${state['ask']:.2f}" if state["ask"] is not None else "-")
    table.add_row("Last Trade:", f"${state['last_trade']:.2f}" if state["last_trade"] is not None else "-")
    table.add_row("Updated:", state["updated"])

    return Panel(table, title="Alpaca Live Quotes", subtitle=status, subtitle_align="left")


def stream_live_quotes(symbol):
    # same IEX-feed entitlement constraint applies to the live websocket
    stream = StockDataStream(API_KEY, SECRET_KEY, feed=DataFeed.IEX)

    # StockDataStream.run() swallows KeyboardInterrupt internally and then
    # tries to gracefully shut down its asyncio event loop / websocket
    # connection in a `finally` block - that shutdown is what actually hangs
    # on Ctrl+C, not our own except/finally below (which never even gets a
    # chance to run, since the exception never reaches us). Installing a raw
    # SIGINT handler that hard-exits the process sidesteps that cleanup
    # entirely instead of waiting on it. This stream also runs synchronously
    # in the main thread (no background StreamWorker thread like the old
    # alpaca_quote_terminal panel had) - that threaded design was the one
    # that used to freeze on Ctrl+C.
    #
    # IMPORTANT: this handler must NOT touch `console`/`live` - rich's Live
    # display runs a background refresh thread that holds a render lock, and
    # if SIGINT lands while that lock is held, any console.print() call here
    # would block forever waiting on it, silently defeating the whole point
    # of this handler. Write straight to the raw file descriptor instead and
    # exit immediately - no locks, no rich, nothing that can hang.
    def _force_quit(sig, frame):
        os.write(2, b"\nStopping...\n")
        os._exit(0)

    signal.signal(signal.SIGINT, _force_quit)

    status = f"Streaming {symbol}... (Ctrl+C to stop)"

    with Live(
        build_quote_panel(symbol, quote_state, status),
        refresh_per_second=4,
        console=console,
    ) as live:

        async def on_quote(data):
            if data.bid_price is not None:
                quote_state["bid"] = data.bid_price
            if data.ask_price is not None:
                quote_state["ask"] = data.ask_price
            quote_state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
            live.update(build_quote_panel(symbol, quote_state, status))

        async def on_trade(data):
            quote_state["last_trade"] = data.price
            quote_state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
            live.update(build_quote_panel(symbol, quote_state, status))

        stream.subscribe_quotes(on_quote, symbol)
        stream.subscribe_trades(on_trade, symbol)

        try:
            stream.run()
        except KeyboardInterrupt:
            pass
        finally:
            stream.stop()


def main():
    console.print(f"\nFetching last 100 one-minute OHLCV bars for [bold]{SYMBOL}[/bold]...\n")
    df = get_last_100_minute_bars(SYMBOL)
    print_historical_table(df, SYMBOL)

    console.print(
        f"Streaming live bid/ask/last-trade for [bold]{SYMBOL}[/bold] (Ctrl+C to stop)...\n"
    )
    stream_live_quotes(SYMBOL)


if __name__ == "__main__":
    main()
