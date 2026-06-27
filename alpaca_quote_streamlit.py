"""
Streamlit app for live Alpaca bid/ask/last-trade prices.

Why Streamlit instead of Tkinter: on this machine, the system's old/
deprecated Tcl/Tk build was rendering windows where only the native Button
showed up - every plain tk.Label/tk.Entry stayed invisible no matter what we
tried (removing Frame containers, switching ttk->tk, grid()->pack(), forcing
explicit bg/fg colors). That points at a rendering bug deep in that old Tk
build itself. Streamlit sidesteps Tk entirely - it renders as a normal web
page in the browser, so none of that toolkit's quirks apply.

How the live updates work:
    A background thread (StreamWorker) runs the Alpaca websocket and writes
    bid/ask/last-trade values into a shared dict guarded by a lock - it
    never touches Streamlit widgets directly. The script itself re-renders
    the metrics from that shared dict and then calls st.rerun() once a
    second, which is the standard way to build a "live" auto-refreshing
    Streamlit page.

    The background thread and shared state live in st.session_state, which
    persists across reruns within the same browser tab/session, so the
    websocket connection isn't restarted every time the page refreshes.

Setup:
    pip install alpaca-py python-dotenv streamlit

.env (same folder as this file):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    streamlit run alpaca_quote_streamlit.py -- AAPL
    streamlit run alpaca_quote_streamlit.py        (defaults to AAPL; change
                                                     the ticker in the page)

Stop with Ctrl+C in the terminal that's running `streamlit run`.
"""

import os
import sys
import threading
import time
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    st.error("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY. Set them in your .env file.")
    st.stop()


class StreamWorker(threading.Thread):
    """Runs one StockDataStream for one symbol in a background thread,
    writing bid/ask/last-trade updates straight into a shared dict guarded
    by a lock. Never touches Streamlit/session_state from this thread - the
    main script thread is the only thing allowed to read `state` (under the
    lock) and render it.
    """

    def __init__(self, symbol, state, lock):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.state = state
        self.lock = lock
        # same IEX-feed entitlement constraint as the other scripts - free/
        # paper accounts only have entitlement to IEX, not SIP.
        self.stream = StockDataStream(API_KEY, SECRET_KEY, feed=DataFeed.IEX)

        async def on_quote(data):
            with self.lock:
                if data.bid_price is not None:
                    self.state["bid"] = data.bid_price
                if data.ask_price is not None:
                    self.state["ask"] = data.ask_price
                self.state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")

        async def on_trade(data):
            with self.lock:
                self.state["last_trade"] = data.price
                self.state["updated"] = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")

        self.stream.subscribe_quotes(on_quote, symbol)
        self.stream.subscribe_trades(on_trade, symbol)

    def run(self):
        try:
            self.stream.run()
        except Exception as e:
            with self.lock:
                self.state["error"] = str(e)

    def stop(self):
        try:
            self.stream.stop()
        except Exception:
            pass


def default_symbol():
    # Streamlit strips its own args before `--`, so anything after `--` on
    # the `streamlit run ... -- AAPL` command line shows up in sys.argv here.
    return sys.argv[1].strip().upper() if len(sys.argv) > 1 else "AAPL"


def fresh_state():
    return {"bid": None, "ask": None, "last_trade": None, "updated": "-", "error": None}


st.set_page_config(page_title="Alpaca Live Quotes", page_icon=":chart_with_upwards_trend:")

# session_state persists across Streamlit reruns within the same browser
# session - this is what lets the background websocket thread survive the
# once-a-second reruns instead of being restarted every time.
if "lock" not in st.session_state:
    st.session_state.lock = threading.Lock()
if "state" not in st.session_state:
    st.session_state.state = fresh_state()
if "symbol" not in st.session_state:
    st.session_state.symbol = default_symbol()
if "worker" not in st.session_state:
    st.session_state.worker = None


def start_stream(symbol):
    old_worker = st.session_state.worker
    if old_worker is not None:
        old_worker.stop()
    st.session_state.state = fresh_state()
    worker = StreamWorker(symbol, st.session_state.state, st.session_state.lock)
    worker.start()
    st.session_state.worker = worker
    st.session_state.symbol = symbol


if st.session_state.worker is None:
    start_stream(st.session_state.symbol)

st.title("Alpaca Live Quotes")

col1, col2 = st.columns([3, 1])
with col1:
    typed_symbol = st.text_input("Ticker", value=st.session_state.symbol).strip().upper()
with col2:
    st.write("")
    st.write("")
    switch_clicked = st.button("Switch", use_container_width=True)

if switch_clicked and typed_symbol and typed_symbol != st.session_state.symbol:
    start_stream(typed_symbol)
    st.rerun()

with st.session_state.lock:
    state = dict(st.session_state.state)

m1, m2, m3 = st.columns(3)
m1.metric("Bid", f"${state['bid']:.2f}" if state["bid"] is not None else "-")
m2.metric("Ask", f"${state['ask']:.2f}" if state["ask"] is not None else "-")
m3.metric("Last Trade", f"${state['last_trade']:.2f}" if state["last_trade"] is not None else "-")

st.caption(f"Updated: {state['updated']}")
if state.get("error"):
    st.error(f"Stream error: {state['error']}")
else:
    st.caption(f"Streaming {st.session_state.symbol}... (Ctrl+C in the terminal to stop)")

# Auto-refresh once a second. This re-runs the whole script from the top,
# which is the standard way to build a "live" page in Streamlit - the
# background thread above keeps running independently and just gets
# re-read from session_state on each pass.
time.sleep(1)
st.rerun()
