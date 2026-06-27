"""
Tkinter GUI for live Alpaca bid/ask/last-trade prices.

The websocket stream runs in a background thread (StreamWorker) and only
ever pushes dicts onto a queue.Queue - it never touches a Tkinter widget
directly, since Tkinter is not thread-safe and calling widget methods from
a non-main thread is a common cause of GUI windows that open blank or
freeze. The GUI itself runs entirely on the main thread's root.mainloop(),
and polls the queue every 250ms via root.after() to pull in new data.

Type a new ticker and press Enter (or click Switch) to restart the stream
for a different symbol without closing the window.

Setup:
    pip install alpaca-py python-dotenv
    (tkinter ships with most Python installs; on some Linux distros you may
    need to separately install it, e.g. `sudo apt install python3-tk`)

.env (same folder as this file):
    ALPACA_API_KEY=your_key_id
    ALPACA_SECRET_KEY=your_secret_key

Run:
    python alpaca_quote_gui.py AAPL
    python alpaca_quote_gui.py        (defaults to AAPL)

Close the window (or its close button) to quit.
"""

import os
import sys
import queue
import threading
import tkinter as tk
from datetime import datetime, timezone

from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Missing ALPACA_API_KEY or ALPACA_SECRET_KEY. Set them in your .env file."
    )


class StreamWorker(threading.Thread):
    """Runs one StockDataStream for one symbol in a background thread,
    pushing bid/ask/last-trade updates onto update_queue. Never touches the
    GUI directly - the main thread is the only thing allowed to update
    Tkinter widgets.
    """

    def __init__(self, symbol, update_queue):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.update_queue = update_queue
        # same IEX-feed entitlement constraint as the other scripts -
        # free/paper accounts only have entitlement to IEX, not SIP.
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
            self.update_queue.put({"type": "error", "message": str(e)})

    def stop(self):
        try:
            self.stream.stop()
        except Exception:
            pass


class QuoteGUI:
    def __init__(self, root, symbol):
        self.root = root
        self.symbol = symbol
        self.update_queue = queue.Queue()
        self.worker = None

        root.title(f"Alpaca Live Quotes - {symbol}")
        root.geometry("380x340")
        root.protocol("WM_DELETE_WINDOW", self.on_close)

        # This old/deprecated system Tk build on macOS partially follows
        # Dark Mode: it themes the window background dark, but plain
        # tk.Label/tk.Entry widgets still default their fg to a
        # system-computed color that ends up rendering as black-on-black
        # (or otherwise invisible) on that dark background. The Switch
        # button always looked fine because native macOS button chrome is
        # drawn by the OS, not colored by Tk. Fix: stop trusting any
        # default colors at all - force an explicit bg/fg on root and on
        # every single widget below.
        BG = "white"
        FG = "black"
        root.configure(bg=BG)

        self.symbol_var = tk.StringVar(value=symbol)
        self.bid_var = tk.StringVar(value="-")
        self.ask_var = tk.StringVar(value="-")
        self.trade_var = tk.StringVar(value="-")
        self.updated_var = tk.StringVar(value="-")
        self.status_var = tk.StringVar(value="Starting...")

        big_font = ("Helvetica", 22, "bold")
        label_font = ("Helvetica", 12)

        tk.Label(root, text="Ticker:", bg=BG, fg=FG).pack(anchor="w", padx=16, pady=(16, 0))
        entry = tk.Entry(root, textvariable=self.symbol_var, width=12, bg=BG, fg=FG,
                          insertbackground=FG, highlightthickness=1, highlightbackground="gray")
        entry.pack(anchor="w", padx=16, pady=(0, 4))
        entry.bind("<Return>", self.on_switch_symbol)
        tk.Button(root, text="Switch", command=self.on_switch_symbol, bg=BG, fg=FG).pack(anchor="w", padx=16, pady=(0, 12))

        tk.Label(root, text="Bid", font=label_font, anchor="w", bg=BG, fg=FG).pack(fill="x", padx=16, pady=(12, 0))
        tk.Label(root, textvariable=self.bid_var, font=big_font, fg="#1a7f37", bg=BG, anchor="w").pack(fill="x", padx=16)

        tk.Label(root, text="Ask", font=label_font, anchor="w", bg=BG, fg=FG).pack(fill="x", padx=16, pady=(12, 0))
        tk.Label(root, textvariable=self.ask_var, font=big_font, fg="#c0392b", bg=BG, anchor="w").pack(fill="x", padx=16)

        tk.Label(root, text="Last Trade", font=label_font, anchor="w", bg=BG, fg=FG).pack(fill="x", padx=16, pady=(12, 0))
        tk.Label(root, textvariable=self.trade_var, font=big_font, bg=BG, fg=FG, anchor="w").pack(fill="x", padx=16)

        tk.Label(root, textvariable=self.updated_var, fg="gray", bg=BG, anchor="w").pack(fill="x", padx=16, pady=(16, 0))
        tk.Label(root, textvariable=self.status_var, fg="gray", bg=BG, anchor="w").pack(fill="x", padx=16)

        self.start_stream(symbol)
        self.poll_queue()

        # Bring the window to the front - on some platforms/window managers
        # a freshly created Tk window can open behind other apps (this was
        # the root cause of an earlier "blank window" issue: the window was
        # actually fine, just hidden behind the terminal).
        root.after(200, self._raise_window)

    def _raise_window(self):
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.root.after(200, lambda: self.root.attributes("-topmost", False))
        self.root.focus_force()

    def start_stream(self, symbol):
        if self.worker is not None:
            self.worker.stop()
        self.bid_var.set("-")
        self.ask_var.set("-")
        self.trade_var.set("-")
        self.updated_var.set("-")
        self.status_var.set(f"Streaming {symbol}...")
        self.worker = StreamWorker(symbol, self.update_queue)
        self.worker.start()

    def on_switch_symbol(self, event=None):
        symbol = self.symbol_var.get().strip().upper()
        if not symbol or symbol == self.symbol:
            return
        self.symbol = symbol
        self.root.title(f"Alpaca Live Quotes - {symbol}")
        self.start_stream(symbol)

    def poll_queue(self):
        try:
            while True:
                msg = self.update_queue.get_nowait()
                if msg["type"] == "error":
                    self.status_var.set(f"Error: {msg['message']}")
                elif msg["type"] == "quote":
                    now = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
                    if msg["bid"] is not None:
                        self.bid_var.set(f"${msg['bid']:.2f}")
                    if msg["ask"] is not None:
                        self.ask_var.set(f"${msg['ask']:.2f}")
                    self.updated_var.set(f"Updated: {now}")
                elif msg["type"] == "trade":
                    now = datetime.now(timezone.utc).astimezone().strftime("%I:%M:%S %p")
                    self.trade_var.set(f"${msg['price']:.2f}")
                    self.updated_var.set(f"Updated: {now}")
        except queue.Empty:
            pass

        # Poll again in 250ms - this is the Tkinter-safe way to bring data
        # from the background websocket thread onto the main thread; never
        # call widget methods directly from StreamWorker's thread.
        self.root.after(250, self.poll_queue)

    def on_close(self):
        if self.worker is not None:
            self.worker.stop()
        self.root.destroy()


def run_quote_gui(symbol):
    root = tk.Tk()
    QuoteGUI(root, symbol)
    root.mainloop()


if __name__ == "__main__":
    SYMBOL = sys.argv[1].strip().upper() if len(sys.argv) > 1 else "AAPL"
    run_quote_gui(SYMBOL)
