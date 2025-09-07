#!/usr/bin/env python3
# bot_testnet.py
import os
import math
import time
import threading
import requests
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from flask import Flask
from binance.client import Client

# -------------------------
# CONFIG (from .env)
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
# set USE_TESTNET to "false" if you want mainnet (be careful!)
USE_TESTNET = os.getenv("USE_TESTNET", "true").lower() in ("1", "true", "yes")

QUOTE = "USDT"

PRICE_MIN = 1.0
PRICE_MAX = 3.0
MIN_VOLUME = 3_000_000
MOVEMENT_MIN_PCT = 1.0

TRADE_USD = 7.0
SLEEP_BETWEEN_CHECKS = 60
CYCLE_DELAY = 13
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.06
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.0
BASE_SL_PCT = 2.0

MICRO_TP_PCT = 0.8
MICRO_TP_FRACTION = 0.35

ROLL_ON_RISE_PCT = 0.5
ROLL_TRIGGER_DELTA_ABS = 0.007
ROLL_TP_STEP_ABS = 0.015
ROLL_SL_STEP_ABS = 0.004
ROLL_COOLDOWN_SECONDS = 10

# -------------------------
# PRE RUN CHECKS
# -------------------------
if not API_KEY or not API_SECRET:
    print("ERROR: API_KEY/API_SECRET not set in environment (.env). Create .env with API_KEY and API_SECRET and retry.")
    print("Example .env contents:")
    print('API_KEY=your_api_key_here')
    print('API_SECRET=your_api_secret_here')
    print('USE_TESTNET=true')
    raise SystemExit(1)

# -------------------------
# INIT / GLOBALS
# -------------------------
client = Client(API_KEY, API_SECRET)

# Point client to testnet if requested
if USE_TESTNET:
    # python-binance supports changing the API_URL attribute for testnet
    client.API_URL = "https://testnet.binance.vision/api"
    print("➡️ Running in TESTNET mode (client.API_URL set to testnet).")
else:
    print("➡️ Running in MAINNET mode. Be careful!")

LAST_NOTIFY = 0
start_balance_usdt = None

TEMP_SKIP = {}  # symbol -> retry_unix_ts
SKIP_SECONDS_ON_MARKET_CLOSED = 60 * 60

# rate-limit small decimal precision
getcontext().prec = 28

# -------------------------
# REBUY / RECENT BUYS CONFIG
# -------------------------
RECENT_BUYS = {}
REBUY_COOLDOWN = 60 * 60
LOSS_COOLDOWN = 60 * 60 * 4
REBUY_MAX_RISE_PCT = 5.0

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 90

CACHE_TTL = 90

# -------------------------
# HELPERS: formatting & rounding
# -------------------------
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 1.0:
        return
    LAST_NOTIFY = now_ts
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": text}, timeout=8)
        except Exception:
            pass

def format_price(value, tick_size):
    try:
        tick = Decimal(str(tick_size))
        precision = max(0, -tick.as_tuple().exponent)
        return format(Decimal(str(value)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN), f'.{precision}f')
    except Exception:
        return f"{value:.8f}"

def format_qty(qty: float, step: float) -> str:
    try:
        if not step or float(step) == 0.0:
            return format(Decimal(str(qty)), 'f')
        q = Decimal(str(qty))
        s = Decimal(str(step))
        if s == 0:
            return format(q, 'f')
        multiples = (q // s)
        quant = multiples * s
        precision = max(0, -s.as_tuple().exponent)
        if quant < Decimal('0'):
            quant = Decimal('0')
        return format(quant, f'.{precision}f')
    except Exception:
        try:
            return str(math.floor(qty))
        except Exception:
            return "0"

def round_step(n, step):
    try:
        if not step or step == 0:
            return n
        s = float(step)
        return math.floor(n / s) * s
    except Exception:
        return n

# -------------------------
# BALANCES / FILTERS
# -------------------------
def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        bal = client.get_asset_balance(asset=asset)
        return float(bal['free'])
    except Exception:
        return 0.0

def get_filters(symbol_info):
    fs = {f.get('filterType'): f for f in symbol_info.get('filters', [])}
    lot = fs.get('LOT_SIZE')
    pricef = fs.get('PRICE_FILTER')
    min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional', None) if fs.get('MIN_NOTIONAL') else None
    return {
        'stepSize': float(lot['stepSize']) if lot else 0.0,
        'minQty': float(lot['minQty']) if lot else 0.0,
        'tickSize': float(pricef['tickSize']) if pricef else 0.0,
        'minNotional': float(min_notional) if min_notional else None
    }

# -------------------------
# TRANSFERS
# -------------------------
def send_profit_to_funding(amount, asset='USDT'):
    """
    Attempt to transfer realized profit to funding wallet.
    NOTE: On the Spot Test Network some /sapi endpoints (like universal_transfer) may not exist.
    This function will try and gracefully handle failures.
    """
    try:
        # try the common method first
        if hasattr(client, 'universal_transfer'):
            result = client.universal_transfer(
                type='MAIN_FUNDING',
                asset=asset,
                amount=str(round(amount, 6))
            )
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
            return result
        else:
            # older client naming / fallback
            result = client.transfer_spot_to_funding(asset=asset, amount=str(round(amount, 6)))
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet (fallback method).")
            return result
    except Exception as e:
        notify(f"⚠️ send_profit_to_funding failed (this is expected on testnet sometimes): {e}")
        return None

# -------------------------
# TICKER CACHE & PICKER
# -------------------------
TICKER_CACHE = None
LAST_FETCH = 0

SYMBOL_INFO_CACHE = {}
SYMBOL_INFO_TTL = 120

OPEN_ORDERS_CACHE = {'ts': 0, 'data': None}
OPEN_ORDERS_TTL = 5

def get_symbol_info_cached(symbol, ttl=SYMBOL_INFO_TTL):
    now = time.time()
    ent = SYMBOL_INFO_CACHE.get(symbol)
    if ent and now - ent[1] < ttl:
        return ent[0]
    try:
        info = client.get_symbol_info(symbol)
        SYMBOL_INFO_CACHE[symbol] = (info, now)
        return info
    except Exception as e:
        notify(f"⚠️ Failed to fetch symbol info for {symbol}: {e}")
        return None

def get_open_orders_cached(symbol=None):
    now = time.time()
    if OPEN_ORDERS_CACHE['data'] is not None and now - OPEN_ORDERS_CACHE['ts'] < OPEN_ORDERS_TTL:
        data = OPEN_ORDERS_CACHE['data']
        if symbol:
            return [o for o in data if o.get('symbol') == symbol]
        return data
    try:
        if symbol:
            data = client.get_open_orders(symbol=symbol)
        else:
            data = client.get_open_orders()
        OPEN_ORDERS_CACHE['data'] = data
        OPEN_ORDERS_CACHE['ts'] = now
        return data
    except Exception as e:
        notify(f"⚠️ Failed to fetch open orders: {e}")
        return OPEN_ORDERS_CACHE['data'] or []

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and now - LAST_FETCH < RATE_LIMIT_BACKOFF:
        return TICKER_CACHE or []
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        try:
            TICKER_CACHE = client.get_ticker()
            LAST_FETCH = now
            RATE_LIMIT_BACKOFF = 0
        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                notify(f"⚠️ Rate limit detected in get_tickers_cached, backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
            else:
                notify(f"⚠️ Failed to refresh tickers: {e}")
            return TICKER_CACHE or []
    return TICKER_CACHE

def cleanup_temp_skip():
    now = time.time()
    for s, until in list(TEMP_SKIP.items()):
        if now >= until:
            del TEMP_SKIP[s]

def cleanup_recent_buys():
    now = time.time()
    for s, info in list(RECENT_BUYS.items()):
        cd = info.get('cooldown', REBUY_COOLDOWN)
        if now >= info['ts'] + cd:
            del RECENT_BUYS[s]

# (the rest of the script: pick_coin, market buy helpers, micro TP, OCO, monitor_and_roll, trade_cycle)
# For brevity I'm keeping the rest identical to your provided logic but ensure client.API_URL set earlier.
# ... (You can paste the remainder of your previously provided functions here unchanged) ...

# -------------------------
# QUICK START: print a few test balances so you can confirm testnet
# -------------------------
if __name__ == "__main__":
    # quick sanity: fetch a few balances and server time
    try:
        server_time = client.get_server_time()
        notify(f"Connected to Binance API server time: {server_time}")
    except Exception as e:
        notify(f"⚠️ Couldn't fetch server time: {e}")

    # print sample balances for sanity
    for asset in ("USDT", "BNB", "BTC"):
        try:
            b = client.get_asset_balance(asset=asset)
            notify(f"{asset}: free={b.get('free')} locked={b.get('locked')}")
        except Exception as e:
            notify(f"Error fetching {asset}: {e}")

    # Start the bot thread (uncomment to run the full trade_cycle)
    # bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    # bot_thread.start()
    # start_flask()
    notify("Setup complete. If you want to run the live bot, uncomment bot thread/start_flask in the __main__ block.")