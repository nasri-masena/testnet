import os
import sys
import time
import math
import json
import atexit
import signal
import random
import logging
import threading
import hashlib
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask, Response
from binance.client import Client
from binance.exceptions import BinanceAPIException

# -------------------------
# CONFIG / ENV (editable)
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = os.getenv("QUOTE", "USDT")

# trading / strategy params (you can tune these via env)
TRADE_USD = float(os.getenv("TRADE_USD", "6.0"))
PRICE_MIN = float(os.getenv("PRICE_MIN", "1.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "3.0"))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "5000000"))

BASE_TP_PCT = float(os.getenv("BASE_TPCT", "2.0"))
BASE_SL_PCT = float(os.getenv("BASE_SL_PCT", "1.5"))

MICRO_TP_PCT = float(os.getenv("MICRO_TPCT", "1.0"))
MICRO_TP_FRACTION = float(os.getenv("MICRO_TP_FRACTION", "0.30"))
MICRO_MAX_WAIT = float(os.getenv("MICRO_MAX_WAIT", "18.0"))

ROLL_ON_RISE_PCT = float(os.getenv("ROLL_ON_RISE_PCT", "1.0"))
ROLL_TRIGGER_DELTA_ABS = float(os.getenv("ROLL_TRIGGER_DELTA_ABS", "0.012"))
ROLL_TP_STEP_ABS = float(os.getenv("ROLL_TP_STEP_ABS", "0.015"))
ROLL_SL_STEP_ABS = float(os.getenv("ROLL_SL_STEP_ABS", "0.004"))
ROLL_COOLDOWN_SECONDS = float(os.getenv("ROLL_COOLDOWN_SECONDS", "60"))
MAX_ROLLS_PER_POSITION = int(os.getenv("MAX_ROLLS_PER_POSITION", "3"))

SLEEP_BETWEEN_CHECKS = float(os.getenv("SLEEP_BETWEEN_CHECKS", "45"))
CYCLE_DELAY = float(os.getenv("CYCLE_DELAY", "12"))
BUY_LOCK_SECONDS = int(os.getenv("BUY_LOCK_SECONDS", "60"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "180"))
OPEN_ORDERS_TTL = int(os.getenv("OPEN_ORDERS_TTL", "45"))

CONFIRM_TRANSFERS = os.getenv("CONFIRM_TRANSFERS", "true").lower() in ("1", "true", "yes")
MIN_TRANSFER_AMOUNT = float(os.getenv("MIN_TRANSFER_AMOUNT", "1.0"))

DEDUPE_STORE = os.getenv("DEDUPE_STORE", "order_dedupe.json")
LOG_FILE = os.getenv("LOG_FILE", "cryptobot.log")

# Picker params
KLINES_5M_LIMIT = 6
KLINES_1M_LIMIT = 6
EMA_SHORT = 3
EMA_LONG = 10
RSI_PERIOD = 14
OB_DEPTH = 5
MIN_OB_IMBALANCE = 1.1
MAX_OB_SPREAD_PCT = 1.5
MIN_OB_LIQUIDITY = 3000.0
TOP_BY_24H_VOLUME = 200
REQUEST_SLEEP = float(os.getenv("REQUEST_SLEEP", "0.18"))
MIN_5M_PCT = 0.6
MIN_1M_PCT = 0.3

# notification / throttle config
NOTIFY_MIN_INTERVAL = float(os.getenv("NOTIFY_MIN_INTERVAL", "1.5"))
NOTIFY_COLLAPSE_WINDOW = float(os.getenv("NOTIFY_COLLAPSE_WINDOW", "60"))
MAX_SIMILAR_API_ERRORS = int(os.getenv("MAX_SIMILAR_API_ERRORS", "4"))
API_ERROR_SUPPRESS_WINDOW = float(os.getenv("API_ERROR_SUPPRESS_WINDOW", "40"))

# -------------------------
# logging
# -------------------------
logger = logging.getLogger("cryptobot")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)
fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
fh.setFormatter(fmt)
logger.addHandler(fh)

# -------------------------
# notify: dedupe + collapse + rate-limit
# -------------------------
NOTIFY_CACHE = {}   # message -> {'count', 'first_ts', 'last_ts'}
API_ERR_CACHE = {}  # err_msg -> {'count','first_ts','last_ts'}

def _send_notify_now(text: str):
    now_ts = datetime.utcnow().isoformat()
    payload = f"[{now_ts}] {text}"
    logger.info(payload)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": payload}, timeout=6)
        except Exception as e:
            logger.debug(f"telegram notify failed: {e}")

def _flush_notify_cache(force=False):
    """
    Flush suppressed messages when window expires or force=True.
    For each cached message, if count>1 produce summary; if single and expired, send.
    """
    now = time.time()
    for msg, meta in list(NOTIFY_CACHE.items()):
        count = meta.get('count', 0)
        first = meta.get('first_ts', now)
        last = meta.get('last_ts', now)
        if force or (now - first >= NOTIFY_COLLAPSE_WINDOW):
            if count > 1:
                _send_notify_now(f"(x{count}) [suppressed {int(now-first)}s] {msg}")
            else:
                _send_notify_now(msg)
            del NOTIFY_CACHE[msg]

def notify(msg: str):
    """
    Rate-limited deduping notify:
      - If same msg repeats within collapse window, increment counter and suppress.
      - Sends immediate only if last actual send older than NOTIFY_MIN_INTERVAL.
      - Periodic flush of suppressed messages by caller could be done via _flush_notify_cache().
    """
    try:
        now = time.time()
        # already suppressed?
        meta = NOTIFY_CACHE.get(msg)
        if meta:
            meta['count'] = meta.get('count', 1) + 1
            meta['last_ts'] = now
            NOTIFY_CACHE[msg] = meta
            # do not send repeated identical messages immediately
            return
        # new message: flush other expired/suppressed items first (non-blocking)
        _flush_notify_cache()
        # track this msg
        NOTIFY_CACHE[msg] = {'count': 1, 'first_ts': now, 'last_ts': now}
        # send immediately if last send far enough
        last_send = getattr(notify, "_last_send_ts", 0.0)
        if now - last_send >= NOTIFY_MIN_INTERVAL:
            _send_notify_now(msg)
            notify._last_send_ts = now
        # else keep in cache to be flushed later
    except Exception:
        logger.exception("notify failed")

# helper to condense API errors (avoid spam)
def _maybe_notify_api_error(err_msg: str):
    try:
        now = time.time()
        meta = API_ERR_CACHE.get(err_msg)
        if not meta:
            API_ERR_CACHE[err_msg] = {'count': 1, 'first_ts': now, 'last_ts': now}
            # first occurrence -> send
            notify(err_msg)
            return
        meta['count'] += 1
        meta['last_ts'] = now
        API_ERR_CACHE[err_msg] = meta
        # send compact summary occasionally
        if meta['count'] % MAX_SIMILAR_API_ERRORS == 0:
            notify(f"(api err x{meta['count']}) {err_msg}")
        # otherwise suppress
    except Exception:
        logger.exception("api err notify failed")

# -------------------------
# init & globals
# -------------------------
getcontext().prec = 28
if not API_KEY or not API_SECRET:
    logger.critical("API_KEY / API_SECRET must be set in environment. Aborting.")
    raise SystemExit(1)

client = Client(API_KEY, API_SECRET)  # live

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BASE_SLEEP = 90
RATE_LIMIT_BACKOFF_MAX = 300

TICKER_CACHE = None
LAST_FETCH = 0
SYMBOL_INFO_CACHE = {}
SYMBOL_INFO_TTL = 120

OPEN_ORDERS_CACHE = {'ts': 0, 'data': None, 'lock': threading.Lock()}

TEMP_SKIP = {}
RECENT_BUYS = {}
LAST_ORDER_HASH = {}
STATE_LOCK = threading.RLock()
CACHE_LOCK = threading.RLock()

# dedupe persistence
def _load_dedupe():
    try:
        if os.path.exists(DEDUPE_STORE):
            with open(DEDUPE_STORE, "r") as f:
                data = json.load(f)
            now = time.time()
            with STATE_LOCK:
                for k, v in data.items():
                    if isinstance(v, list) and len(v) == 2 and now - v[1] < 3600*24:
                        LAST_ORDER_HASH[k] = (v[0], v[1])
            notify("✅ Dedupe loaded")
    except Exception as e:
        logger.debug(f"dedupe load failed: {e}")

def _save_dedupe():
    try:
        with STATE_LOCK:
            tmp = {k: [v[0], v[1]] for k, v in LAST_ORDER_HASH.items()}
        with open(DEDUPE_STORE + ".tmp", "w") as f:
            json.dump(tmp, f)
        os.replace(DEDUPE_STORE + ".tmp", DEDUPE_STORE)
        logger.debug("Dedupe saved")
    except Exception as e:
        logger.debug(f"dedupe save failed: {e}")

_load_dedupe()
atexit.register(_save_dedupe)

# -------------------------
# helpers & safe API wrapper
# -------------------------
def _clean_params(d: dict):
    out = {}
    for k, v in (d or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        if isinstance(v, (int, float, Decimal)):
            out[k] = str(v)
        else:
            out[k] = v
    return out

def _hash_params(params: dict) -> str:
    s = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(s.encode()).hexdigest()

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
        multiples = (q // s)
        quant = multiples * s
        precision = max(0, -s.as_tuple().exponent)
        if quant < Decimal('0'):
            quant = Decimal('0')
        return format(quant, f'.{precision}f')
    except Exception:
        return "0"

def safe_api_call(fn, *args, retries=3, backoff=0.25, **kwargs):
    """
    Centralized wrapper with rate-limit handling and suppressed repeated error notifications.
    Always prefer keyword args for client methods (python-binance compatibility).
    """
    global RATE_LIMIT_BACKOFF
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except BinanceAPIException as e:
            err = str(e)
            last_err = err
            # malformed param -> do not retry
            if 'code=-1102' in err or 'Mandatory parameter' in err or "was empty/null" in err:
                _maybe_notify_api_error(f"⚠️ Binance malformed params (no-retry): {err}")
                # log payload snippet if present
                try:
                    payload = kwargs.get('params') or {k: v for k, v in kwargs.items() if k not in ('api_key', 'api_secret')}
                    logger.debug(f"malformed payload snippet: {json.dumps(payload, default=str)[:1000]}")
                except Exception:
                    pass
                return None
            # rate-limit detection
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                _maybe_notify_api_error(f"❗ Rate-limit detected: backing off {RATE_LIMIT_BACKOFF}s ({err})")
                time.sleep(RATE_LIMIT_BACKOFF)
                return None
            # other API errors - log debug and notify compactly
            logger.debug(f"BinanceAPIException (attempt {attempt}/{retries}): {err}")
            _maybe_notify_api_error(f"⚠️ BinanceAPIException (attempt {attempt}/{retries}): {err}")
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            last_err = str(e)
            logger.debug(f"API call error (attempt {attempt}/{retries}): {e}")
            _maybe_notify_api_error(f"⚠️ API error (attempt {attempt}/{retries}): {e}")
            time.sleep(backoff * attempt)
            continue
    _maybe_notify_api_error(f"❌ API failed after {retries} attempts: {last_err}")
    return None

def exec_api_method(name: str, **kwargs):
    fn = getattr(client, name, None)
    if not fn:
        notify(f"❌ client has no method {name}")
        return None
    return safe_api_call(fn, **kwargs)

def place_order_idempotent(api_method: str, params: dict, dedupe_ttl: int = 10):
    params_clean = _clean_params(params)
    h = _hash_params(params_clean)
    sym = params_clean.get('symbol', 'GLOBAL')
    now = time.time()
    with STATE_LOCK:
        prev = LAST_ORDER_HASH.get(sym)
        if prev and prev[0] == h and now - prev[1] < dedupe_ttl:
            notify(f"⚠️ Skipping duplicate order for {sym} (within {dedupe_ttl}s).")
            return None
        resp = exec_api_method(api_method, **params_clean)
        if resp is not None:
            LAST_ORDER_HASH[sym] = (h, now)
            _save_dedupe()
        return resp

# -------------------------
# caches
# -------------------------
def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH
    now = time.time()
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        res = safe_api_call(client.get_ticker)
        if res:
            with CACHE_LOCK:
                TICKER_CACHE = res
                LAST_FETCH = now
        else:
            notify("⚠️ get_tickers_cached failed - using cache")
    return TICKER_CACHE or []

def get_symbol_info_cached(symbol, ttl=SYMBOL_INFO_TTL):
    now = time.time()
    with CACHE_LOCK:
        ent = SYMBOL_INFO_CACHE.get(symbol)
        if ent and now - ent[1] < ttl:
            return ent[0]
    info = safe_api_call(client.get_symbol_info, symbol=symbol)
    if info:
        with CACHE_LOCK:
            SYMBOL_INFO_CACHE[symbol] = (info, now)
    return info

def get_open_orders_cached(symbol=None):
    now = time.time()
    with OPEN_ORDERS_CACHE['lock']:
        if OPEN_ORDERS_CACHE['data'] is not None and now - OPEN_ORDERS_CACHE['ts'] < OPEN_ORDERS_TTL:
            data = OPEN_ORDERS_CACHE['data']
            if symbol:
                return [o for o in data if o.get('symbol') == symbol]
            return data
    data = safe_api_call(client.get_open_orders, symbol=symbol) if symbol else safe_api_call(client.get_open_orders)
    if data is None:
        with OPEN_ORDERS_CACHE['lock']:
            return OPEN_ORDERS_CACHE['data'] or []
    with OPEN_ORDERS_CACHE['lock']:
        OPEN_ORDERS_CACHE['data'] = data
        OPEN_ORDERS_CACHE['ts'] = now
    return data

# -------------------------
# trading helpers
# -------------------------
def get_free_usdt():
    try:
        res = safe_api_call(client.get_asset_balance, asset=QUOTE)
        if not res:
            return 0.0
        return float(res.get('free') or 0.0)
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        res = safe_api_call(client.get_asset_balance, asset=asset)
        if not res:
            return 0.0
        return float(res.get('free') or 0.0)
    except Exception:
        return 0.0

def get_filters(symbol_info):
    if not symbol_info:
        return {'stepSize': 0.0, 'minQty': 0.0, 'tickSize': 0.0, 'minNotional': None}
    fs = {f.get('filterType'): f for f in symbol_info.get('filters', [])}
    lot = fs.get('LOT_SIZE')
    pricef = fs.get('PRICE_FILTER')
    min_notional = None
    if fs.get('MIN_NOTIONAL'):
        min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional')
    elif fs.get('NOTIONAL'):
        min_notional = fs.get('NOTIONAL', {}).get('minNotional')
    return {
        'stepSize': float(lot['stepSize']) if lot else 0.0,
        'minQty': float(lot['minQty']) if lot else 0.0,
        'tickSize': float(pricef['tickSize']) if pricef else 0.0,
        'minNotional': float(min_notional) if min_notional else None
    }

def orderbook_bullish(symbol, depth=5, min_imbalance=1.05, max_spread_pct=1.5):
    ob = safe_api_call(client.get_order_book, symbol=symbol, limit=depth)
    if not ob:
        return False
    bids = ob.get('bids') or []
    asks = ob.get('asks') or []
    if not bids or not asks:
        return False
    try:
        top_bid_p, top_bid_q = float(bids[0][0]), float(bids[0][1])
        top_ask_p, top_ask_q = float(asks[0][0]), float(asks[0][1])
        spread_pct = (top_ask_p - top_bid_p) / (top_bid_p + 1e-12) * 100.0
        bid_sum = sum(float(b[1]) for b in bids[:depth]) + 1e-12
        ask_sum = sum(float(a[1]) for a in asks[:depth]) + 1e-12
        imbalance = bid_sum / ask_sum
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

def _parse_market_buy_exec(order_resp):
    executed_qty = 0.0
    avg_price = 0.0
    try:
        if not order_resp:
            return 0.0, 0.0
        ex = order_resp.get('executedQty')
        if ex:
            executed_qty = float(ex)
        if executed_qty > 0:
            cumm = order_resp.get('cummulativeQuoteQty') or order_resp.get('cumulativeQuoteQty') or 0.0
            try:
                cumm = float(cumm)
            except Exception:
                cumm = 0.0
            if cumm > 0:
                avg_price = cumm / executed_qty
        if executed_qty == 0.0:
            fills = order_resp.get('fills') or []
            total_qty = 0.0
            total_quote = 0.0
            for f in fills:
                try:
                    q = float(f.get('qty', 0.0) or 0.0)
                    p = float(f.get('price', 0.0) or 0.0)
                except Exception:
                    q = 0.0; p = 0.0
                total_qty += q
                total_quote += q * p
            if total_qty > 0:
                executed_qty = total_qty
                avg_price = total_quote / total_qty if total_qty > 0 else 0.0
    except Exception:
        return 0.0, 0.0
    return executed_qty, avg_price

def is_symbol_tradable(symbol):
    info = get_symbol_info_cached(symbol)
    if not info:
        return False
    status = (info.get('status') or "").upper()
    if status != 'TRADING':
        return False
    perms = info.get('permissions') or []
    if isinstance(perms, list) and any(str(p).upper() == 'SPOT' for p in perms):
        return True
    order_types = info.get('orderTypes') or []
    if isinstance(order_types, list):
        for ot in order_types:
            if str(ot).upper() in ('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT'):
                return True
    return False

def place_safe_market_buy(symbol, usd_amount, require_orderbook: bool = False):
    now = time.time()
    skip_until = TEMP_SKIP.get(symbol)
    if skip_until and now < skip_until:
        notify(f"⏭️ Skipping {symbol} until {time.ctime(skip_until)} (recent failure).")
        return None, None

    if not is_symbol_tradable(symbol):
        notify(f"⛔ Symbol {symbol} not tradable. Temp skipping.")
        with STATE_LOCK:
            TEMP_SKIP[symbol] = time.time() + 3600
        return None, None

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"❌ couldn't fetch symbol info for {symbol}")
        with STATE_LOCK:
            TEMP_SKIP[symbol] = time.time() + 3600
        return None, None
    f = get_filters(info)

    if require_orderbook:
        try:
            if not orderbook_bullish(symbol, depth=5, min_imbalance=1.1, max_spread_pct=0.6):
                notify(f"⚠️ Orderbook not bullish for {symbol}; aborting market buy.")
                return None, None
        except Exception as e:
            logger.debug(f"orderbook check error: {e}")

    price = None
    tickers = get_tickers_cached()
    for t in tickers:
        if t.get('symbol') == symbol:
            price = float(t.get('lastPrice') or 0.0)
            break
    if price is None:
        tk = safe_api_call(client.get_symbol_ticker, symbol=symbol)
        if not tk:
            notify(f"⚠️ Failed to fetch ticker for {symbol}")
            return None, None
        price = float(tk.get('price') or 0.0)
    if price <= 0:
        notify(f"❌ Invalid price for {symbol}: {price}")
        return None, None

    qty_target = usd_amount / price
    step = f.get('stepSize') or 1e-8
    qty_target = math.floor(qty_target / step) * step
    qty_target = max(qty_target, f.get('minQty', 0.0))

    min_notional = f.get('minNotional')
    if min_notional:
        notional = qty_target * price
        if notional < min_notional - 1e-12:
            needed_qty = math.ceil(min_notional / price / (step)) * step
            free_usdt = safe_api_call(client.get_asset_balance, asset=QUOTE)
            free_usdt = float(free_usdt.get('free') or 0.0) if free_usdt else 0.0
            if needed_qty * price > free_usdt + 1e-8:
                notify(f"❌ Not enough funds for MIN_NOTIONAL on {symbol}")
                return None, None
            qty_target = needed_qty

    qty_str = format_qty(qty_target, f.get('stepSize', 0.0))
    if float(qty_str) <= 0:
        notify(f"❌ Computed qty invalid for {symbol}: {qty_str}")
        return None, None

    params = {"symbol": symbol, "quantity": qty_str}
    order = place_order_idempotent('order_market_buy', params, dedupe_ttl=6)
    if not order:
        notify(f"❌ Market buy failed for {symbol}")
        return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    step = f.get('stepSize') or 1e-8
    free_after_clip = math.floor(free_after / step) * step
    if free_after_clip >= f.get('minQty', 0.0) and (executed_qty <= 0 or abs(free_after_clip - executed_qty) / (executed_qty + 1e-9) > 0.02):
        notify(f"ℹ️ Adjust executed_qty {executed_qty} -> balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price or avg_price == 0.0:
            avg_price = price

    executed_qty = math.floor(executed_qty / step) * step
    if executed_qty < f.get('minQty', 0.0) or executed_qty <= 0:
        notify(f"❌ Executed qty too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} @ approx {avg_price:.8f} (≈${executed_qty*avg_price:.6f})")
    return executed_qty, avg_price

def place_market_sell_fallback(symbol, qty, f):
    qty_str = format_qty(qty, f.get('stepSize', 0.0))
    notify(f"⚠️ MARKET sell fallback for {symbol}: qty={qty_str}")
    resp = place_order_idempotent('order_market_sell', {'symbol': symbol, 'quantity': qty_str}, dedupe_ttl=6)
    if resp:
        notify(f"✅ Market sell fallback executed for {symbol}")
        with OPEN_ORDERS_CACHE['lock']:
            OPEN_ORDERS_CACHE['data'] = None
    else:
        notify(f"❌ Market sell fallback failed for {symbol}")
    return resp

def place_oco_sell(symbol, qty, buy_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT, explicit_tp=None, explicit_sl=None):
    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: no symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    tp = explicit_tp if explicit_tp is not None else (buy_price * (1 + tp_pct / 100.0))
    sp = explicit_sl if explicit_sl is not None else (buy_price * (1 - sl_pct / 100.0))
    stop_limit = sp * 0.999

    step = f.get('stepSize') or 1e-8
    tick = f.get('tickSize') or 0.0

    qty = math.floor(qty / step) * step
    if tick and tick > 0:
        tp = math.ceil(tp / tick) * tick
        sp = math.floor(sp / tick) * tick
        sl = math.floor(stop_limit / tick) * tick
    else:
        sl = stop_limit

    if qty <= 0:
        notify("❌ place_oco_sell: qty <= 0 after clipping")
        return None

    free_qty = get_free_asset(asset)
    if free_qty + 1e-12 < qty:
        new_qty = math.floor(max(0.0, free_qty - step) / step) * step
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell for {symbol}")
            return None
        qty = new_qty
        notify(f"ℹ️ Adjust sell qty down to {qty}")

    min_notional = f.get('minNotional')
    if min_notional and qty * tp < min_notional - 1e-12:
        needed_qty = math.ceil(min_notional / tp / step) * step
        if needed_qty <= free_qty + 1e-12:
            qty = needed_qty
            notify(f"ℹ️ Increased qty to meet minNotional -> {qty}")
        else:
            notify("⚠️ Cannot meet minNotional for OCO; will fallback.")

    qty_str = format_qty(qty, f.get('stepSize', 0.0))
    tp_str = format_price(tp, f.get('tickSize', 0.0))
    sp_str = format_price(sp, f.get('tickSize', 0.0))
    sl_str = format_price(sl, f.get('tickSize', 0.0))

    params = {"symbol": symbol, "side": "SELL", "quantity": qty_str,
              "price": tp_str, "stopPrice": sp_str, "stopLimitPrice": sl_str, "stopLimitTimeInForce": "GTC"}
    notify(f"🔁 Attempting OCO for {symbol} TP={tp_str} SL={sp_str} qty={qty_str}")
    # Use standard create_oco_order only (avoid alternative 'aboveType' param which may cause -1102)
    oco = place_order_idempotent('create_oco_order', params, dedupe_ttl=8)
    if oco:
        with OPEN_ORDERS_CACHE['lock']:
            OPEN_ORDERS_CACHE['data'] = None
        notify(f"📌 OCO placed for {symbol}")
        return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}

    notify("⚠️ OCO failed -> fallback to separate TP/SL")
    tp_order = place_order_idempotent('order_limit_sell', {"symbol": symbol, "quantity": qty_str, "price": tp_str}, dedupe_ttl=6)
    sl_order = place_order_idempotent('create_order', {"symbol": symbol, "side": "SELL", "type": "STOP_LOSS_LIMIT",
                                                         "stopPrice": sp_str, "price": sl_str, "timeInForce": "GTC", "quantity": qty_str},
                                      dedupe_ttl=6)
    if tp_order or sl_order:
        with OPEN_ORDERS_CACHE['lock']:
            OPEN_ORDERS_CACHE['data'] = None
        notify("ℹ️ Fallback TP/SL placed")
        return {'tp': tp, 'sl': sp, 'method': 'fallback', 'raw': {'tp': tp_order, 'sl': sl_order}}

    fallback_market = place_market_sell_fallback(symbol, qty, f)
    if fallback_market:
        return {'tp': tp, 'sl': sp, 'method': 'market_fallback', 'raw': fallback_market}

    with STATE_LOCK:
        TEMP_SKIP[symbol] = time.time() + 3600
    notify("❌ All protective attempts failed; temp skip symbol.")
    return None

def place_micro_tp(symbol, qty, entry_price, f, pct=MICRO_TP_PCT, fraction=MICRO_TP_FRACTION):
    try:
        sell_qty = float(qty) * float(fraction)
        step = f.get('stepSize') or 1e-8
        sell_qty = math.floor(sell_qty / step) * step
        if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
            notify(f"ℹ️ Micro TP: sell_qty too small ({sell_qty}) for {symbol}, skipping.")
            return None, 0.0, None

        tp_price = float(entry_price) * (1.0 + float(pct) / 100.0)
        tick = f.get('tickSize') or 0.0
        if tick and tick > 0:
            tp_price = math.ceil(tp_price / tick) * tick

        if f.get('minNotional') and sell_qty * tp_price < f['minNotional'] - 1e-12:
            notify("⚠️ Micro TP would violate minNotional - skipping.")
            return None, 0.0, None

        qty_str = format_qty(sell_qty, f.get('stepSize', 0.0))
        price_str = format_price(tp_price, f.get('tickSize', 0.0))
        notify(f"📍 Placing micro TP for {symbol}: {qty_str} @ {price_str}")
        order = place_order_idempotent('order_limit_sell', {"symbol": symbol, "quantity": qty_str, "price": price_str}, dedupe_ttl=6)
        if not order:
            notify("⚠️ Micro TP order failed")
            return None, 0.0, None
        with OPEN_ORDERS_CACHE['lock']:
            OPEN_ORDERS_CACHE['data'] = None

        order_id = order.get('orderId') if isinstance(order, dict) else None
        poll = 0.6
        waited = 0.0
        while waited < MICRO_MAX_WAIT:
            status = safe_api_call(client.get_order, symbol=symbol, orderId=order_id) if order_id else None
            if status:
                executed_qty = float(status.get('executedQty') or 0.0)
                if executed_qty > 0:
                    avg_fill = None
                    cumm = status.get('cummulativeQuoteQty') or status.get('cumulativeQuoteQty') or 0.0
                    try:
                        cumm = float(cumm)
                        if executed_qty > 0 and cumm > 0:
                            avg_fill = cumm / executed_qty
                    except Exception:
                        avg_fill = None
                    if avg_fill is None:
                        avg_fill = tp_price
                    profit_usd = (avg_fill - float(entry_price)) * executed_qty
                    if profit_usd and profit_usd > 0 and profit_usd >= MIN_TRANSFER_AMOUNT and not CONFIRM_TRANSFERS:
                        try:
                            exec_api_method('universal_transfer', type='SPOT_TO_FUNDING', asset=QUOTE, amount=str(round(profit_usd, 6)))
                            notify(f"💸 Micro TP profit ${profit_usd:.6f} sent to funding.")
                        except Exception as e:
                            notify(f"⚠️ transfer failed: {e}")
                    else:
                        notify(f"ℹ️ Micro TP filled profit ${profit_usd:.6f} (not auto-transferred).")
                    return order, executed_qty, tp_price
            time.sleep(poll)
            waited += poll
        notify("ℹ️ Micro TP not filled in wait window.")
        return order, 0.0, tp_price
    except Exception as e:
        notify(f"⚠️ place_micro_tp error: {e}")
        return None, 0.0, None

# -------------------------
# PICKER (adapted, uses keyword param API calls)
# -------------------------
def pick_coin():
    def pct_change(open_p, close_p):
        if open_p == 0:
            return 0.0
        return (close_p - open_p) / open_p * 100.0

    def ema_local(values, period):
        if not values or period <= 0:
            return None
        alpha = 2.0 / (period + 1.0)
        e = float(values[0])
        for v in values[1:]:
            e = alpha * float(v) + (1 - alpha) * e
        return e

    def compute_rsi_local(closes, period=14):
        if not closes or len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(0.0, diff))
            losses.append(max(0.0, -diff))
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period if sum(losses[:period]) != 0 else 1e-9
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / (avg_loss if avg_loss > 0 else 1e-9)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    tickers = get_tickers_cached() or []
    now = time.time()
    pre = []

    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE):
            continue
        try:
            last = float(t.get('lastPrice') or 0.0)
            qvol = float(t.get('quoteVolume') or 0.0)
            ch = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        # price bounds
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        # volume baseline
        if qvol < MIN_VOLUME:
            continue
        # 24h change guard
        if ch < 0.5 or ch > 15.0:
            continue
        # skip
        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue
        last_buy = RECENT_BUYS.get(sym)
        if last_buy:
            if now < last_buy['ts'] + BUY_LOCK_SECONDS:
                continue
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]

    scored = []
    for sym, last_price, qvol, change_24h in candidates:
        time.sleep(REQUEST_SLEEP)
        kl5 = safe_api_call(client.get_klines, symbol=sym, interval='5m', limit=KLINES_5M_LIMIT)
        if not kl5 or len(kl5) < 3:
            continue
        time.sleep(REQUEST_SLEEP)
        kl1 = safe_api_call(client.get_klines, symbol=sym, interval='1m', limit=KLINES_1M_LIMIT)
        if not kl1 or len(kl1) < 2:
            continue
        try:
            closes_5m = [float(k[4]) for k in kl5]
            vols_5m = []
            for k in kl5:
                if len(k) > 7 and k[7] is not None:
                    vols_5m.append(float(k[7]))
                else:
                    vols_5m.append(float(k[5]) * float(k[4]))
            closes_1m = [float(k[4]) for k in kl1]
            vols_1m = []
            for k in kl1:
                if len(k) > 7 and k[7] is not None:
                    vols_1m.append(float(k[7]))
                else:
                    vols_1m.append(float(k[5]) * float(k[4]))

            open_5m = float(kl5[0][1])
            pct_5m = pct_change(open_5m, closes_5m[-1])
            open_1m = float(kl1[0][1])
            pct_1m = pct_change(open_1m, closes_1m[-1])

            avg_prev_5m = (sum(vols_5m[:-1]) / max(len(vols_5m[:-1]), 1)) if len(vols_5m) > 1 else vols_5m[-1]
            vol_ratio_5m = vols_5m[-1] / (avg_prev_5m + 1e-12)

            avg_prev_1m = (sum(vols_1m[:-1]) / max(len(vols_1m[:-1]), 1)) if len(vols_1m) > 1 else vols_1m[-1]
            vol_ratio_1m = vols_1m[-1] / (avg_prev_1m + 1e-12)

            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_ok = False
            ema_uplift = 0.0
            if short_ema and long_ema:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                ema_ok = short_ema > long_ema * 1.0005

            rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
            rsi_ok = True
            if rsi_val is not None and rsi_val > 68:
                rsi_ok = False

            time.sleep(REQUEST_SLEEP)
            ob = safe_api_call(client.get_order_book, symbol=sym, limit=OB_DEPTH)
            ob_ok = False
            ob_imbalance = 1.0
            ob_spread_pct = 100.0
            bid_quote_liq = 0.0
            if ob:
                bids = ob.get('bids') or []
                asks = ob.get('asks') or []
                if bids and asks:
                    top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
                    bid_sum = sum(float(b[1]) for b in bids[:OB_DEPTH]) + 1e-12
                    ask_sum = sum(float(a[1]) for a in asks[:OB_DEPTH]) + 1e-12
                    ob_imbalance = bid_sum / ask_sum
                    ob_spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
                    bid_quote_liq = sum(float(b[0]) * float(b[1]) for b in bids[:OB_DEPTH])
                    if bid_quote_liq >= MIN_OB_LIQUIDITY:
                        ob_ok = (ob_imbalance >= MIN_OB_IMBALANCE) and (ob_spread_pct <= MAX_OB_SPREAD_PCT)
                    else:
                        ob_ok = False

            score = 0.0
            score += max(0.0, pct_5m) * 4.0
            score += max(0.0, pct_1m) * 2.0
            score += max(0.0, (vol_ratio_5m - 1.0)) * 3.0 * 100.0
            score += ema_uplift * 5.0 * 100.0
            if rsi_val is not None:
                score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.5 * 0.2
            score += max(0.0, change_24h) * 1.0 * 0.5
            if ob_ok:
                score += 2.0 * 10.0
            if last_price <= PRICE_MAX:
                score += 6.0

            strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and vol_ratio_5m >= 1.4
                                and ema_ok and rsi_ok and ob_ok)

            scored.append({
                "symbol": sym,
                "last_price": last_price,
                "24h_change": change_24h,
                "24h_vol": qvol,
                "pct_5m": pct_5m,
                "pct_1m": pct_1m,
                "vol_ratio_5m": vol_ratio_5m,
                "ema_ok": ema_ok,
                "ema_uplift": ema_uplift,
                "rsi": rsi_val,
                "ob_ok": ob_ok,
                "ob_imbalance": ob_imbalance,
                "ob_spread_pct": ob_spread_pct,
                "ob_bid_liq": bid_quote_liq,
                "score": score,
                "strong_candidate": strong_candidate
            })
        except Exception as e:
            logger.debug(f"pick_coin evaluate error {sym}: {e}")
            _maybe_notify_api_error(f"⚠️ pick_coin evaluate error {sym}: {e}")
            continue

    if not scored:
        return None

    scored.sort(key=lambda x: x['score'], reverse=True)
    strongs = [s for s in scored if s['strong_candidate']]
    if strongs:
        best = sorted(strongs, key=lambda x: x['score'], reverse=True)[0]
    else:
        best = scored[0]
    return (best['symbol'], best['last_price'], best['24h_vol'], best['24h_change'])

# -------------------------
# monitor_and_roll (same logic; uses place_oco_sell/market fallback)
# -------------------------
def monitor_and_roll(symbol, qty, entry_price, f):
    orig_qty = qty
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    oco = place_oco_sell(symbol, qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
    if oco is None:
        notify(f"❌ Initial OCO failed for {symbol}, aborting monitor.")
        return False, entry_price, 0.0

    last_tp = None
    last_roll_ts = 0.0
    last_roll_price = entry_price
    roll_count = 0
    last_price_seen = entry_price

    def clip_tp(v, tick):
        if not tick or tick == 0:
            return v
        return math.ceil(v / tick) * tick

    def clip_sl(v, tick):
        if not tick or tick == 0:
            return v
        return math.floor(v / tick) * tick

    while True:
        try:
            time.sleep(SLEEP_BETWEEN_CHECKS)
            asset = symbol[:-len(QUOTE)]
            price_now = get_tickers_cached()
            # prefer cached single price
            p = None
            for t in price_now:
                if t.get('symbol') == symbol:
                    try:
                        p = float(t.get('lastPrice') or t.get('price') or 0.0)
                    except Exception:
                        p = None
                    break
            if p is None:
                res = safe_api_call(client.get_symbol_ticker, symbol=symbol)
                if not res:
                    notify("⚠️ monitor_and_roll: failed to fetch price, continuing.")
                    continue
                p = float(res.get('price') or res.get('lastPrice') or 0.0)
            price_now = p

            price_moving_up = price_now > last_price_seen
            last_price_seen = price_now

            free_qty = get_free_asset(asset)
            available_for_sell = min(math.floor(free_qty / (f.get('stepSize') or 1e-8)) * (f.get('stepSize') or 1e-8), orig_qty)
            open_orders = get_open_orders_cached(symbol)

            if available_for_sell < math.floor(orig_qty * 0.05 / (f.get('stepSize') or 1e-8)) * (f.get('stepSize') or 1e-8) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= max(ROLL_TRIGGER_DELTA_ABS, entry_price * (0.0075))
            near_trigger = (price_now >= curr_tp * (1 - 0.035)) and (price_now < curr_tp * 1.05)
            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(entry_price * 0.004, ROLL_TRIGGER_DELTA_ABS * 0.4, tick or 0.0)
            moved_enough = price_delta >= minimal_move
            now_ts = time.time()
            can_roll_time = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS
            passed_price_debounce = price_now >= (last_roll_price + ROLL_TRIGGER_DELTA_ABS)

            should_roll = ((near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs)
            if should_roll and passed_price_debounce and price_moving_up and can_roll_time and available_for_sell >= f.get('minQty', 0.0):
                if roll_count >= MAX_ROLLS_PER_POSITION:
                    notify(f"⚠️ Reached max rolls ({MAX_ROLLS_PER_POSITION}) for {symbol}, will not roll further.")
                    last_roll_ts = now_ts
                    continue

                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, curr_tp={curr_tp:.8f}, delta={price_delta:.6f}")
                candidate_tp = curr_tp + ROLL_TP_STEP_ABS
                candidate_sl = curr_sl + ROLL_SL_STEP_ABS
                if candidate_sl > entry_price:
                    candidate_sl = entry_price

                new_tp = clip_tp(candidate_tp, tick)
                new_sl = clip_sl(candidate_sl, tick)
                tick_step = tick or 0.0
                if new_tp <= new_sl + tick_step:
                    if tick_step > 0:
                        new_tp = new_sl + tick_step * 2
                    else:
                        new_tp = new_sl + max(1e-8, ROLL_TP_STEP_ABS)
                if new_tp <= curr_tp:
                    if tick_step > 0:
                        new_tp = math.ceil((curr_tp + tick_step) / tick_step) * tick_step
                    else:
                        new_tp = curr_tp + max(1e-8, ROLL_TP_STEP_ABS)

                sell_qty = math.floor(available_for_sell / (f.get('stepSize') or 1e-8)) * (f.get('stepSize') or 1e-8)
                if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
                    notify(f"⚠️ Roll skipped: sell_qty {sell_qty} too small or < minQty.")
                    last_roll_ts = now_ts
                    continue

                min_notional = f.get('minNotional')
                if min_notional:
                    adjust_cnt = 0
                    max_adj = 40
                    while adjust_cnt < max_adj and sell_qty * new_tp < min_notional - 1e-12:
                        if tick_step > 0:
                            new_tp = clip_tp(new_tp + tick_step, tick_step)
                        else:
                            new_tp = new_tp + max(1e-8, new_tp * 0.001)
                        adjust_cnt += 1
                    if sell_qty * new_tp < min_notional - 1e-12:
                        needed_qty = math.ceil(min_notional / new_tp / (f.get('stepSize') or 1e-8)) * (f.get('stepSize') or 1e-8)
                        if needed_qty <= available_for_sell + 1e-12 and needed_qty > sell_qty:
                            notify(f"ℹ️ Increasing sell_qty to {needed_qty} to meet minNotional for roll.")
                            sell_qty = needed_qty
                        else:
                            notify(f"⚠️ Roll aborted: cannot meet minNotional for {symbol} even after TP bumps.")
                            last_roll_ts = now_ts
                            continue

                last_roll_ts = now_ts
                cancel_all_open_orders(symbol)
                time.sleep(random.uniform(0.3, 0.8))

                oco2 = place_oco_sell(symbol, sell_qty, entry_price, explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    roll_count += 1
                    last_tp = curr_tp
                    curr_tp = new_tp
                    curr_sl = new_sl
                    last_roll_price = price_now
                    notify(f"🔁 Rolled OCO: new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty} (roll #{roll_count})")
                else:
                    notify("⚠️ Roll attempt failed; will try to re-place protective OCO next loop.")
                    time.sleep(0.4)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        notify("ℹ️ Fallback OCO re-placed after failed roll.")
                    else:
                        notify("❌ Fallback OCO also failed; TEMP skipping symbol.")
                        with STATE_LOCK:
                            TEMP_SKIP[symbol] = time.time() + 3600
        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            logger.exception("monitor_and_roll")
            return False, entry_price, 0.0

def cancel_all_open_orders(symbol, max_cancel=6, inter_delay=0.25):
    try:
        open_orders = get_open_orders_cached(symbol)
        cancelled = 0
        for o in open_orders:
            if cancelled >= max_cancel:
                logger.debug(f"Reached max_cancel ({max_cancel}) for {symbol}")
                break
            try:
                safe_api_call(client.cancel_order, symbol=symbol, orderId=o.get('orderId'))
                cancelled += 1
                time.sleep(inter_delay)
            except Exception as e:
                logger.debug(f"Cancel failed for {symbol} order {o.get('orderId')}: {e}")
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
        with OPEN_ORDERS_CACHE['lock']:
            OPEN_ORDERS_CACHE['data'] = None
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")
        logger.exception("cancel_all_open_orders")

# -------------------------
# MAIN TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0

def trade_cycle():
    global ACTIVE_SYMBOL, LAST_BUY_TS
    start_balance = safe_api_call(client.get_asset_balance, asset=QUOTE)
    start_balance_val = float(start_balance.get('free') or 0.0) if start_balance else 0.0
    notify(f"🔰 Start balance snapshot: {start_balance_val:.6f} {QUOTE}")

    while True:
        try:
            # housekeeping flush notifications occasionally
            _flush_notify_cache()

            now = time.time()
            for s, until in list(TEMP_SKIP.items()):
                if now >= until:
                    del TEMP_SKIP[s]

            if ACTIVE_SYMBOL is not None:
                notify(f"⏳ Active trade {ACTIVE_SYMBOL}, skipping buy cycle.")
                time.sleep(CYCLE_DELAY)
                continue

            if time.time() - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            candidate = pick_coin()
            if not candidate:
                notify("⚠️ No candidate found - sleeping.")
                time.sleep(CYCLE_DELAY)
                continue

            symbol, price, vol, ch = candidate
            notify(f"🎯 Selected {symbol} price={price} 24hΔ={ch}")

            free_usdt = safe_api_call(client.get_asset_balance, asset=QUOTE)
            free_usdt = float(free_usdt.get('free') or 0.0) if free_usdt else 0.0
            usd_to_buy = min(TRADE_USD, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT free={free_usdt:.4f}")
                time.sleep(CYCLE_DELAY)
                continue

            buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            if not buy_res or buy_res == (None, None):
                notify("ℹ️ Buy skipped/failed.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify("⚠️ Unexpected buy result.")
                time.sleep(CYCLE_DELAY)
                continue

            with STATE_LOCK:
                RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None}

            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            micro_order, micro_qty, micro_price = place_micro_tp(symbol, qty, entry_price, f)

            remaining = max(0.0, qty - (micro_qty or 0.0))
            step = f.get('stepSize') or 1e-8
            remaining = math.floor(remaining / step) * step
            if remaining <= 0 or remaining < f.get('minQty', 0.0):
                notify("ℹ️ Nothing left to monitor after micro TP.")
                LAST_BUY_TS = time.time()
                continue

            with STATE_LOCK:
                ACTIVE_SYMBOL = symbol
            try:
                closed, exit_price, profit = monitor_and_roll(symbol, remaining, entry_price, f)
            finally:
                with STATE_LOCK:
                    ACTIVE_SYMBOL = None

            total_profit = profit or 0.0
            if micro_order and micro_qty and micro_price:
                try:
                    total_profit += (micro_price - entry_price) * micro_qty
                except Exception:
                    pass

            notify(f"📊 Trade ended for {symbol} profit≈${total_profit:.6f}")
            LAST_BUY_TS = time.time()
            time.sleep(2)

        except Exception as e:
            notify(f"❌ Unexpected error in trade_cycle: {e}")
            logger.exception("trade_cycle")
            time.sleep(CYCLE_DELAY)

# -------------------------
# Flask health & shutdown
# -------------------------
app = Flask(__name__)
@app.route("/health")
def health():
    return Response("OK", status=200)

@app.route("/metrics")
def metrics():
    with STATE_LOCK:
        active = ACTIVE_SYMBOL or "idle"
        buys = len(RECENT_BUYS)
    body = f"active_symbol {active}\nrecent_buys {buys}\n"
    return Response(body, status=200, mimetype="text/plain")

stop_event = threading.Event()
def _signal_handler(signum, frame):
    notify(f"Signal {signum} received - shutting down")
    stop_event.set()
    _save_dedupe()
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def start_flask():
    port = int(os.environ.get("PORT", "5000"))
    notify(f"Starting health server on :{port}")
    app.run(host="0.0.0.0", port=port, threaded=True)

# -------------------------
# bootstrap
# -------------------------
if __name__ == "__main__":
    notify("Booting cryptobot (LIVE final with keyword-arg API calls & notify dedupe)")
    acct_ok = safe_api_call(client.get_account)
    if not acct_ok:
        notify("❌ API validation failed - aborting")
        raise SystemExit(1)
    notify("✅ API validated, starting trade loop")
    t = threading.Thread(target=trade_cycle, daemon=True)
    t.start()
    start_flask()