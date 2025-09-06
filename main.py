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
# CONFIG
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
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
# INIT / GLOBALS
# -------------------------
client = Client(API_KEY, API_SECRET)
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
    try:
        result = client.universal_transfer(
            type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
            asset=asset,
            amount=str(round(amount, 6))
        )
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        try:
            result = client.universal_transfer(
                type='SPOT_TO_FUNDING',
                asset=asset,
                amount=str(round(amount, 6))
            )
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet (fallback).")
            return result
        except Exception as e2:
            notify(f"❌ Failed to transfer profit: {e} | {e2}")
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

def pick_coin():
    global RATE_LIMIT_BACKOFF
    cleanup_temp_skip()
    cleanup_recent_buys()
    now = time.time()
    tickers = get_tickers_cached() or []
    prefiltered = []

    MIN_QVOL = max(MIN_VOLUME, 500_000)
    MIN_CHANGE_PCT = max(1.0, MOVEMENT_MIN_PCT if 'MOVEMENT_MIN_PCT' in globals() else 1.0)
    TOP_CANDIDATES = 120
    MIN_VOL_RATIO = 1.4
    MIN_RECENT_PCT = 0.5
    KLINES_LIMIT = 10

    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE):
            continue
        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue
        last = RECENT_BUYS.get(sym)
        try:
            price = float(t.get('lastPrice') or 0.0)
        except Exception:
            continue
        if last:
            if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                continue
            last_price = last.get('price')
            if last_price and price > last_price * (1 + REBUY_MAX_RISE_PCT / 100.0):
                continue
        try:
            qvol = float(t.get('quoteVolume') or 0.0)
            change_pct = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if qvol < MIN_QVOL:
            continue
        if change_pct < MIN_CHANGE_PCT:
            continue
        prefiltered.append((sym, price, qvol, change_pct))

    if not prefiltered:
        return None

    prefiltered.sort(key=lambda x: x[2], reverse=True)
    prefiltered = prefiltered[:TOP_CANDIDATES]

    candidates = []
    for sym, price, qvol, change_pct in prefiltered:
        try:
            klines = client.get_klines(symbol=sym, interval='5m', limit=KLINES_LIMIT)
        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                notify(f"⚠️ Rate limit while fetching klines: {err}. Pausing pick_coin briefly.")
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                time.sleep(RATE_LIMIT_BACKOFF)
                break
            continue

        if not klines or len(klines) < 4:
            continue

        vols = []
        closes = []
        for k in klines:
            try:
                if len(k) > 7 and k[7] is not None:
                    vols.append(float(k[7]))
                else:
                    vols.append(float(k[5]) * float(k[4]))
            except Exception:
                vols.append(0.0)
            try:
                closes.append(float(k[4]))
            except Exception:
                closes.append(0.0)

        recent_vol = vols[-1] if vols else 0.0
        if recent_vol <= 0:
            continue

        avg_vol = (sum(vols[-(KLINES_LIMIT-1):-1]) / max(len(vols[-(KLINES_LIMIT-1):-1]), 1)) if len(vols) > 1 else recent_vol
        vol_ratio = recent_vol / (avg_vol + 1e-9)

        recent_pct = 0.0
        if len(closes) >= 4 and closes[-4] > 0:
            recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

        if vol_ratio < MIN_VOL_RATIO:
            continue
        if recent_pct < MIN_RECENT_PCT:
            continue

        # quick RSI sanity
        rsi_ok = True
        if len(closes) >= 15:
            gains = []
            losses = []
            for i in range(1, 15):
                diff = closes[-15 + i] - closes[-16 + i]
                if diff > 0:
                    gains.append(diff)
                else:
                    losses.append(abs(diff))
            avg_gain = sum(gains) / 14.0 if gains else 0.0
            avg_loss = sum(losses) / 14.0 if losses else 1e-9
            rs = avg_gain / (avg_loss if avg_loss > 0 else 1e-9)
            rsi = 100 - (100 / (1 + rs))
            if rsi > 70:
                rsi_ok = False
        if not rsi_ok:
            continue

        score = change_pct * qvol * vol_ratio * max(1.0, recent_pct / 2.0)
        candidates.append((sym, price, qvol, change_pct, vol_ratio, recent_pct, score))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[-1], reverse=True)
    best = candidates[0]
    return (best[0], best[1], best[2], best[3])

# -------------------------
# MARKET BUY helpers
# -------------------------
def _parse_market_buy_exec(order_resp):
    executed_qty = 0.0
    avg_price = 0.0
    try:
        if not order_resp:
            return 0.0, 0.0
        for key in ('executedQty',):
            val = order_resp.get(key)
            if val is not None:
                try:
                    if float(val) > 0:
                        executed_qty = float(val)
                        break
                except Exception:
                    pass
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
    try:
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
                if str(ot).upper() in ('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_MARKET', 'TAKE_PROFIT_MARKET'):
                    return True
        return False
    except Exception as e:
        notify(f"⚠️ is_symbol_tradable check failed for {symbol}: {e}")
        return False

def place_safe_market_buy(symbol, usd_amount):
    now = time.time()
    skip_until = TEMP_SKIP.get(symbol)
    if skip_until and now < skip_until:
        notify(f"⏭️ Skipping {symbol} until {time.ctime(skip_until)} (recent failure).")
        return None, None

    if not is_symbol_tradable(symbol):
        notify(f"⛔ Symbol {symbol} not tradable / market closed. Skipping and blacklisting.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"❌ place_safe_market_buy: couldn't fetch symbol info for {symbol}")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None
    f = get_filters(info)

    try:
        price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    except Exception as e:
        notify(f"⚠️ Failed to fetch ticker for {symbol}: {e}")
        return None, None

    qty_target = usd_amount / price
    qty_target = max(qty_target, f['minQty'])
    qty_target = round_step(qty_target, f['stepSize'])

    if f['minNotional']:
        notional = qty_target * price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                notify(f"❌ Not enough funds for MIN_NOTIONAL on {symbol}")
                return None, None
            qty_target = needed_qty

    qty_str = format_qty(qty_target, f['stepSize'])

    try:
        order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
    except Exception as e:
        err = str(e)
        notify(f"❌ Market buy failed for {symbol}: {err}")
        if '-1013' in err or 'Market is closed' in err:
            TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order_resp)

    time.sleep(0.8)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    free_after_clip = round_step(free_after, f['stepSize'])

    if free_after_clip >= f['minQty'] and (executed_qty <= 0 or abs(free_after_clip - executed_qty) / (executed_qty + 1e-9) > 0.02):
        notify(f"ℹ️ Adjusting executed qty from parsed {executed_qty} to actual free balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price or avg_price == 0.0:
            avg_price = price

    executed_qty = round_step(executed_qty, f['stepSize'])
    if executed_qty < f['minQty'] or executed_qty <= 0:
        notify(f"❌ Executed quantity too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    if free_after_clip < max(1e-8, executed_qty * 0.5):
        notify(f"⚠️ After buy free balance {free_after_clip} is much smaller than expected executed {executed_qty}. Skipping symbol for a while.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty*avg_price:.6f}")
    return executed_qty, avg_price

# --- Modified place_micro_tp: returns (order_resp, sell_qty, tp_price)
def place_micro_tp(symbol, qty, entry_price, f, pct=MICRO_TP_PCT, fraction=MICRO_TP_FRACTION):
    """
    Place a small limit sell for `fraction` of qty at price = entry_price*(1 + pct/100).
    Short-poll for quick fills and send realized micro-profit to funding wallet.
    Returns (order_response_or_None, sell_qty_float, tp_price_used_or_None)
    """
    try:
        sell_qty = float(qty) * float(fraction)
        sell_qty = round_step(sell_qty, f.get('stepSize', 0.0))
        if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
            notify(f"ℹ️ Micro TP: sell_qty too small ({sell_qty}) for {symbol}, skipping micro TP.")
            return None, 0.0, None

        tp_price = float(entry_price) * (1.0 + float(pct) / 100.0)
        tick = f.get('tickSize', 0.0) or 0.0
        if tick and tick > 0:
            tp_price = math.ceil(tp_price / tick) * tick

        if f.get('minNotional'):
            if sell_qty * tp_price < f['minNotional'] - 1e-12:
                notify(f"⚠️ Micro TP would violate MIN_NOTIONAL for {symbol} (need {f['minNotional']}). Skipping micro TP.")
                return None, 0.0, None

        qty_str = format_qty(sell_qty, f.get('stepSize', 0.0))
        price_str = format_price(tp_price, f.get('tickSize', 0.0))

        try:
            # include timeInForce for clarity
            order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str, timeInForce='GTC')
            notify(f"📍 Micro TP placed for {symbol}: sell {qty_str} @ {price_str}")
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            # short-poll to see if the order filled quickly (so we can send profit)
            # small window to avoid blocking main loop
            order_id = None
            if isinstance(order, dict):
                order_id = order.get('orderId') or order.get('orderId')
            if not order_id:
                return order, sell_qty, tp_price

            poll_interval = 0.5
            max_wait = 8.0
            waited = 0.0

            while waited < max_wait:
                try:
                    status = client.get_order(symbol=symbol, orderId=order_id)
                except Exception:
                    break

                executed_qty = 0.0
                avg_fill_price = None
                try:
                    ex = status.get('executedQty')
                    if ex is not None:
                        executed_qty = float(ex)
                except Exception:
                    executed_qty = 0.0

                if executed_qty == 0.0:
                    fills = status.get('fills') or []
                    total_q = 0.0
                    total_quote = 0.0
                    for fll in fills:
                        try:
                            fq = float(fll.get('qty', 0.0) or 0.0)
                            fp = float(fll.get('price', 0.0) or 0.0)
                        except Exception:
                            fq = 0.0; fp = 0.0
                        total_q += fq
                        total_quote += fq * fp
                    if total_q > 0:
                        executed_qty = total_q
                        avg_fill_price = (total_quote / total_q) if total_q > 0 else None

                if executed_qty and executed_qty > 0.0:
                    if avg_fill_price is None:
                        cumm = status.get('cummulativeQuoteQty') or status.get('cumulativeQuoteQty') or 0.0
                        try:
                            cumm = float(cumm)
                            if executed_qty > 0 and cumm > 0:
                                avg_fill_price = cumm / executed_qty
                        except Exception:
                            avg_fill_price = None

                    if avg_fill_price is None:
                        avg_fill_price = tp_price

                    profit_usd = (avg_fill_price - float(entry_price)) * executed_qty
                    try:
                        profit_to_send = float(round(profit_usd, 6))
                    except Exception:
                        profit_to_send = profit_usd

                    if profit_to_send and profit_to_send > 0.0:
                        try:
                            send_profit_to_funding(profit_to_send)
                            notify(f"💸 Micro TP profit ${profit_to_send:.6f} for {symbol} sent to funding.")
                        except Exception as e:
                            notify(f"⚠️ Failed to transfer micro profit for {symbol}: {e}")
                    else:
                        notify(f"ℹ️ Micro TP filled but profit non-positive (${profit_to_send:.6f}) — not sending.")
                    break

                time.sleep(poll_interval)
                waited += poll_interval

            return order, sell_qty, tp_price

        except Exception as e:
            notify(f"⚠️ Failed to place micro TP for {symbol}: {e}")
            return None, 0.0, None

    except Exception as e:
        notify(f"⚠️ place_micro_tp error: {e}")
        return None, 0.0, None

# -------------------------
# OCO SELL with fallbacks
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    tp = explicit_tp if explicit_tp is not None else (buy_price * (1 + tp_pct / 100.0))
    sp = explicit_sl if explicit_sl is not None else (buy_price * (1 - sl_pct / 100.0))
    stop_limit = sp * 0.999

    def clip_floor(v, step):
        if not step or step == 0:
            return v
        return math.floor(v / step) * step

    def clip_ceil(v, step):
        if not step or step == 0:
            return v
        return math.ceil(v / step) * step

    qty = clip_floor(qty, f['stepSize'])
    tp = clip_floor(tp, f['tickSize'])
    sp = clip_floor(sp, f['tickSize'])
    sl = clip_floor(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    free_qty = get_free_asset(asset)
    safe_margin = f['stepSize'] if f['stepSize'] and f['stepSize'] > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip_floor(max(0.0, free_qty - safe_margin), f['stepSize'])
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

    min_notional = f.get('minNotional')
    tick = f.get('tickSize') or 0.0
    max_adjust_attempts = 40
    adjust_attempt = 0
    if min_notional:
        while adjust_attempt < max_adjust_attempts and qty * tp < min_notional - 1e-12:
            if tick and tick > 0:
                tp = clip_ceil(tp + tick, tick)
            else:
                tp = tp + max(1e-8, tp * 0.001)
            adjust_attempt += 1
        if qty * tp < min_notional - 1e-12:
            notify(f"⚠️ Unable to satisfy MIN_NOTIONAL for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Skipping OCO.")
            return None

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    for attempt in range(1, retries + 1):
        try:
            oco = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=qty_str,
                price=tp_str,
                stopPrice=sp_str,
                stopLimitPrice=sl_str,
                stopLimitTimeInForce='GTC'
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            if 'aboveType' in err or '-1102' in err or 'Mandatory parameter' in err:
                notify("ℹ️ Detected 'aboveType' style requirement; will attempt alternative param names.")
                break
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    for attempt in range(1, retries + 1):
        try:
            oco2 = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=qty_str,
                aboveType="LIMIT_MAKER",
                abovePrice=tp_str,
                belowType="STOP_LOSS_LIMIT",
                belowStopPrice=sp_str,
                belowPrice=sl_str,
                belowTimeInForce="GTC"
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
            if 'NOTIONAL' in err or 'minNotional' in err or 'Filter failure' in err:
                if tick and tick > 0:
                    tp = clip_ceil(tp + tick, tick)
                else:
                    tp = tp + max(1e-8, tp * 0.001)
                tp_str = format_price(tp, f['tickSize'])
                if min_notional and qty * tp < min_notional - 1e-12:
                    notify(f"⚠️ Still below minNotional after bump (qty*tp={qty*tp:.8f}), continuing attempts.")
                    time.sleep(delay)
                    continue
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-market).")
    tp_order = None
    sl_order = None
    try:
        tp_order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=tp_str, timeInForce='GTC')
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback TP limit failed: {e}")
    try:
        sl_order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=sp_str,
            quantity=qty_str
        )
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📉 SL STOP_MARKET placed (fallback): trigger={sp_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-market failed: {e}")

    if tp_order or sl_order:
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}
    else:
        notify("❌ All attempts to protect position failed (no TP/SL placed).")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None

# -------------------------
# CANCEL HELPERS
# -------------------------
def cancel_all_open_orders(symbol):
    try:
        open_orders = get_open_orders_cached(symbol)
        cancelled = 0
        for o in open_orders:
            oid = o.get('orderId') or o.get('orderId')
            if oid:
                try:
                    client.cancel_order(symbol=symbol, orderId=oid)
                    cancelled += 1
                except Exception:
                    pass
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
        OPEN_ORDERS_CACHE['data'] = None
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")

# -------------------------
# MONITOR & ROLL
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
            try:
                price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
            except Exception as e:
                notify(f"⚠️ Failed to fetch price in monitor: {e}")
                continue

            free_qty = get_free_asset(asset)
            available_for_sell = min(round_step(free_qty, f.get('stepSize', 0.0)), orig_qty)

            open_orders = get_open_orders_cached(symbol)

            if available_for_sell < round_step(orig_qty * 0.05, f.get('stepSize', 0.0)) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= ROLL_TRIGGER_DELTA_ABS

            near_trigger = (price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)) and (price_now < curr_tp * 1.05)

            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(ROLL_TRIGGER_DELTA_ABS * 0.4, tick or 0.0)
            moved_enough = price_delta >= minimal_move

            now_ts = time.time()
            can_roll = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            if ( (near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs ) and available_for_sell >= f.get('minQty', 0.0) and can_roll:
                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, curr_tp={curr_tp:.8f}, delta={price_delta:.6f} (near={near_trigger},pct={rise_trigger_pct},abs={rise_trigger_abs})")
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

                sell_qty = round_step(available_for_sell, f.get('stepSize', 0.0))
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
                        notify(f"⚠️ Roll aborted: cannot meet minNotional for {symbol} even after TP bumps (qty*tp={sell_qty*new_tp:.8f} < {min_notional}).")
                        last_roll_ts = now_ts
                        continue

                last_roll_ts = now_ts
                cancel_all_open_orders(symbol)
                time.sleep(0.5)

                oco2 = place_oco_sell(symbol, sell_qty, entry_price,
                                      explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    last_tp = curr_tp
                    curr_tp = new_tp
                    curr_sl = new_sl
                    notify(f"🔁 Rolled OCO (abs-step): new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty}")
                else:
                    notify("⚠️ Roll attempt failed; previous orders are cancelled. Will try to re-place previous OCO on next loop.")
                    time.sleep(0.4)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        notify("ℹ️ Fallback OCO re-placed after failed roll.")
                    else:
                        notify("❌ Fallback OCO also failed; skipping and TEMP skipping symbol.")
                        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0

# -------------------------
# MAIN TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 60

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF

    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            cleanup_recent_buys()

            open_orders_global = get_open_orders_cached()
            if open_orders_global:
                notify("⏳ Still waiting for previous trade(s) to finish (open orders present)...")
                time.sleep(300)
                continue

            if ACTIVE_SYMBOL is not None:
                notify(f"⏳ Active trade in progress for {ACTIVE_SYMBOL}, skipping new buys.")
                time.sleep(CYCLE_DELAY)
                continue

            now = time.time()
            if now - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            candidate = pick_coin()
            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                if RATE_LIMIT_BACKOFF:
                    notify(f"⏸ Backing off due to prior rate-limit for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    time.sleep(180)
                continue

            symbol, price, volume, change = candidate
            notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, vol≈{volume})")

            last = RECENT_BUYS.get(symbol)
            if last:
                if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                    notify(f"⏭️ Skipping {symbol} due to recent buy cooldown.")
                    time.sleep(CYCLE_DELAY)
                    continue
                if last.get('price') and price > last['price'] * (1 + REBUY_MAX_RISE_PCT / 100.0):
                    notify(f"⏭️ Skipping {symbol} because price rose >{REBUY_MAX_RISE_PCT}% since last buy.")
                    time.sleep(CYCLE_DELAY)
                    continue

            free_usdt = get_free_usdt()
            usd_to_buy = min(TRADE_USD, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={free_usdt:.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            buy_res = place_safe_market_buy(symbol, usd_to_buy)
            if not buy_res or buy_res == (None, None):
                notify(f"ℹ️ Buy skipped/failed for {symbol}.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}, skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}

            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            if not f:
                notify(f"⚠️ Could not fetch filters for {symbol} after buy; aborting monitoring for safety.")
                ACTIVE_SYMBOL = None
                time.sleep(CYCLE_DELAY)
                continue

            micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None
            try:
                micro_order, micro_sold_qty, micro_tp_price = place_micro_tp(symbol, qty, entry_price, f)
            except Exception as e:
                notify(f"⚠️ Micro TP placement error: {e}")
                micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None

            qty_remaining = round_step(max(0.0, qty - micro_sold_qty), f.get('stepSize', 0.0))
            if qty_remaining <= 0 or qty_remaining < f.get('minQty', 0.0):
                notify(f"ℹ️ Nothing left to monitor after micro TP for {symbol} (qty_remaining={qty_remaining}).")
                ACTIVE_SYMBOL = symbol
                LAST_BUY_TS = time.time()
                ACTIVE_SYMBOL = None
                continue

            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            total_profit_usd = profit_usd or 0.0
            if micro_order and micro_sold_qty and micro_tp_price is not None:
                try:
                    micro_profit = (micro_tp_price - entry_price) * micro_sold_qty
                    total_profit_usdt = micro_profit
                    total_profit_usd += micro_profit
                except Exception:
                    pass

            now2 = time.time()
            ent = RECENT_BUYS.get(symbol, {})
            ent['ts'] = now2
            ent['price'] = entry_price
            ent['profit'] = total_profit_usd
            if ent['profit'] is None:
                ent['cooldown'] = REBUY_COOLDOWN
            elif ent['profit'] < 0:
                ent['cooldown'] = LOSS_COOLDOWN
            else:
                ent['cooldown'] = REBUY_COOLDOWN
            RECENT_BUYS[symbol] = ent

            if closed and total_profit_usd and total_profit_usd > 0:
                send_profit_to_funding(total_profit_usd)

            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(
                    RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                    RATE_LIMIT_BACKOFF_MAX
                )
                notify(f"❌ Rate limit reached in trade_cycle: backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue

            notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)

        time.sleep(30)

# -------------------------
# FLASK KEEPALIVE
# -------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()
