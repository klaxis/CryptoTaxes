# -*- coding: utf-8 -*-
"""
Gemini reader for CryptoTaxes.
Supports:
  1) CSV exports (recommended; simplest + complete history)
  2) Private API (/v1/mytrades, /v1/transfers) using HMAC-SHA384

Outputs a common list[dict] of fills/txns compatible with the repo’s pipeline:
  {
    'exchange': 'GEMINI',
    'pair': 'BTC-USD',
    'type': 'BUY' | 'SELL' | 'DEPOSIT' | 'WITHDRAWAL' | 'FEE',
    'amount': float,             # base asset for trades; positive=buy, negative=sell
    'price': float | None,       # quote/base for trades; None for deposits/withdrawals/fees
    'fee': float,                # positive number
    'fee_currency': 'USD'|'BTC'|...,
    'timestamp': datetime (UTC),
    'tid': str,                  # trade/transfer id
  }

CSV notes:
- Use Gemini “Trade History” CSV and (optionally) “Transfers” CSV.
- Typical columns seen: Timestamp, Type, Symbol, Amount, Price, Fee, Fee Currency, Order ID, Trade ID, Client Order ID.

API notes:
- Provide API key/secret; we pull symbols, then call /v1/mytrades per symbol and /v1/transfers for funding.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import base64
import hmac
import hashlib
import time
import os
import urllib.request, urllib.error


from typing import Iterable, List, Dict, Optional
try:
    # Python 3.8+
    from typing import TypedDict
except Exception:
    TypedDict = dict  # fallback

import urllib.request
import urllib.error

GEMINI_API_BASE = "https://api.gemini.com"


REQUEST_TIMEOUT = 20        # seconds
MAX_RETRIES     = 3
BACKOFF_SECONDS = 2
_USER_AGENT     = "CryptoTaxes-Gemini/1.0 (+python-urllib)"
_last_nonce_ms  = 0



_PRICE_CACHE = {}  # (symbol, tf) -> list[candles]
_REQUEST_TIMEOUT = 20
_USER_AGENT = "CryptoTaxes-Gemini/1.0 (+python-urllib)"


class Txn(TypedDict, total=False):
    exchange: str
    pair: str
    type: str
    amount: float
    price: Optional[float]
    fee: float
    fee_currency: str
    timestamp: dt.datetime
    tid: str

# -------- CSV MODE ---------------------------------------------------------






def _next_nonce_ms() -> str:
    global _last_nonce_ms
    now = int(time.time() * 1000)
    if now <= _last_nonce_ms:
        now = _last_nonce_ms + 1
    _last_nonce_ms = now
    return str(now)


def _parse_gemini_timestamp(ts: str) -> dt.datetime:
    """
    Gemini CSV often uses ISO8601 like '2024-05-09 13:22:10 UTC' or
    '2024-05-09T13:22:10Z'. Support both.
    """
    s = ts.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            t = dt.datetime.strptime(s, fmt)
            if t.tzinfo is None:
                # Treat Z / UTC as UTC
                t = t.replace(tzinfo=dt.timezone.utc)
            return t.astimezone(dt.timezone.utc)
        except ValueError:
            continue
    # Last resort: try fromtimestamp if it looks numeric (ms or s)
    try:
        val = int(s)
        if val > 10_000_000_000:  # ms
            return dt.datetime.fromtimestamp(val / 1000.0, tz=dt.timezone.utc)
        return dt.datetime.fromtimestamp(val, tz=dt.timezone.utc)
    except Exception:
        pass
    raise ValueError(f"Unrecognized Gemini timestamp: {ts}")

def _symbol_to_pair(symbol: str) -> str:
    # Gemini symbols can be 'btcusd', 'ethusd', 'ethbtc' etc. Normalize to 'BTC-USD'
    s = symbol.strip().upper()
    # Known quote set (expand if needed)
    QUOTES = ("USD", "USDT", "USDC", "BTC", "ETH", "DAI", "EUR", "GBP")
    # Try to split by known suffix quote
    for q in sorted(QUOTES, key=len, reverse=True):
        if s.endswith(q):
            base = s[: -len(q)]
            return f"{base}-{q}"
    # Fallback: insert dash in middle
    if len(s) >= 6:
        return s[:3] + "-" + s[3:]
    return s

def read_gemini_csv_trades(path: str) -> List[Txn]:
    """
    Parse Gemini Trade History CSV.
    Expected columns (variable across exports): 'Timestamp', 'Type', 'Symbol',
    'Amount', 'Price', 'Fee', 'Fee Currency', 'Order ID', 'Trade ID'
    """
    txns: List[Txn] = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                t = _parse_gemini_timestamp(row.get('Timestamp') or row.get('Date') or row.get('time') or '')
                typ = (row.get('Type') or row.get('Side') or '').strip().upper()
                symbol = row.get('Symbol') or row.get('symbol') or ''
                pair = _symbol_to_pair(symbol)
                amount = float(row.get('Amount') or row.get('amount') or 0.0)
                price = row.get('Price') or row.get('price')
                price = float(price) if price not in (None, '',) else None
                fee = float(row.get('Fee') or row.get('fee') or 0.0)
                fee_ccy = (row.get('Fee Currency') or row.get('feeCurrency') or row.get('fee currency') or 'USD').upper()
                tid = (row.get('Trade ID') or row.get('trade_id') or row.get('Order ID') or row.get('order_id') or '').strip()

                # Map type to BUY/SELL amounts sign convention: positive=BUY, negative=SELL (base)
                if typ in ('BUY', 'MARGIN BUY'):
                    signed_amount = abs(amount)
                    trade_type = 'BUY'
                elif typ in ('SELL', 'MARGIN SELL'):
                    signed_amount = -abs(amount)
                    trade_type = 'SELL'
                else:
                    # Some CSVs include 'Rebate'/'Fee' rows in trade file; skip here (handled elsewhere)
                    continue

                txns.append(Txn(
                    exchange='GEMINI',
                    pair=pair,
                    type=trade_type,
                    amount=signed_amount,
                    price=price,
                    fee=abs(fee),
                    fee_currency=fee_ccy,
                    timestamp=t,
                    tid=tid or f"{pair}-{t.timestamp()}"
                ))
            except Exception:
                # Be forgiving: skip malformed rows
                continue
    return txns

def read_gemini_csv_transfers(path: str) -> List[Txn]:
    """
    Parse Gemini Transfers CSV (deposits/withdrawals).
    Typical columns: 'Timestamp','Type' ('Deposit'/'Withdrawal'), 'Currency','Amount','Fee'
    """
    txns: List[Txn] = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                t = _parse_gemini_timestamp(row.get('Timestamp') or row.get('Date') or '')
                typ = (row.get('Type') or '').strip().upper()
                ccy = (row.get('Currency') or row.get('currency') or '').strip().upper()
                amount = float(row.get('Amount') or 0.0)
                fee = float(row.get('Fee') or 0.0)
                tid = (row.get('Tx Hash') or row.get('TXID') or row.get('Transaction ID') or '').strip()

                entry: Txn = {
                    'exchange': 'GEMINI',
                    'pair': f'{ccy}-USD' if ccy and ccy != 'USD' else 'USD-USD',
                    'price': None,
                    'fee': abs(fee),
                    'fee_currency': ccy if fee else 'USD',
                    'timestamp': t,
                    'tid': tid or f"transfer-{ccy}-{t.timestamp()}"
                }
                if typ.startswith('DEP'):
                    entry['type'] = 'DEPOSIT'
                    entry['amount'] = abs(amount)
                elif typ.startswith('WITH'):
                    entry['type'] = 'WITHDRAWAL'
                    entry['amount'] = -abs(amount)
                else:
                    continue
                txns.append(entry)  # add
            except Exception:
                continue
    return txns

# -------- API MODE ---------------------------------------------------------

def _gemini_headers(path: str, payload: Dict, api_key: str, api_secret: str) -> Dict[str, str]:
    payload = dict(payload)
    payload.setdefault('request', path)
    payload.setdefault('nonce', _next_nonce_ms())

    raw = json.dumps(payload).encode('utf-8')
    b64 = base64.b64encode(raw)
    sig = hmac.new(api_secret.encode('utf-8'), b64, hashlib.sha384).hexdigest()
    return {
        'Content-Type': 'application/json',
        'X-GEMINI-APIKEY': api_key,
        'X-GEMINI-PAYLOAD': b64.decode('ascii'),
        'X-GEMINI-SIGNATURE': sig,
        'X-GEMINI-NONCE': payload['nonce'],
        'User-Agent': _USER_AGENT,
    }

import urllib.request, urllib.error, time

def _http_post(path: str, payload: Dict, api_key: str, api_secret: str) -> Dict:
    url = GEMINI_API_BASE + path
    headers = _gemini_headers(path, payload, api_key, api_secret)
    for attempt in range(1, MAX_RETRIES + 1):
        req = urllib.request.Request(url, method='POST', headers=headers)
        try:
            with urllib.request.urlopen(req, data=b'', timeout=REQUEST_TIMEOUT) as resp:
                data = resp.read()
                return json.loads(data.decode('utf-8'))
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='ignore')
            # Forward 4xx/5xx details; do not retry 4xx except 429
            if e.code != 429 or attempt == MAX_RETRIES:
                raise RuntimeError(f"Gemini HTTP {e.code} at {path} :: {body}") from None
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
        time.sleep(BACKOFF_SECONDS * attempt)

def _http_get(path: str) -> Dict:
    url = GEMINI_API_BASE + path
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': _USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                data = resp.read()
                return json.loads(data.decode('utf-8'))
        except Exception:
            if attempt == MAX_RETRIES:
                raise
        time.sleep(BACKOFF_SECONDS * attempt)


def _list_symbols() -> List[str]:
    # public endpoint
    syms = _http_get("/v1/symbols")
    # normalize to lowercase strings
    return [s.lower() for s in syms]

def read_gemini_api_trades(api_key: str, api_secret: str, since_ms: Optional[int]=None, limit_per_sym: int=500) -> List[Txn]:
    txns: List[Txn] = []
    symbols = _list_symbols()
    for sym in symbols:
        payload = {
            "request": "/v1/mytrades",
            "symbol": sym,
            "limit_trades": limit_per_sym
        }
        # Gemini historically accepted 'timestamp' or 'since' (ms). We send both.
        if since_ms:
            payload["since"] = str(since_ms)
            payload["timestamp"] = str(since_ms // 1000)

        try:
            data = _http_post("/v1/mytrades", payload, api_key, api_secret)
        except RuntimeError as e:
            # Some symbols may be inactive for your account; skip gracefully
            continue

        for row in data:
            try:
                t_ms = int(row.get('timestampms') or row.get('timestamp') or 0)
                if t_ms < (since_ms or 0):
                    continue
                t = dt.datetime.fromtimestamp((t_ms / 1000.0), tz=dt.timezone.utc)
                side = (row.get('type') or '').strip().upper()
                amount = float(row.get('amount') or 0.0)
                price  = float(row.get('price') or 0.0)
                fee    = float(row.get('fee_amount') or 0.0)
                fee_ccy = (row.get('fee_currency') or 'USD').upper()
                trade_id = str(row.get('tid') or row.get('trade_id') or row.get('order_id') or '')
                pair = _symbol_to_pair(sym)
                signed_amount = abs(amount) if side == 'BUY' else -abs(amount)
                txns.append(Txn(
                    exchange='GEMINI',
                    pair=pair,
                    type=side,
                    amount=signed_amount,
                    price=price,
                    fee=abs(fee),
                    fee_currency=fee_ccy,
                    timestamp=t,
                    tid=trade_id or f"{pair}-{t.timestamp()}"
                ))
            except Exception:
                continue
    return txns



def read_gemini_api_transfers(api_key: str, api_secret: str, since_ms: Optional[int]=None) -> List[Txn]:
    payload = {"request": "/v1/transfers"}
    if since_ms:
        payload["since"] = str(since_ms)
        payload["timestamp"] = str(since_ms // 1000)
    data = _http_post("/v1/transfers", payload, api_key, api_secret)
    txns: List[Txn] = []
    for row in data:
        try:
            t_ms = int(row.get('timestampms') or row.get('timestamp') or 0)
            t = dt.datetime.fromtimestamp((t_ms/1000.0) if t_ms>10_000_000_000 else t_ms, tz=dt.timezone.utc)
            typ = (row.get('type') or '').strip().upper() # 'Deposit' or 'Withdrawal'
            ccy = (row.get('currency') or '').strip().upper()
            amount = float(row.get('amount') or 0.0)
            fee = float(row.get('fee') or 0.0)
            eid = str(row.get('eid') or row.get('txHash') or row.get('txhash') or '')
            entry: Txn = {
                'exchange': 'GEMINI',
                'pair': f'{ccy}-USD' if ccy and ccy != 'USD' else 'USD-USD',
                'price': None,
                'fee': abs(fee),
                'fee_currency': ccy if fee else 'USD',
                'timestamp': t,
                'tid': eid or f"transfer-{ccy}-{t.timestamp()}"
            }
            if typ.startswith('DEP'):
                entry['type'] = 'DEPOSIT'
                entry['amount'] = abs(amount)
            elif typ.startswith('WITH'):
                entry['type'] = 'WITHDRAWAL'
                entry['amount'] = -abs(amount)
            else:
                continue
            txns.append(entry)
        except Exception:
            continue
    return txns


    # ... (unchanged loop; keep your try/except and timestamp filter same as above) ...



# --- Seamless integration with CryptoTaxes.py ---
 

def _txn_to_order(txn):
    """
    Convert a normalized Gemini txn dict to the format cost_basis.py expects:
    [order_time, product, 'buy'|'sell', cost_in_quote_ccy, amount, price_per_coin, exchange_currency]
    """
    t = txn['timestamp']
    pair = (txn.get('pair') or '').upper()
    if '-' in pair:
        base, quote = pair.split('-', 1)
    else:
        # crude fallback
        base, quote = pair[:-3], pair[-3:]
    side = (txn['type'] or '').lower()
    if side not in ('buy', 'sell'):
        return None  # ignore deposits/withdrawals here

    amt = float(txn['amount'])              # base units
    price = float(txn.get('price') or 0.0)  # quote/base
    fee = float(txn.get('fee') or 0.0)
    fee_ccy = (txn.get('fee_currency') or quote).upper()

    gross = abs(amt) * price  # in quote currency

    # Only net the fee if it's charged in the same quote currency;
    # (if fee is in crypto, we don't convert it here)
    fee_in_quote = fee if fee_ccy == quote else 0.0

    if side == 'buy':
        cost = gross + fee_in_quote        # spent in quote ccy
        amount_field = abs(amt)            # positive
    else:  # sell
        cost = gross - fee_in_quote        # proceeds in quote ccy
        amount_field = abs(amt)            # remaining volume tracked as positive

    return [t, base, side, round(cost, 10), round(amount_field, 10),
            round(price, 10), quote]

def get_buys_sells(trades_csv: str = "Gemini_TradeHistory.csv",
                   use_api_fallback: bool = True):
    """
    Returns (buys, sells) in the exact structure CryptoTaxes expects.
    Tries CSV first, then API if allowed and credentials are present.
    """
    buys, sells = [], []
    txns = []

    try:
        if os.path.exists(trades_csv):
            # If you also export transfers, you can pass transfers_csv=... here
            txns = read_gemini_from_csv(trades_csv, transfers_csv=None)
        elif use_api_fallback:
            try:
                from credentials import gemini_key, gemini_secret
            except Exception:
                gemini_key = gemini_secret = ""
            if gemini_key and gemini_secret:
                txns = read_gemini_from_api(gemini_key, gemini_secret, since=None)
    except Exception:
        txns = []

    for txn in txns:
        order = _txn_to_order(txn)
        if not order:
            continue
        if order[2] == 'buy':
            buys.append(order)
        elif order[2] == 'sell':
            sells.append(order)

    return buys, sells




# -------- PUBLIC API -------------------------------------------------------

def read_gemini_from_csv(trades_csv: str, transfers_csv: Optional[str]=None) -> List[Txn]:
    txns = read_gemini_csv_trades(trades_csv)
    if transfers_csv:
        txns.extend(read_gemini_csv_transfers(transfers_csv))
    # sort chronologically
    txns.sort(key=lambda x: x['timestamp'])
    return txns

def read_gemini_from_api(api_key: str, api_secret: str, since: Optional[dt.datetime]=None) -> List[Txn]:
    since_ms = int(since.timestamp()*1000) if since else None
    txns = read_gemini_api_trades(api_key, api_secret, since_ms=since_ms)
    txns.extend(read_gemini_api_transfers(api_key, api_secret, since_ms=since_ms))
    txns.sort(key=lambda x: x['timestamp'])
    return txns

# update the high-level bridge
def get_buys_sells(trades_csv: str = "Gemini_TradeHistory.csv",
                   use_api_fallback: bool = True,
                   since_year: Optional[int] = None):
    """
    Returns (buys, sells) in the structure CryptoTaxes expects.
    If CSV absent and use_api_fallback=True, calls the API filtered by since_year.
    """
    buys, sells = [], []
    txns = []
    since_dt = None
    if since_year:
        since_dt = dt.datetime(since_year, 1, 1, tzinfo=dt.timezone.utc)

    try:
        if os.path.exists(trades_csv):
            txns = read_gemini_from_csv(trades_csv, transfers_csv=None)
        elif use_api_fallback:
            try:
                from credentials import gemini_key, gemini_secret
            except Exception:
                gemini_key = gemini_secret = ""
            if gemini_key and gemini_secret:
                txns = read_gemini_from_api(gemini_key, gemini_secret, since=since_dt)
    except Exception:
        txns = []

    for txn in txns:
        order = _txn_to_order(txn)
        if not order:
            continue
        if order[2] == 'buy':
            buys.append(order)
        elif order[2] == 'sell':
            sells.append(order)

    return buys, sells

import urllib.request, json, datetime as dt

def _candles(symbol: str, tf: str = "1m"):
    """Return v2 candles list for symbol, or [] on error."""
    try:
        url = f"https://api.gemini.com/v2/candles/{symbol.lower()}/{tf}"
        with urllib.request.urlopen(url, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return []



def _fetch_candles(symbol: str, tf: str) -> list:
    """
    Fetch Gemini v2 candles for symbol/timeframe, with simple caching & retries.
    Returns a list of candles: [timestamp_ms, open, high, low, close, volume].
    """
    key = (symbol.lower(), tf)
    if key in _PRICE_CACHE:
        return _PRICE_CACHE[key]

    url = f"https://api.gemini.com/v2/candles/{symbol.lower()}/{tf}"
    hdrs = {"User-Agent": _USER_AGENT}
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
                if isinstance(data, list):
                    _PRICE_CACHE[key] = data
                    return data
        except Exception:
            if attempt == 2:
                return []
            time.sleep(1.5 * (attempt + 1))
    return []


def _closest_close(candles: list, ts_ms: int) -> float:
    """
    Return the close price of the candle with timestamp closest to ts_ms.
    If candles empty, return 0.0
    """
    if not candles:
        return 0.0
    best = None
    best_dist = None
    for c in candles:
        c_ts = int(c[0])
        dist = abs(c_ts - ts_ms)
        if best is None or dist < best_dist:
            best = c
            best_dist = dist
    return float(best[4]) if best else 0.0

def get_btc_price(ts: dt.datetime) -> float:
    """
    Historical BTC/USD price near the given UTC datetime, using Gemini candles.
    Tries multiple timeframes to widen history; picks the closest candle.
    """
    ts_ms = int(ts.timestamp() * 1000)
    for tf in ("1m", "5m", "15m", "1h", "6h", "1d"):
        px = _closest_close(_fetch_candles("btcusd", tf), ts_ms)
        if px > 0:
            return px
    # Last resort: current ticker (only if all candles failed)
    try:
        req = urllib.request.Request("https://api.gemini.com/v1/pubticker/btcusd",
                                     headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
            return float(data.get("last", 0.0))
    except Exception:
        return 0.0


def get_usd_per_quote(quote: str, ts: dt.datetime) -> float:
    """
    USD per 1 unit of 'quote' at (or near) time ts.
    Strategy:
      - USD: 1.0
      - Stablecoins (USDT/USDC/DAI): try candles; else 1.0
      - BTC: use get_btc_price(ts)
      - Direct {quote}USD candles if available
      - Else via BTC bridge: {quote}BTC * BTCUSD (careful with direction; we use 1 / (BTC/{quote}))
    """
    q = (quote or "").upper()
    if not q or q == "USD":
        return 1.0

    ts_ms = int(ts.timestamp() * 1000)

    # Stablecoins ~ $1 but try candles first
    if q in ("USDT", "USDC", "DAI"):
        for tf in ("1m", "5m", "15m", "1h", "6h", "1d"):
            px = _closest_close(_fetch_candles(f"{q.lower()}usd", tf), ts_ms)
            if px > 0:
                return px
        return 1.0

    if q == "BTC":
        return get_btc_price(ts)

    # Try direct {quote}USD
    for tf in ("1m", "5m", "15m", "1h", "6h", "1d"):
        px = _closest_close(_fetch_candles(f"{q.lower()}usd", tf), ts_ms)
        if px > 0:
            return px

    # Fall back via BTC: pair is {quote}BTC (price is BTC quoted in quote)
    # Example: 'ethbtc' close = BTC per 1 ETH. We need USD per 1 ETH:
    #   (BTC per 1 ETH) * (USD per 1 BTC) = USD per 1 ETH
    for tf in ("1m", "5m", "15m", "1h", "6h", "1d"):
        qbtc = _closest_close(_fetch_candles(f"{q.lower()}btc", tf), ts_ms)
        if qbtc > 0:
            return qbtc * get_btc_price(ts)

    return 0.0



