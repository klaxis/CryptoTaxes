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
from typing import Iterable, List, Dict, Optional
try:
    # Python 3.8+
    from typing import TypedDict
except Exception:
    TypedDict = dict  # fallback

import urllib.request
import urllib.error

GEMINI_API_BASE = "https://api.gemini.com"

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
    """
    Build Gemini auth headers per docs:
      X-GEMINI-APIKEY
      X-GEMINI-PAYLOAD (base64 of JSON payload)
      X-GEMINI-SIGNATURE (HMAC-SHA384 of payload using api_secret)
      X-GEMINI-NONCE (string; milliseconds preferred)
    """
    payload = dict(payload)  # copy
    if 'request' not in payload:
        payload['request'] = path
    if 'nonce' not in payload:
        payload['nonce'] = str(int(time.time() * 1000))  # ms, as Gemini recommends

    raw = json.dumps(payload).encode('utf-8')
    b64 = base64.b64encode(raw)
    sig = hmac.new(api_secret.encode('utf-8'), b64, hashlib.sha384).hexdigest()
    return {
        'Content-Type': 'application/json',
        'X-GEMINI-APIKEY': api_key,
        'X-GEMINI-PAYLOAD': b64.decode('ascii'),
        'X-GEMINI-SIGNATURE': sig,
        'X-GEMINI-NONCE': payload['nonce'],
    }

def _http_post(path: str, payload: Dict, api_key: str, api_secret: str) -> Dict:
    url = GEMINI_API_BASE + path
    headers = _gemini_headers(path, payload, api_key, api_secret)
    req = urllib.request.Request(url, method='POST', headers=headers)
    try:
        with urllib.request.urlopen(req, data=b'') as resp:
            data = resp.read()
            return json.loads(data.decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='ignore')
        raise RuntimeError(f"Gemini HTTP {e.code} at {path} :: {body}") from None

def _http_get(path: str) -> Dict:
    url = GEMINI_API_BASE + path
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
        return json.loads(data.decode('utf-8'))

def _list_symbols() -> List[str]:
    # public endpoint
    syms = _http_get("/v1/symbols")
    # normalize to lowercase strings
    return [s.lower() for s in syms]

def read_gemini_api_trades(api_key: str, api_secret: str, since_ms: Optional[int]=None, limit_per_sym: int=500) -> List[Txn]:
    """
    Pull fills from /v1/mytrades for each symbol.
    since_ms: optional unix ms timestamp to limit history.
    """
    txns: List[Txn] = []
    for sym in _list_symbols():
        payload = {
            "request": "/v1/mytrades",
            "symbol": sym,
            "limit_trades": limit_per_sym
        }
        if since_ms:
            payload["since"] = str(since_ms)
        data = _http_post("/v1/mytrades", payload, api_key, api_secret)
        # Each row typically: price, amount, timestamp, type ('Buy'/'Sell'), fee_amount, fee_currency, trade_id, order_id
        for row in data:
            try:
                t_ms = int(row.get('timestampms') or row.get('timestamp') or 0)
                t = dt.datetime.fromtimestamp((t_ms/1000.0) if t_ms>10_000_000_000 else t_ms, tz=dt.timezone.utc)
                side = (row.get('type') or '').strip().upper()  # 'BUY'|'SELL'
                amount = float(row.get('amount') or 0.0)
                price = float(row.get('price') or 0.0)
                fee = float(row.get('fee_amount') or 0.0)
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
    """
    Pull funding from /v1/transfers
    """
    payload = {
        "request": "/v1/transfers"
    }
    if since_ms:
        payload["since"] = str(since_ms)
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
