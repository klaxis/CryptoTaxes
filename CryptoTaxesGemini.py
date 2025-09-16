# CryptoTaxes_patched_example.py
# Minimal example showing how to aggregate and include Gemini.
# Use this as a reference if you prefer to patch manually.

import os, sys, datetime as dt

# Import your existing readers here (coinbase/gdax/etc.)

# Gemini imports
try:
    from gemini_reader import read_gemini_from_csv, read_gemini_from_api
    try:
        from credentials import gemini_key, gemini_secret
    except Exception:
        gemini_key = gemini_secret = ""
except Exception:
    read_gemini_from_csv = read_gemini_from_api = None
    gemini_key = gemini_secret = ""

def main():
    all_txns = []

    # ... your existing readers populate all_txns ...

    # Gemini auto-wire
    if read_gemini_from_csv:
        trades_csv = "Gemini_TradeHistory.csv"
        transfers_csv = "Gemini_Transfers.csv"
        if os.path.exists(trades_csv):
            print("[Gemini] Loading from CSV:", trades_csv)
            all_txns.extend(read_gemini_from_csv(trades_csv, transfers_csv if os.path.exists(transfers_csv) else None))
        elif gemini_key and gemini_secret:
            print("[Gemini] Loading from API key/secret in credentials.py")
            all_txns.extend(read_gemini_from_api(gemini_key, gemini_secret))
        else:
            print("[Gemini] Skipped (no CSVs found and no API creds)")

    # Continue with your pipeline:
    # - normalize / sort
    # - export Form 8949 / reports
    all_txns.sort(key=lambda x: x["timestamp"])

    print(f"Total txns: {len(all_txns)}")
    # ... downstream processing ...

if __name__ == "__main__":
    main()
