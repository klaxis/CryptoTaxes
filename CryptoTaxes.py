# This will calculate cryptocurrency taxes based off coinbase, gdax, and bittrex logs
import gemini_reader
#import gdax_reader
#import bittrex_reader
#import coinbase_reader
import fill_8949
import pickle
import os
import turbo_tax
import argparse
import cost_basis
import dateutil.parser
import sys
import copy

from gemini_reader import read_gemini_from_csv, read_gemini_from_api
from credentials import gemini_key, gemini_secret


def fix_orders(orders):
    """
    Normalize exchange-quoted trades to USD and create a balancing trade
    in the quote asset (sell when we buy base; buy when we sell base).

    Input order shape:
      [t(datetime), product(str), side('buy'|'sell'),
       cost_in_quote(float), amount_base(float), price_per_coin_in_quote(float),
       quote_ccy(str)]

    Returns:
      buys_fixed, sells_fixed  # both lists of USD-denominated orders + balancing quote legs
    """
    buys_fixed = []
    sells_fixed = []

    for order in orders:
        try:
            t, product, side, cost_in_quote, amt_base, px_in_quote, quote = order
        except Exception:
            print("WEIRD! Malformed order:", order)
            continue

        # Normalize inputs
        side = (side or "").lower()
        quote = (quote or "USD").upper()
        product = (product or "").upper()
        if product == "BCC":
            product = "BCH"

        if side not in ("buy", "sell"):
            print("WEIRD! Unknown order buy/sell type!", order)
            continue

        # Get USD per 1 unit of the quote currency at trade time
        if quote == "USD":
            usd_per_quote = 1.0
        else:
            # requires gemini_reader.get_usd_per_quote(ts)
            #usd_per_quote = gemini_reader.get_usd_per_quote(quote, t)
            usd_per_quote = gemini_reader.get_usd_per_quote(order[6], order[0])
            cost_usd = float(order[3]) * float(usd_per_quote)
            if not usd_per_quote or usd_per_quote <= 0:
                # Fallbacks to avoid hard zeros (still prints a warning)
                if quote == "BTC":
                    usd_per_quote = gemini_reader.get_btc_price(t)
                if not usd_per_quote or usd_per_quote <= 0:
                    print(f"[WARN] Could not value quote '{quote}' at {t}. Using 0.0 — review this trade:", order)
                    usd_per_quote = 0.0

        # Convert to USD
        cost_usd = float(cost_in_quote) * float(usd_per_quote)
        # Guard divide-by-zero on amount
        cost_per_coin_usd = (cost_usd / float(amt_base)) if float(amt_base) else 0.0

        # Build the primary USD-denominated leg for the base product
        base_leg = [t, product, side, round(cost_usd, 10),
                    round(float(amt_base), 10), round(cost_per_coin_usd, 10), "USD"]

        # Build the balancing leg in the quote asset (only if non-USD quote)
        # Quantity of the quote leg is "cost_in_quote" units at price "usd_per_quote"
        if quote != "USD":
            quote_side = "sell" if side == "buy" else "buy"
            quote_leg = [t, quote, quote_side, round(cost_usd, 10),
                         round(float(cost_in_quote), 10), round(float(usd_per_quote), 10), "USD"]

        # Append to outputs
        if side == "buy":
            buys_fixed.append(base_leg)
            if quote != "USD":
                sells_fixed.append(quote_leg)  # we "sold" the quote to fund the buy
        else:  # side == 'sell'
            sells_fixed.append(base_leg)
            if quote != "USD":
                buys_fixed.append(quote_leg)   # we "bought" the quote from proceeds

    return buys_fixed, sells_fixed


if __name__ == '__main__':
    # Get the user's name and social security number
    # Parse potential inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('-name', default='Full Name', help='Your full name for filling out form 8949.')
    parser.add_argument('-social', default='123456789', help='Your social security number for filling out form 8949.')
    parser.add_argument('-year', type=int, default=2017, help='The tax year you want to fill out.')
    parser.add_argument('-startyear', type=int, default=0, help='The year to start looking for buy orders.  ' +
                                                                'Use this if you have the cost basis for previous ' +
                                                                'years (pass the filename with -costbasis)')
    parser.add_argument('-costbasis', default='', help='An optional file containing the cost basis of coins not ' +
                                                       'included in your GDAX, Coinbase, or Bittrex history.')
    parser.add_argument('--download', action='store_true', help='Use this flag to download the transaction history.  ' +
                                                                'Otherwise the data will be loaded from save.p')
    parser.add_argument('--turbotax', action='store_true', help='Use this flag to make a Turbo Tax txf import file.')
    parser.add_argument('--form8949', action='store_true', help='Use this flag to make the IRS form 8949 pdfs.')
    parser.add_argument('--saveorders', action='store_true', help='Use this flag to save the orders in a Python ' +
                                                                  'pickle file.')
    # Use a preset argument list if using pycharm console
    if 'pydevconsole' in sys.argv[0]:
        args = parser.parse_args([
                                  '-name', "Glenn Sugar",
                                  '-year', '2017',
                                  '-startyear', '2017'])
    else:
        args = parser.parse_args()

    if args.download:

        # ---------------- GEMINI-ONLY MODE ----------------
        # Honor -startyear for API pulls
        since_year = args.startyear if hasattr(args, "startyear") and args.startyear else None
        print(f"[Gemini] Loading via API since {since_year or 'beginning'}…")

        # IMPORTANT: this requires gemini_reader.get_buys_sells to support since_year (we added that)
        gemini_buys, gemini_sells = gemini_reader.get_buys_sells(
        trades_csv="Gemini_TradeHistory.csv",  # ignored if not present
        use_api_fallback=True,                 # force API if CSV not there
        since_year=since_year
            )

        # Stub out other exchanges to empty lists so the rest of the pipeline works unchanged
        coinbase_buys, coinbase_sells = [], []
        gdax_buys, gdax_sells         = [], []
        bittrex_buys, bittrex_sells   = [], []
        # -------------- END GEMINI-ONLY MODE --------------

        # Go through the buys and sells and see if they are coin-coin transactions
        # Fixed means that coin-coin transactions are now coin-usd, usd-coin
        print('Fixing coin-coin transactions...')
        print("Fixing coin-coin transactions...")

        # 1) Normalize Gemini orders only
        buys_fixed, sells_fixed = [], []
        for orders in [gemini_buys, gemini_sells]:
            b, s = fix_orders(orders)
            buys_fixed += b
            sells_fixed += s


        # 2) Sort the normalized USD orders
        buys_sorted  = sorted(buys_fixed,  key=lambda x: x[0])
        sells_sorted = sorted(sells_fixed, key=lambda x: x[0])

        # 3) Debug prints
        print(f"[DEBUG] Gemini buys: {len(gemini_buys)}, sells: {len(gemini_sells)}")
        print(f"[DEBUG] USD buys_sorted: {len(buys_sorted)}, sells_sorted: {len(sells_sorted)}")
        if buys_sorted[:1]:  print("[DEBUG] first USD buy:",  buys_sorted[0])
        if sells_sorted[:1]: print("[DEBUG] first USD sell:", sells_sorted[0])

        # 4) Compute totals
        net_cost    = sum(o[3] for o in buys_sorted)
        net_revenue = sum(o[3] for o in sells_sorted)
        net_gain    = net_revenue - net_cost

        # 5) Save state
        try:
            import pickle
            with open("save.p", "wb") as f:
                pickle.dump([buys_sorted, sells_sorted, buys_sorted + sells_sorted], f)
            with open("save_everything.p", "wb") as f:
                pickle.dump([buys_sorted, sells_sorted, buys_sorted + sells_sorted,
                             [], [], [], [], [], [],  # empty placeholders
                             gemini_buys, gemini_sells], f)
            print("Orders saved into save.p and save_everything.p")
        except Exception as e:
            print("[WARN] Could not save pickle files:", e)

        # 6) Call cost basis calc
        full_orders = cost_basis.get_cost_basis(
            copy.deepcopy(sells_sorted),
            copy.deepcopy(buys_sorted),
            basis_type='highest',
            tax_year=args.year
        )

        print(f"Done! Net Gain: {net_gain:.2f}, Net Revenue: {net_revenue:.2f}, Net Cost: {net_cost:.2f}")

    else:
        # Load the transaction data
        try:
         with open("save.p", "rb") as f:
            [buys_sorted, sells_sorted, full_orders,
            coinbase_buys, coinbase_sells,
            gdax_buys, gdax_sells,
            bittrex_buys, bittrex_sells,
            gemini_buys, gemini_sells] = pickle.load(open("save_everything.p", "rb"))
            #[buys_sorted, sells_sorted, full_orders] = pickle.load(f)
            print("Loaded previous state from save.p")
        except FileNotFoundError:
            print("No save.p found, building from scratch...")
            buys_sorted, sells_sorted, full_orders = [], [], []

        print('Buy and sell orders are loaded.')
        # See if any buy orders should be removed due to startyear input argument
        if args.startyear > 0:
            # Loop through the buys and remove any that are before the startyear
            buys_fixed_startyear = []
            startyear_buys = dateutil.parser.parse('%d/1/2 00:00:00Z' % args.startyear)
            for b in buys_sorted:
                if b[0] >= startyear_buys:
                    buys_fixed_startyear.append(b)
            buys_sorted = buys_fixed_startyear
        # See if we should add more buys via a costbasis file
        if len(args.costbasis) > 0:
            # parse the costbasis file
            other_buys = cost_basis.parse_cost_basis_file(args.costbasis)
            buys_sorted += other_buys
            buys_sorted = sorted(list(buys_sorted), key=lambda buy_order: buy_order[0])
        full_orders = cost_basis.get_cost_basis(copy.deepcopy(sells_sorted), copy.deepcopy(buys_sorted),
                                                basis_type='highest', tax_year=args.year)

    # Make the Turbo Tax import file
    if args.turbotax:
        print('Creating the Turbo Tax import file.')
        turbo_tax.make_txf(full_orders)

    # Make the 8949 forms
    if args.form8949:
        print('Creating the 8949 forms.')
        fill_8949.makePDF(full_orders, "test", args.name, args.social)

    # Get the net data (net cost basis, net revenue, net gain/loss
    net_cost = 0
    net_rev = 0
    net_gain = 0
    for o in full_orders:
        net_cost += o[4]
        net_rev += o[3]
        net_gain += o[5]

    # make a cumulative gain array for debugging purposes
    #cumulative_gains = []
    #running_gain = 0
    #for i in range(len(full_orders)):
    #     cumulative_gains.append(full_orders[i][5] + running_gain)
    #     running_gain += full_orders[i][5]
    #print('Done! Net Gain: %1.2f, Net Revenue: %1.2f, Net Cost: %1.2f' % (net_gain, net_rev, net_cost))
