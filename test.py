# from bdshare import get_current_trade_data, get_dsex_data, get_current_trading_code, get_company_info
import pandas as pd
from bdshare import get_current_trade_data, get_company_info, get_current_trading_code


def extract_outstanding_securities(live_df: list) -> str:
    """
    Robustly extract 'Total No. of Outstanding Securities'
    from the list of DataFrames returned by get_company_info().
    """
    ltp = live_df['ltp'].values[0]  # Last traded price
    market_cap = 10  # From annual report or DSE website

    outstanding = market_cap / ltp
    print(f"Estimated Outstanding Securities: {outstanding:,.0f}")

    return "N/A"


# ── Main ──────────────────────────────────────────────────────────────────────

codes = get_current_trading_code()['symbol'].tolist()
# codes = ['GP', 'BATBC', 'ROBI']   # ← uncomment to test with a small subset

results_list = []

for c in codes[:10]:
    try:
        comp_info = get_company_info(c)
        live_df   = get_current_trade_data(c)

        outstanding_shares = extract_outstanding_securities(live_df)

        if not live_df.empty:
            results_list.append({
                'Company':                            c,
                'Closing Price':                      live_df['close'].values[0],
                "Day's Value (mn)":                   live_df['value'].values[0],
                "Day's Volume (Nos.)":                live_df['volume'].values[0],
                'Total No. of Outstanding Securities': outstanding_shares,
            })
        else:
            print(f"[WARN] No live data for symbol: {c}")

    except Exception as e:
        print(f"[ERROR] {c}: {e}")

# ── Export ────────────────────────────────────────────────────────────────────

if results_list:
    df_final = pd.DataFrame(results_list)
    df_final.to_csv('dse_market_data.csv', index=False)
    print(df_final.to_string(index=False))
    print("\nCSV exported successfully → dse_market_data.csv")
else:
    print("No data collected.")

print("Done.")