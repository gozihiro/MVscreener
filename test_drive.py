import yfinance as yf
import pandas as pd

def test_financial_data(ticker_symbol):
    print(f"=== Testing Data for: {ticker_symbol} ===")
    tk = yf.Ticker(ticker_symbol)

    # 1. .info からの取得 (従来の試行方法)
    info = tk.info
    print(f"[Method 1: .info]")
    print(f"  - EBITDA: {info.get('ebitda', 'N/A')}")
    print(f"  - Operating Margins: {info.get('operatingMargins', 'N/A')}")

    # 2. .financials (通期損益計算書) からの取得
    print(f"\n[Method 2: .financials (Annual)]")
    financials = tk.financials
    if not financials.empty:
        # EBITDA または Operating Income の行を探す
        target_rows = ['EBITDA', 'Operating Income', 'Ebit']
        for row in target_rows:
            if row in financials.index:
                val = financials.loc[row].iloc[0] # 最新年度
                print(f"  - {row}: {val}")
            else:
                print(f"  - {row}: Not Found in index")
    else:
        print("  - Annual Financials is empty.")

    # 3. .quarterly_financials (四半期損益計算書) からの取得
    print(f"\n[Method 3: .quarterly_financials (Quarterly)]")
    q_financials = tk.quarterly_financials
    if not q_financials.empty:
        if 'EBITDA' in q_financials.index:
            # 最新の4四半期分を表示
            print("  - Recent 4 Quarters EBITDA:")
            print(q_financials.loc['EBITDA'].head(4))
        else:
            print("  - EBITDA not found in quarterly index.")
    else:
        print("  - Quarterly Financials is empty.")

# テスト実行（データが豊富そうな大型銘柄で試すのが確実です）
test_symbols = ['AAPL', 'NVDA', 'VRT']
for symbol in test_symbols:
    test_financial_data(symbol)
    print("-" * 30)
