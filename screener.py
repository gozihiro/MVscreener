import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import os

# --- 設定エリア ---
USER_AGENT = 'Stock-Screener/Debug-v1 (contact: your-email@example.com)'
LOCAL_SAVE_PATH = 'minervini_final_results.csv'
# ----------------

def get_full_universe():
    headers = {'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate'}
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        return [item['ticker'].replace('-', '.') for item in res.json().values()]
    except Exception as e:
        print(f"【エラー】ティッカーリスト取得失敗: {e}")
        return []

def get_market_health_summary():
    try:
        idx = yf.download("^GSPC", period="1y", progress=False, auto_adjust=True)
        if isinstance(idx.columns, pd.MultiIndex): 
            idx.columns = idx.columns.get_level_values(0)
        c, v = idx['Close'].squeeze(), idx['Volume'].squeeze()
        sma50 = c.rolling(50).mean().iloc[-1]
        dist_days = 0
        for i in range(1, 26):
            if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1]: dist_days += 1
        return f"--- 市場環境: {c.iloc[-1] > sma50} (Dist: {dist_days}日) ---"
    except Exception as e:
        return f"--- 市場環境判定エラー: {e} ---"

def analyze_ticker_debug(ticker):
    """どこで脱落したかをログに出力するデバッグ版"""
    try:
        stock = yf.Ticker(ticker)
        
        # 1. info取得チェック
        info = stock.info
        if not info or 'marketCap' not in info:
            # ログが埋まるのを防ぐため、特定の銘柄（例: NVDA）の時だけ詳細を表示
            if ticker in ["NVDA", "AAPL", "MSFT", "TSLA"]:
                print(f"【デバッグ】{ticker}: info取得失敗（Yahooからブロックされている可能性があります）")
            return None

        mkt_cap = info.get('marketCap', 0)
        if mkt_cap == 0 or mkt_cap > 100 * 1e9: 
            return None

        # 2. 株価データ取得チェック
        df = stock.history(period="1y", interval="1d", auto_adjust=True)
        if df.empty or len(df) < 200:
            if ticker in ["NVDA", "AAPL", "MSFT", "TSLA"]:
                print(f"【デバッグ】{ticker}: 株価データ取得失敗（データが空です）")
            return None

        if isinstance(df.columns, pd.MultiIndex): 
            df.columns = df.columns.get_level_values(0)

        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
        ema10, vol_sma50 = c.ewm(span=10, adjust=False).mean(), v.rolling(50).mean()

        tags = []
        # --- テクニカル判定（ロジックは不変） ---
        is_stage2_vcp = (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1])
        sma200_rising = (sma200.iloc[-20:].diff().dropna() > 0).all()
        vol_dry_up = (v.iloc[-3:] < vol_sma50.iloc[-3:]).all()
        bbw = (c.rolling(20).std() * 4) / sma20
        bbw_min = bbw.iloc[-1] == bbw.iloc[-20:].min()
        if is_stage2_vcp and sma200_rising and vol_dry_up and bbw_min:
            tags.append("VCP_Original")

        if (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1]):
            gain_8w = (c.iloc[-1] / c.iloc[-40]) >= 1.70 if len(c) >= 40 else False
            on_ema10 = (c.iloc[-3:] > ema10.iloc[-3:]).all()
            max_40 = h.iloc[-40:].max()
            if gain_8w and on_ema10 and (c.iloc[-1] / max_40 >= 0.75):
                tags.append("PowerPlay(70%+)")
            gain_2w = 1.10 <= (c.iloc[-1] / c.iloc[-10]) <= 1.70 if len(c) >= 10 else False
            if gain_2w and (c.iloc[-1] / h.iloc[-10:].max() >= 0.90) and ((h.iloc[-3:] - l.iloc[-3:]).mean() < (h - l).rolling(10).mean().iloc[-1]):
                tags.append("High-Base")

        if tags:
            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
            return {
                "銘柄": ticker, "価格": round(c.iloc[-1], 2), "パターン": ", ".join(tags),
                "成長性判定": "判定済み", "売上成長(%)": round(rev_g * 100, 1) if rev_g else "不明",
                "EPS成長(%)": round(eps_g * 100, 1) if eps_g else "不明", "時価総額(B)": round(mkt_cap/1e9, 2)
            }
    except Exception as e:
        if ticker in ["NVDA", "AAPL", "MSFT", "TSLA"]:
            print(f"【デバッグ】{ticker}でエラー発生: {e}")
    return None

if __name__ == "__main__":
    print(get_market_health_summary())
    universe = get_full_universe()
    results = []
    print(f"全 {len(universe)} 銘柄のスキャン...")
    
    # 最初の数件でデータ取得ができているかテスト
    for ticker in ["AAPL", "NVDA", "MSFT", "TSLA"]:
        analyze_ticker_debug(ticker)

    for i, ticker in enumerate(universe):
        res = analyze_ticker_debug(ticker)
        if res:
            results.append(res)
            print(f"【的中】 {res['銘柄']}")
        if i % 100 == 0: print(f"Progress: {i}/{len(universe)}...")
        time.sleep(0.15) # ブロック回避のため少し長めに待機

    df = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    df.to_csv(LOCAL_SAVE_PATH, index=False, encoding='utf-8-sig')
    print(f"ファイル作成完了: {len(results)}件")
