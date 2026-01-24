import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
from requests import Session

# --- 設定 ---
USER_AGENT = 'Stock-Screener/Gatekeeper-v3 (contact: gozihiro17@gmail.com)'
SAVE_PATH = 'minervini_final_results.csv'
BATCH_SIZE = 50     
BATCH_SLEEP = 40    # 1万件を約5.5時間で走破する設定
# -----------

def get_market_health(session):
    for target in ["^GSPC", "SPY"]:
        try:
            idx = yf.download(target, period="1y", progress=False, auto_adjust=True, session=session)
            if idx.empty: continue
            if isinstance(idx.columns, pd.MultiIndex): idx.columns = idx.columns.get_level_values(0)
            c, v = idx['Close'].squeeze(), idx['Volume'].squeeze()
            sma50 = c.rolling(50).mean().iloc[-1]
            dist_days = sum(1 for i in range(1, 26) if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1])
            return f"--- 市場環境: {'強気' if c.iloc[-1] > sma50 else '警戒'} (源: {target} / 売り抜け: {dist_days}日) ---"
        except: continue
    return "--- 市場環境判定不能 ---"

def run():
    session = Session()
    session.headers.update({'User-Agent': USER_AGENT})
    print(get_market_health(session))
    
    # 銘柄リスト取得
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {'User-Agent': USER_AGENT, 'Host': 'www.sec.gov'}
    universe = [item['ticker'].replace('-', '.') for item in session.get(url, headers=headers).json().values()]
    
    results = []
    total = len(universe)
    print(f"分析開始: {total} 銘柄")

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            # 1. テクニカルデータ一括取得 (超低負荷)
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, session=session, threads=True)
            
            if data.empty:
                print(f"Progress {i}: 通信エラー待機...")
                time.sleep(60)
                continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    if len(df) < 200: continue
                    
                    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
                    sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
                    ema10, vol_sma50 = c.ewm(span=10, adjust=False).mean(), v.rolling(50).mean()

                    # ロジック判定
                    tags = []
                    is_stage2 = (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1])
                    if is_stage2 and (sma200.iloc[-20:].diff().dropna() > 0).all() and \
                       (v.iloc[-3:] < vol_sma50.iloc[-3:]).all():
                        tags.append("VCP")
                    
                    # パワープレイ / ハイベース
                    if c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1]:
                        if (c.iloc[-1]/c.iloc[-40] >= 1.70 if len(c)>=40 else False): tags.append("PP")
                        if (1.10 <= c.iloc[-1]/c.iloc[-10] <= 1.70 if len(c)>=10 else False): tags.append("HB")

                    # 2. 合格銘柄のみ詳細(info)を取得 (重い処理)
                    if tags:
                        stock = yf.Ticker(ticker, session=session)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        if 0 < mkt_cap <= 100 * 1e9:
                            results.append({"銘柄": ticker, "パターン": ", ".join(tags), "価格": round(c.iloc[-1], 2)})
                            print(f"  > 的中: {ticker}")
                except: continue
        except: pass
        
        # 【重要】バッチごとに必ず進捗を表示
        print(f"進捗: {i + len(batch)}/{total} 完了 (的中累計: {len(results)})")
        time.sleep(BATCH_SLEEP + random.uniform(0, 5))

    pd.DataFrame(results if results else [{"結果": "的中なし"}]).to_csv(SAVE_PATH, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    run()
