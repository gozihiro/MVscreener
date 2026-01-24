import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
import sys

# --- 設定 ---
SEC_USER_AGENT = 'Minervini-Bot/Safe-v6 (contact: gozihiro17@gmail.com)'
SAVE_PATH = 'minervini_final_results.csv'
# -----------

def log(msg):
    """GitHubのログ画面に即座に出力する"""
    print(msg, flush=True)

def get_market_health():
    log(">> ステップ1: 市場環境（S&P500）の判定を開始...")
    for target in ["^GSPC", "SPY"]:
        try:
            # session引数を削除
            data = yf.download(target, period="1y", progress=False, auto_adjust=True, timeout=20)
            if data.empty: continue
            
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            c = data['Close'].squeeze()
            v = data['Volume'].squeeze()
            sma50 = c.rolling(50).mean().iloc[-1]
            dist_days = sum(1 for i in range(1, 26) if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1])
            status = "強気" if c.iloc[-1] > sma50 and dist_days < 5 else "警戒"
            return f"--- 市場環境: {status} ({target} / 売り抜け日: {dist_days}) ---"
        except Exception as e:
            log(f"   - {target} 取得エラー: {e}")
            continue
    return "--- 市場環境判定不能 ---"

def run():
    log("=== 修正済みスクリプト起動 ===")
    
    # 1. 市場判定
    log(get_market_health())
    
    # 2. SEC銘柄リスト取得（ここはrequestsを使用）
    log(">> ステップ2: SECから銘柄リストを取得中...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {'User-Agent': SEC_USER_AGENT, 'Host': 'www.sec.gov'}
        res = requests.get(url, headers=headers, timeout=25)
        universe = [item['ticker'].replace('-', '.') for item in res.json().values()]
        log(f"   - {len(universe)} 銘柄取得成功。")
    except Exception as e:
        log(f"【致命的エラー】リスト取得失敗: {e}")
        return

    results = []
    log(">> ステップ3: 全件スキャンを開始（1バッチ50銘柄 / 40秒待機）")

    # 3. 本番スキャン
    for i in range(0, len(universe), 50):
        batch = universe[i:i + 50]
        try:
            log(f"   [進捗] {i}/{len(universe)} スキャン中...")
            # yf.download から session 引数を削除
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, threads=True, timeout=60)
            
            if data.empty:
                log(f"   [警告] バッチ {i} のデータが空でした。60秒待機します。")
                time.sleep(60)
                continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    if len(df) < 200: continue
                    
                    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
                    sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
                    
                    # 簡易条件判定（ロジック維持）
                    if (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1]):
                        # 合格銘柄のみ詳細確認（session引数なし）
                        stock = yf.Ticker(ticker)
                        info = stock.info
                        if 0 < info.get('marketCap', 0) <= 100 * 1e9:
                            results.append({"銘柄": ticker, "価格": round(c.iloc[-1], 2)})
                            log(f"      > 的中: {ticker}")
                except: continue
        except Exception as e:
            log(f"   [エラー] バッチ {i} 処理中に例外: {e}")
        
        # 完走のための戦略的待機
        time.sleep(40 + random.uniform(0, 5))

    log("=== 全工程終了 ===")
    pd.DataFrame(results if results else [{"結果": "的中なし"}]).to_csv(SAVE_PATH, index=False)

if __name__ == "__main__":
    run()
