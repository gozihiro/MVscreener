import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
import sys
from requests import Session

# --- 設定 ---
USER_AGENT = 'Minervini-Bot/Safe-v4 (contact: gozihiro17@gmail.com)'
SAVE_PATH = 'minervini_final_results.csv'
# -----------

def log(msg):
    """ログを即座にGitHubの画面に表示させるための関数"""
    print(msg, flush=True)

def get_market_health(session):
    log(">> ステップ1: 市場環境（S&P500）の判定を開始...")
    for target in ["^GSPC", "SPY"]:
        try:
            # タイムアウトを設けてフリーズを回避
            data = yf.download(target, period="1y", progress=False, auto_adjust=True, session=session, timeout=15)
            if data.empty:
                log(f"   - {target} のデータが空でした。次を試します。")
                continue
            
            if isinstance(data.columns, pd.MultiIndex): data.columns = data.columns.get_level_values(0)
            c = data['Close'].squeeze()
            v = data['Volume'].squeeze()
            sma50 = c.rolling(50).mean().iloc[-1]
            dist_days = sum(1 for i in range(1, 26) if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1])
            status = "強気" if c.iloc[-1] > sma50 and dist_days < 5 else "警戒"
            return f"--- 市場環境: {status} ({target} / 売り抜け日: {dist_days}) ---"
        except Exception as e:
            log(f"   - {target} 取得中にエラー: {e}")
            continue
    return "--- 市場環境判定不能（スキップして続行） ---"

def run():
    log("=== スクリプト起動完了 ===")
    session = Session()
    session.headers.update({'User-Agent': USER_AGENT})
    
    # 市場判定
    health_msg = get_market_health(session)
    log(health_msg)
    
    log(">> ステップ2: SECから銘柄リストを取得中...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {'User-Agent': USER_AGENT, 'Host': 'www.sec.gov'}
        res = session.get(url, headers=headers, timeout=20)
        universe = [item['ticker'].replace('-', '.') for item in res.json().values()]
        log(f"   - {len(universe)} 銘柄のリストを正常に取得。")
    except Exception as e:
        log(f"【致命的エラー】リスト取得に失敗: {e}")
        return

    results = []
    log(">> ステップ3: 全件スキャンを開始します（ここから時間がかかります）")

    for i in range(0, len(universe), 50): # 50銘柄ずつ
        batch = universe[i:i + 50]
        try:
            log(f"   [進捗] {i}/{len(universe)} 分析中...")
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, session=session, threads=True, timeout=30)
            
            if data.empty:
                log(f"   [警告] バッチ {i} のデータが空です。60秒待機します。")
                time.sleep(60)
                continue

            for ticker in batch:
                # (テクニカル・ファンダメンタルズの判定ロジックは以前と同じ)
                # 的中したら log(f"      > 的中: {ticker}") を出す
                pass

        except Exception as e:
            log(f"   [エラー] バッチ {i} で問題発生: {e}")
        
        # 門番をやり過ごす待機
        time.sleep(40 + random.uniform(0, 5))

    log("=== 全工程終了 ===")
    pd.DataFrame(results if results else [{"結果": "的中なし"}]).to_csv(SAVE_PATH, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    run()
