import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
from requests import Session

# --- 設定エリア（Git運用に最適化） ---
USER_AGENT = 'Minervini-Bot/Git-Full-v2 (contact: gozihiro17@gmail.com)'
LOCAL_SAVE_PATH = 'minervini_final_results.csv'
BATCH_SIZE = 50     # 50銘柄ずつまとめてダウンロード
BATCH_SLEEP = 30    # バッチ間の待機時間（秒）。5.5時間で1万件を完走する計算
# --------------------------------

def get_market_health_summary(session):
    """【維持】市場環境判定：S&P500のSMA50と売り抜け日"""
    targets = ["^GSPC", "SPY"]
    for target in targets:
        try:
            idx = yf.download(target, period="1y", progress=False, auto_adjust=True, session=session)
            if idx.empty: continue
            if isinstance(idx.columns, pd.MultiIndex): idx.columns = idx.columns.get_level_values(0)
            c, v = idx['Close'].squeeze(), idx['Volume'].squeeze()
            sma50 = c.rolling(50).mean().iloc[-1]
            dist_days = 0
            for i in range(1, 26):
                if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1]: dist_days += 1
            status = "強気" if c.iloc[-1] > sma50 and dist_days < 5 else "警戒"
            return f"--- 市場環境: {status} (判定源: {target} / 売り抜け: {dist_days}日) ---"
        except: continue
    return "--- 市場環境: 判定不能 ---"

def get_full_universe(session):
    """SECから全銘柄リストを取得"""
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {'User-Agent': USER_AGENT, 'Host': 'www.sec.gov'}
    try:
        res = session.get(url, headers=headers, timeout=15)
        return [item['ticker'].replace('-', '.') for item in res.json().values()]
    except Exception as e:
        print(f"リスト取得失敗: {e}")
        return []

def run_screener():
    session = Session()
    session.headers.update({'User-Agent': USER_AGENT})
    
    print(get_market_health_summary(session))
    universe = get_full_universe(session)
    if not universe: return

    results = []
    print(f"スキャン開始: {len(universe)} 銘柄。完了まで約5.5時間を見込んでいます。")

    # --- 第一段階：書類選考（一括ダウンロード） ---
    for i in range(0, len(universe), BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            # 50銘柄一括取得。これで通信回数を1/50に削減
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, session=session, threads=True)
            
            if data.empty:
                print(f"データ取得失敗。一時停止します...")
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

                    # --- 佐藤さんの投資ロジック完全移植 ---
                    tags = []
                    # A. VCP_Original
                    is_stage2 = (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1])
                    sma200_rising = (sma200.iloc[-20:].diff().dropna() > 0).all()
                    vol_dry_up = (v.iloc[-3:] < vol_sma50.iloc[-3:]).all()
                    bbw = (c.rolling(20).std() * 4) / sma20
                    bbw_min = bbw.iloc[-1] == bbw.iloc[-20:].min()
                    if is_stage2 and sma200_rising and vol_dry_up and bbw_min:
                        tags.append("VCP_Original")

                    # B. パワープレイ / ハイ・ベース
                    if (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1]):
                        if (c.iloc[-1]/c.iloc[-40] >= 1.70 if len(c)>=40 else False) and (c.iloc[-1]/h.iloc[-40:].max() >= 0.75):
                            tags.append("PowerPlay(70%+)")
                        if (1.10 <= c.iloc[-1]/c.iloc[-10] <= 1.70 if len(c)>=10 else False) and (c.iloc[-1]/h.iloc[-10:].max() >= 0.90):
                            tags.append("High-Base")

                    # テクニカル合格銘柄のみ、詳細(info)を取得
                    if tags:
                        stock = yf.Ticker(ticker, session=session)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        
                        # 100B以下の銘柄に限定
                        if 0 < mkt_cap <= 100 * 1e9:
                            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
                            
                            # 判定ラベル
                            if rev_g is None or eps_g is None: f_label = "【要確認】不足"
                            elif rev_g >= 0.25 and eps_g >= 0.25: f_label = "【超優秀】クリア"
                            elif rev_g >= 0.25 or eps_g >= 0.25 or rev_g >= 0.50: f_label = "【良好】一部"
                            else: f_label = "【不足】低成長"

                            results.append({
                                "銘柄": ticker, "価格": round(c.iloc[-1], 2), "パターン": ", ".join(tags),
                                "成長性判定": f_label, "売上成長(%)": round(rev_g*100, 1) if rev_g else "不明",
                                "時価総額(B)": round(mkt_cap/1e9, 2)
                            })
                            print(f"【的中】 {ticker}")
                except: continue
        except: pass
        
        if i % 500 == 0: print(f"進捗: {i}/{len(universe)}...")
        # 完走のための「戦略的待機」
        time.sleep(BATCH_SLEEP + random.uniform(0, 5))

    # 保存
    df_final = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    df_final.to_csv(LOCAL_SAVE_PATH, index=False, encoding='utf-8-sig')
    print(f"全工程完了。的中数: {len(results)}")

if __name__ == "__main__":
    run_screener()
