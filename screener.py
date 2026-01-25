import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
import sys
# --- Added for OAuth 2.0 Drive Upload ---
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
# ----------------------------------------

# --- 設定エリア（ロジック不変・待機時間を完走用に最適化） ---
SEC_USER_AGENT = 'Minervini-Bot/Git-Full-v2 (contact: gozihiro17@gmail.com)'
LOCAL_SAVE_PATH = 'minervini_final_results.csv'
BATCH_SIZE = 50     
# 1万件（200回通信）を5.5時間で終えるための待機秒数（約90秒）
BATCH_SLEEP_BASE = 85 
# -----------------------------------------------------

def log(msg):
    """GitHubのログ画面に即座に出力する（バッファリング回避）"""
    print(msg, flush=True)

def upload_to_drive(file_path):
    """OAuth 2.0を使用してGoogle Driveへアップロード"""
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    refresh_token = os.environ.get('REFRESH_TOKEN')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')

    if not all([client_id, client_secret, refresh_token, folder_id]):
        log("【警告】Drive設定用の環境変数が不足しています。")
        return

    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri="https://oauth2.googleapis.com/token"
        )
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': file_path, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        log(f">> ✅ Google Driveアップロード成功")
    except Exception as e:
        log(f">> ❌ Driveアップロード失敗: {e}")

def get_market_health_summary():
    """【維持】市場環境判定：S&P500のSMA50と売り抜け日"""
    log(">> ステップ1: 市場環境の判定を開始...")
    for target in ["^GSPC", "SPY"]:
        try:
            idx = yf.download(target, period="1y", progress=False, auto_adjust=True, timeout=20)
            if idx.empty: continue
            if isinstance(idx.columns, pd.MultiIndex): idx.columns = idx.columns.get_level_values(0)
            c, v = idx['Close'].squeeze(), idx['Volume'].squeeze()
            sma50 = c.rolling(50).mean().iloc[-1]
            dist_days = sum(1 for i in range(1, 26) if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1])
            status = "強気" if c.iloc[-1] > sma50 and dist_days < 5 else "警戒"
            return f"--- 市場環境: {status} (判定源: {target} / 売り抜け: {dist_days}日) ---"
        except Exception as e:
            log(f"   - {target} 取得エラー: {e}")
            continue
    return "--- 市場環境: 判定不能 ---"

def get_full_universe():
    """SECから全銘柄リストを取得"""
    log(">> ステップ2: 銘柄リストを取得中...")
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {'User-Agent': SEC_USER_AGENT, 'Host': 'www.sec.gov'}
    try:
        res = requests.get(url, headers=headers, timeout=25)
        return [item['ticker'].replace('-', '.') for item in res.json().values()]
    except Exception as e:
        log(f"【エラー】リスト取得失敗: {e}")
        return []

def run_screener():
    log("=== スクリーナー起動（完走優先モード） ===")
    log(get_market_health_summary())
    universe = get_full_universe()
    if not universe: return

    results = []
    total = len(universe)
    log(f">> ステップ3: 全 {total} 銘柄のスキャンを開始。")
    log(f"   1バッチ（{BATCH_SIZE}銘柄）ごとに約90秒待機し、5.5時間かけて慎重に進みます。")

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            # 1バッチごとに必ずログを出力して生存報告
            log(f"   [進捗] {i}/{total} 分析中... (現在までの的中: {len(results)}件)")
            
            # yf.download から session を除外
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, threads=True, timeout=60)
            
            if data.empty:
                log(f"   [警告] バッチ {i} のデータが空です。制限回避のため120秒待機します。")
                time.sleep(120)
                continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    if len(df) < 200: continue
                    
                    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
                    sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
                    ema10, vol_sma50 = c.ewm(span=10, adjust=False).mean(), v.rolling(50).mean()

                    # --- 【ロジック維持】判定ロジックは一切変えません ---
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

                    if tags:
                        stock = yf.Ticker(ticker)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        if 0 < mkt_cap <= 100 * 1e9:
                            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
                            if rev_g is None or eps_g is None: f_label = "【要確認】不足"
                            elif rev_g >= 0.25 and eps_g >= 0.25: f_label = "【超優秀】クリア"
                            elif rev_g >= 0.25 or eps_g >= 0.25 or rev_g >= 0.50: f_label = "【良好】一部"
                            else: f_label = "【不足】低成長"

                            results.append({
                                "銘柄": ticker, "価格": round(c.iloc[-1], 2), "パターン": ", ".join(tags),
                                "成長性判定": f_label, "売上成長(%)": round(rev_g*100, 1) if rev_g else "不明",
                                "時価総額(B)": round(mkt_cap/1e9, 2)
                            })
                            log(f"      > 【的中】: {ticker}")
                except: continue
        except Exception as e:
            log(f"   [エラー] バッチ {i}: {e}")
        
        # 次のバッチへの待機（平均90秒）
        time.sleep(BATCH_SLEEP_BASE + random.uniform(0, 10))

    # 最終保存
    df_final = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    df_final.to_csv(LOCAL_SAVE_PATH, index=False, encoding='utf-8-sig')
    log(f"=== 全工程完了。最終的中数: {len(results)} ===")
    
    # --- OAuth 2.0 アップロード呼び出し ---
    upload_to_drive(LOCAL_SAVE_PATH)

if __name__ == "__main__":
    run_screener()
