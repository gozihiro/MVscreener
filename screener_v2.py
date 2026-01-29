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
    """
    【2026実戦仕様】市場環境判定：
    FTD無効化ロジック、売り抜け日優先判定、SMA50フィルターを統合。
    """
    log(">> ステップ1: 指数データ解析による市場環境の厳密判定を開始...")
    try:
        # 過去60日分のS&P500データを取得
        idx = yf.download("^GSPC", period="60d", progress=False, auto_adjust=True)
        if idx.empty: return "判定不能", 0, 0
        
        # yfinanceのマルチインデックス対策
        if isinstance(idx.columns, pd.MultiIndex):
            idx.columns = idx.columns.get_level_values(0)
        
        c = idx['Close'].squeeze()
        v = idx['Volume'].squeeze()
        changes = c.pct_change()
        
        # --- 1. 売り抜け日 (Distribution Days) のカウント ---
        # 過去25取引日において「価格下落 かつ 出来高増」の日を数える
        dist_days = sum(1 for i in range(1, 26) if c.iloc[-i] < c.iloc[-i-1] and v.iloc[-i] > v.iloc[-i-1])
        
        # --- 2. ラリーの起点（直近最安値）の特定 ---
        # 過去20日の中での最安値を「ラリー開始点」の候補とする
        window_20 = c.tail(20)
        low_val = window_20.min()
        # 最安値が何日前かを特定
        days_since_low = len(window_20) - 1 - window_20.argmin()
        
        # --- 3. FTD (Follow-Through Day) の探索と有効性チェック ---
        ftd_found = False
        rally_failed = False
        
        if days_since_low > 0:
            # 【重要】安値更新チェック
            # ラリー開始後に、その安値を一度でも下回っていたら「ラリー失敗」
            prices_since_low = c.tail(int(days_since_low) + 1)
            if (prices_since_low.iloc[1:] < low_val).any():
                rally_failed = True
            
            # ラリーが失敗していなければ、4日目以降にFTDがあったか探す
            if not rally_failed and days_since_low >= 4:
                # 安値から4日目〜現在までの期間をスキャン
                for i in range(int(days_since_low), 3, -1):
                    # 条件: 前日比+1.7%以上 かつ 出来高が前日を上回る
                    if changes.iloc[-i] >= 0.017 and v.iloc[-i] > v.iloc[-i-1]:
                        ftd_found = True
                        break

        # --- 4. 最終ステータスの決定 (優先順位を厳守) ---
        sma50 = c.rolling(50).mean().iloc[-1]
        curr_price = c.iloc[-1]
        
        # A. 最優先：上昇確定 (FTDあり 且つ SMA50の上)
        if ftd_found and curr_price > sma50:
            status = "🚀 上昇確定 (Confirmed Uptrend)"
            
        # B. 次点：下落警戒 (売り抜け日が6日以上)
        # ※たとえFTDが出ていても、売り抜け日が多ければこちらを優先して警告する
        elif dist_days >= 6:
            status = "🔴 下落警戒 (Market Under Pressure)"
            
        # C. ラリー継続中 (安値を割っておらず、FTD待ちの状態)
        elif days_since_low > 0 and not rally_failed and not ftd_found:
            status = "🟡 ラリー試行中 (Rally Attempt)"
            
        # D. ラリー失敗または安値更新中
        else:
            if curr_price < sma50:
                status = "📉 下落トレンド (Downtrend)"
            else:
                status = "🔄 調整中 (Correcting)"
                
        return status, dist_days, int(days_since_low)

    except Exception as e:
        log(f"❌ 市場判定エラー: {e}")
        return "エラー停止", 0, 0
        
def get_full_universe():
    """SECから主要取引所（Nasdaq, NYSE, NYSE American）の銘柄リストのみを取得"""
    log(">> ステップ2: 主要市場（Nasdaq/NYSE）の銘柄リストを取得中...")
    
    # 取引所情報が含まれる拡張版のファイルを使用します
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    headers = {'User-Agent': SEC_USER_AGENT, 'Host': 'www.sec.gov'}
    
    try:
        res = requests.get(url, headers=headers, timeout=25)
        json_data = res.json()
        
        # 監視対象を主要な取引所に限定（OTCやBATSなどのマイナー市場を除外）
        allowed_exchanges = ['Nasdaq', 'NYSE', 'NYSE American']
        
        tickers = [
            row[2].replace('-', '.') # index 2 が ticker
            for row in json_data['data'] 
            if row[3] in allowed_exchanges # index 3 が exchange
        ]
        
        log(f">> ✅ 主要市場から {len(tickers)} 銘柄を特定（OTC等を除外完了）。")
        return tickers
    except Exception as e:
        log(f"【エラー】リスト取得失敗: {e}")
        return []

def run_screener():
    log("=== スクリーナー起動（完走優先モード） ===")
    
    # 指数判定の取得
    mkt_status, dist_count, low_days = get_market_health_summary()
    market_summary = f"{mkt_status} (売り抜け:{dist_count}日 / 安値から:{low_days}日目)"
    log(f"--- 市場環境: {market_summary} ---")

    universe = get_full_universe()
    if not universe: return

    results = []
    advances, declines = 0, 0 # A/D用カウンター
    total = len(universe)
    log(f">> ステップ3: 全 {total} 銘柄のスキャンを開始。")
    log(f"    1バッチ（{BATCH_SIZE}銘柄）ごとに約90秒待機し、5.5時間かけて慎重に進みます。")

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            # 1バッチごとに必ずログを出力して生存報告
            log(f"    [進捗] {i}/{total} 分析中... (現在までの的中: {len(results)}件)")
            
            # 完走のため1回（1年分）のダウンロードに集約
            data = yf.download(batch, period="1y", interval="1d", progress=False, 
                               auto_adjust=True, threads=True, timeout=60)
            
            if data.empty:
                log(f"    [警告] バッチ {i} のデータが空です。制限回避のため120秒待機します。")
                time.sleep(120)
                continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    
                    # --- A/D カウント（フィルタリング前の生データで実施） ---
                    if len(df) >= 2:
                        if df['Close'].iloc[-1] > df['Close'].iloc[-2]: advances += 1
                        else: declines += 1

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
                        
                        # High-Base(Strict) の条件追加
                        is_high_base = (1.10 <= c.iloc[-1]/c.iloc[-10] <= 1.70 if len(c)>=10 else False) and (c.iloc[-1]/h.iloc[-10:].max() >= 0.90)
                        if is_high_base:
                            recent_explosion = (c.iloc[-5:].pct_change() >= 0.10).any()
                            if recent_explosion and vol_dry_up:
                                tags.append("High-Base(Strict)")
                            else:
                                tags.append("High-Base")

                    if tags:
                        stock = yf.Ticker(ticker)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        if 0 < mkt_cap <= 100 * 1e9:
                            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
                            
                            # --- EBITDA成長率と営業CFの取得ロジック強化 (ここを差し替え) ---
                            ebitda_g = info.get('ebitdaGrowth')
                            if ebitda_g is None:
                                try:
                                    qf = stock.quarterly_financials
                                    if 'EBITDA' in qf.index and qf.shape[1] >= 5:
                                        cur, prev = qf.loc['EBITDA'].iloc[0], qf.loc['EBITDA'].iloc[4]
                                        if prev and prev != 0: ebitda_g = (cur - prev) / abs(prev)
                                    elif 'Operating Income' in qf.index and qf.shape[1] >= 5:
                                        cur, prev = qf.loc['Operating Income'].iloc[0], qf.loc['Operating Income'].iloc[4]
                                        if prev and prev != 0: ebitda_g = (cur - prev) / abs(prev)
                                except: pass

                            ocf = info.get('operatingCashflow')
                            if ocf is None:
                                try:
                                    qf = stock.quarterly_financials
                                    if 'Operating Cash Flow' in qf.index:
                                        ocf = qf.loc['Operating Cash Flow'].iloc[0] * 4 # 年換算近似
                                except: pass
                            # --------------------------------------------------------
                            
                            if rev_g is None or eps_g is None: f_label = "【要確認】不足"
                            elif rev_g >= 0.25 and eps_g >= 0.25: f_label = "【超優秀】クリア"
                            elif rev_g >= 0.25 or eps_g >= 0.25 or rev_g >= 0.50: f_label = "【良好】一部"
                            else: f_label = "【不足】低成長"

                            results.append({
                                "銘柄": ticker, "価格": round(c.iloc[-1], 2), "パターン": ", ".join(tags),
                                "成長性判定": f_label, 
                                "売上成長(%)": round(rev_g*100, 1) if rev_g else "不明",
                                "営業利益成長(EBITDA)%": round(ebitda_g*100, 1) if ebitda_g else "不明",
                                "純利益成長(%)": round(eps_g*100, 1) if eps_g else "不明",
                                "営業CF(M)": round(ocf/1e6, 2) if ocf else "不明",
                                "時価総額(B)": round(mkt_cap/1e9, 2)
                            })
                            log(f"      > 【的中】: {ticker}")
                except: continue
        except Exception as e:
            log(f"    [エラー] バッチ {i}: {e}")
        
        # 次のバッチへの待機（平均90秒）
        time.sleep(BATCH_SLEEP_BASE + random.uniform(0, 10))

    # 市場の広がり (A/D) をレポートに追記
    ad_ratio = round(advances/max(1, declines), 2)
    special_msg = " 【!】内部改善中：先行銘柄をチェックせよ" if ad_ratio >= 1.5 and mkt_status in ["🟡 ラリー試行中 (Rally Attempt)", "🔴 下落警戒 (Market Under Pressure)"] else ""
    final_mkt_summary = f"{market_summary} | A/D比:{ad_ratio} (↑{advances} ↓{declines}){special_msg}"

    # 最終保存（1行目に市場環境評価を記録）
    df_final = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    with open(LOCAL_SAVE_PATH, 'w', encoding='utf-8-sig') as f:
        f.write(f"REPORT_METADATA,{final_mkt_summary}\n")
        df_final.to_csv(f, index=False)
    
    log(f"=== 全工程完了。最終的中数: {len(results)} ===")
    
    # --- OAuth 2.0 アップロード呼び出し ---
    upload_to_drive(LOCAL_SAVE_PATH)

if __name__ == "__main__":
    run_screener()
