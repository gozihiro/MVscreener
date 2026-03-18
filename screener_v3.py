import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import random
import os
import sys
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- 設定エリア ---
SEC_USER_AGENT = 'Minervini-Bot/Git-Full-v3 (contact: gozihiro17@gmail.com)'
LOCAL_SAVE_PATH = 'minervini_final_results.csv'
BATCH_SIZE = 50
BATCH_SLEEP_BASE = 85

def log(msg):
    print(msg, flush=True)

def upload_to_drive(file_path, drive_file_name):
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
            token=None, refresh_token=refresh_token, client_id=client_id,
            client_secret=client_secret, token_uri="https://oauth2.googleapis.com/token"
        )
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': drive_file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        log(f">> ✅ Driveアップロード成功: {drive_file_name}")
    except Exception as e:
        log(f">> ❌ Driveアップロード失敗: {e}")

def calculate_launchpad_score(df, ticker, tags, index_change):
    """
    ミネルヴィニ/エルダー流『発射台スコア』算出 (0-10点)
    """
    if len(df) < 20: return 0
    
    # 直近データ
    c_today = df['Close'].iloc[-1]
    o_today = df['Open'].iloc[-1]
    h_today = df['High'].iloc[-1]
    l_today = df['Low'].iloc[-1]
    v_today = df['Volume'].iloc[-1]
    
    day_range = h_today - l_today
    if day_range == 0: return 0

    # 【重要追加条件】陽線判定：終値が始値以下（陰線または同値）なら、即座に0点として除外
    if c_today <= o_today:
        return 0
    
    # --- A. 共通基礎点 (最大6点) ---
    base_score = 0
    # 1. Closing Range (高値引け)
    closing_range = (c_today - l_today) / day_range
    if closing_range >= 0.8: base_score += 2
    
    # 2. Narrow Range (値幅収縮)
    avg_range_20 = (df['High'] - df['Low']).rolling(20).mean().iloc[-1]
    if day_range < (avg_range_20 * 0.8): base_score += 2
    
    # 3. No Upper Shadow (上ヒゲ排除)
    upper_shadow = h_today - max(o_today, c_today)
    if upper_shadow < (day_shadow_val := day_range * 0.1): base_score += 2

    # --- B. パターン別ボーナス (最大4点) ---
    bonus_vcp = 0
    if "VCP_3Steps_Validated" in tags or "VCP_Original" in tags:
        # VDU (Volume Dry-up)
        vol_sma50 = df['Volume'].rolling(50).mean().iloc[-1]
        if v_today < (vol_sma50 * 0.5): bonus_vcp += 2
        # Closing Tightness (終値の横並び)
        last_3_closes = df['Close'].tail(3)
        if last_3_closes.std() < (c_today * 0.005): bonus_vcp += 2

    bonus_hb = 0
    if any("High-Base" in t for t in tags):
        # MA Support (20日線付近)
        sma20 = df['Close'].rolling(20).mean().iloc[-1]
        if l_today <= (sma20 * 1.015): bonus_hb += 2
        # Shakeout (下ヒゲ振るい落とし)
        lower_shadow = min(o_today, c_today) - l_today
        body_size = abs(c_today - o_today)
        if lower_shadow > body_size: bonus_hb += 2

    bonus_pp = 0
    if "PowerPlay(70%+)" in tags:
        # Momentum Persistence (高値圏維持)
        high_5d = df['High'].tail(5).max()
        if c_today >= (high_5d * 0.98): bonus_pp += 2
        # Relative Strength (逆行高)
        stock_change = (c_today / df['Close'].iloc[-2]) - 1
        if stock_change >= 0 and index_change < 0: bonus_pp += 2

    final_score = base_score + max(bonus_vcp, bonus_hb, bonus_pp)
    return min(10, final_score)

def get_market_health_summary():
    """指数データ解析による市場環境判定"""
    log(">> ステップ1: 指数データ解析開始...")
    try:
        idx = yf.download("^GSPC", period="75d", progress=False, auto_adjust=True)
        if idx.empty: return "判定不能", 0, 0, 0
        if isinstance(idx.columns, pd.MultiIndex): idx.columns = idx.columns.get_level_values(0)
        
        c = idx['Close'].squeeze()
        v = idx['Volume'].squeeze()
        v_sma50 = v.rolling(50).mean()
        changes = c.pct_change()
        last_index_change = changes.iloc[-1]
        
        dist_days = 0
        for i in range(25, 0, -1):
            curr, prev = -i, -i-1
            if changes.iloc[curr] <= -0.002 and v.iloc[curr] > v.iloc[prev] and v.iloc[curr] > v_sma50.iloc[curr]:
                if not (c.iloc[curr+1:] >= c.iloc[curr] * 1.05).any(): dist_days += 1
        
        window_25 = c.tail(25)
        low_val = window_25.min()
        days_since_low = len(window_25) - 1 - window_25.argmin()
        
        ft_found = False
        if days_since_low >= 4:
            for i in range(int(days_since_low), 3, -1):
                if changes.iloc[-i] >= 0.015 and v.iloc[-i] > v.iloc[-i-1]:
                    ft_found = True; break

        sma50 = c.rolling(50).mean().iloc[-1]
        curr_price = c.iloc[-1]
        if ft_found and curr_price > sma50: status = "🚀 上昇確定 (Confirmed Uptrend)"
        elif dist_days >= 6: status = "🔴 下落警戒 (Market Under Pressure)"
        elif days_since_low > 0 and not (c.tail(int(days_since_low)+1).iloc[1:] < low_val).any() and not ft_found:
            status = "🟡 ラリー試行中 (Rally Attempt)"
        else: status = "📉 下落トレンド (Downtrend)" if curr_price < sma50 else "🔄 調整中 (Correcting)"
                
        return status, dist_days, int(days_since_low), last_index_change
    except Exception as e:
        log(f"❌ 市場判定エラー: {e}")
        return "エラー停止", 0, 0, 0

def get_jp_full_universe():
    """日本市場の全銘柄リスト取得"""
    log(">> ステップ1.1: 日本市場の全銘柄リスト取得中...")
    try:
        url = "https://raw.githubusercontent.com/ta9mar/jp-stock-codes/master/codes-all.csv"
        df = pd.read_csv(url)
        tickers = [f"{str(code)}.T" for code in df['code'].tolist()]
        return tickers
    except Exception as e:
        log(f"【エラー】日本市場銘柄リスト取得失敗: {e}")
        return []

def get_jp_market_summary(adv=0, dec=0, ad_ratio=0.0):
    """日本市場の環境解析（1321.Tによる指数判定。A/D比はメインループから受け取る）"""
    log(">> ステップ1.2: 日本市場の解析開始...")
    try:
        # 1. 指数データ取得
        idx = yf.download("1321.T", period="100d", progress=False, auto_adjust=True)
        if isinstance(idx.columns, pd.MultiIndex): idx.columns = idx.columns.get_level_values(0)
        c = idx['Close'].squeeze()
        v = idx['Volume'].squeeze()
        v_sma50 = v.rolling(50).mean()
        changes = c.pct_change()
        
        dist_days = 0
        for i in range(25, 0, -1):
            curr, prev = -i, -i-1
            if changes.iloc[curr] <= -0.004 and v.iloc[curr] > v.iloc[prev] and v.iloc[curr] > v_sma50.iloc[curr]:
                dist_days += 1
        
        ma200 = c.rolling(200).mean().iloc[-1] if len(c) >= 200 else c.mean()
        curr_p = c.iloc[-1]

        if curr_p < ma200 or dist_days >= 6: status = "🔴 下落警戒 (Market Under Pressure)"
        elif dist_days >= 4 or ad_ratio < 0.8: status = "🔶 上昇負担 (Uptrend Under Pressure)"
        else: status = "🚀 上昇確認 (Confirmed Uptrend)"

        summary_str = f"JP_METADATA,{status} | A/D比:{ad_ratio} (↑{adv} ↓{dec}) | 売り抜け:{dist_days}日"
        return summary_str
    except Exception as e:
        return f"JP_METADATA,解析エラー:{str(e)}"

def get_full_universe():
    """SECから主要取引所の銘柄リストを取得"""
    log(">> ステップ2: 銘柄リスト取得中...")
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    headers = {'User-Agent': SEC_USER_AGENT, 'Host': 'www.sec.gov'}
    try:
        res = requests.get(url, headers=headers, timeout=25)
        json_data = res.json()
        allowed = ['Nasdaq', 'NYSE', 'NYSE American']
        tickers = [row[2].replace('-', '.') for row in json_data['data'] if row[3] in allowed]
        log(f">> ✅ {len(tickers)} 銘柄を特定。")
        return tickers
    except Exception as e:
        log(f"【エラー】リスト取得失敗: {e}"); return []

def run_screener():
    log("=== スクリーナー V3 起動（発射台スコア搭載） ===")
    mkt_status, dist_count, low_days, index_change = get_market_health_summary()
    
    # 1. ユニバース統合
    universe = get_full_universe()
    jp_universe = get_jp_full_universe()
    if jp_universe:
        universe += jp_universe
        log(f">> 日本市場 {len(jp_universe)} 銘柄をユニバースに追加しました。")

    if not universe: return

    results = []
    advances, declines = 0, 0
    jp_advances, jp_declines = 0, 0
    total = len(universe)

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            log(f"    [進捗] {i}/{total} 分析中...")
            data = yf.download(batch, period="2y", interval="1d", progress=False, auto_adjust=True, threads=True)
            if data.empty: time.sleep(120); continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    if len(df) < 252: continue
                    
                    # A/Dカウンター
                    is_up = df['Close'].iloc[-1] > df['Close'].iloc[-2]
                    if ticker.endswith('.T'):
                        if is_up: jp_advances += 1
                        else: jp_declines += 1
                        # 日本株はAD比カウントのみ行い、以下の詳細スクリーニングには進まない
                        continue 
                    else:
                        if is_up: advances += 1
                        else: declines += 1

                    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
                    curr_p = c.iloc[-1]
                    
                    # 指標算出
                    sma20 = c.rolling(20).mean()
                    sma50 = c.rolling(50).mean()
                    sma150 = c.rolling(150).mean()
                    sma200 = c.rolling(200).mean()
                    vol_sma50 = v.rolling(50).mean()
                    high_52w = h.rolling(252).max().iloc[-1]
                    low_52w = l.rolling(252).min().iloc[-1]

                    tags = []
                    
                    # --- 1. ミネルヴィニ・トレンドテンプレート (厳密8条件) ---
                    template_ok = (
                        curr_p > sma50.iloc[-1] > sma150.iloc[-1] > sma200.iloc[-1] and
                        sma200.iloc[-1] > sma200.iloc[-20] and
                        curr_p >= low_52w * 1.30 and
                        curr_p >= high_52w * 0.75
                    )

                    # --- 2. 3段階VCP収縮判定 ---
                    def check_vcp_strict(lookback):
                        step = lookback // 3
                        d1 = (h.iloc[-lookback:-lookback+step].max() - l.iloc[-lookback:-lookback+step].min()) / h.iloc[-lookback:-lookback+step].max()
                        d2 = (h.iloc[-lookback+step:-step].max() - l.iloc[-lookback+step:-step].min()) / h.iloc[-lookback+step:-step].max()
                        d3 = (h.iloc[-step:].max() - l.iloc[-step:].min()) / h.iloc[-step:].max()
                        return (d1 > d3) and (d2 > d3) and (d3 < 0.10)

                    vcp_strict_ok = any([check_vcp_strict(lb) for lb in [60, 90, 120]])

                    # A. VCP_Original
                    if (curr_p > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1]) and \
                       (sma200.iloc[-1] > sma200.iloc[-20]) and \
                       (c.rolling(20).std()*4/sma20).iloc[-1] <= (c.rolling(20).std()*4/sma20).iloc[-20:].min() * 1.05:
                        tags.append("VCP_Original")

                    # B. PowerPlay & High-Base
                    if (curr_p > sma20.iloc[-1] > sma50.iloc[-1]):
                        if (curr_p/c.iloc[-40] >= 1.70) and (curr_p/h.iloc[-40:].max() >= 0.75):
                            tags.append("PowerPlay(70%+)")
                        
                        # 【修正】ハイベース判定：3-6週間(15-30日)の間、現在まで継続して終値が10%以内であるか
                        tight_duration = 0
                        for d_idx in range(1, 41): # 6週(30日)を超える銘柄を正確に除外するため40日までカウント
                            if len(c) < d_idx: break
                            win = c.tail(d_idx)
                            if (win.max() / win.min() <= 1.10): tight_duration = d_idx
                            else: break

                        if (1.10 <= curr_p/c.iloc[-20] <= 1.70) and (15 <= tight_duration <= 30):
                            if (c.iloc[-5:].pct_change() >= 0.10).any() and (v.iloc[-3:] < vol_sma50.iloc[-3:]).all():
                                tags.append(f"High-Base(Strict:{tight_duration}d)")
                            else:
                                tags.append(f"High-Base({tight_duration}d)")

                    # --- C. Micro-VCP (3-Day Silence) ---
                    p1, p2, p3 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
                    # Micro-VCP用のトレンドテンプレート（点データを使用）
                    is_micro_stage2 = (
                        curr_p > sma150.iloc[-1] and sma150.iloc[-1] > sma200.iloc[-1] and
                        sma200.iloc[-1] > sma200.iloc[-20] and curr_p > sma50.iloc[-1] and
                        curr_p > low_52w * 1.25 and curr_p > high_52w * 0.75
                    )
                    if is_micro_stage2:
                        day1_rebound = p3['Close'] > p3['Open']
                        atr_val = (h - l).rolling(14).mean().iloc[-3]
                        day2_range = p2['High'] - p2['Low']
                        day2_shakeout = (p2['Close'] < p2['Open']) and (day2_range > atr_val)
                        day3_range = p1['High'] - p1['Low']
                        is_quiet = (day3_range < day2_range * 0.6 and p1['Volume'] < p2['Volume'] * 0.7 and p1['Volume'] < vol_sma50.iloc[-1])
                        is_tight_candle = abs(p1['Close'] - p1['Open']) < day3_range * 0.5
                        
                        if day1_rebound and day2_shakeout and is_quiet and is_tight_candle:
                            risk_pct = ((p1['High'] - p1['Low']) / p1['High']) * 100
                            tags.append(f"Micro-VCP(T:{p1['High']:.2f}/S:{p1['Low']:.2f}/R:{risk_pct:.1f}%)")

                    # --- D. MovingAverage Squeeze (REED Style) ---
                    ema10 = c.ewm(span=10, adjust=False).mean()
                    ma10, ma20, ma50_v = ema10.iloc[-1], sma20.iloc[-1], sma50.iloc[-1]
                    ma_list = [ma10, ma20, ma50_v]
                    is_squeezed = (max(ma_list) / min(ma_list) <= 1.015)
                    if is_squeezed and curr_p > max(ma_list) and ema10.iloc[-1] > ema10.iloc[-2]:
                        tags.append("MA_Squeeze(REED)")

                    if tags:
                        if template_ok: tags.append("[Trend_OK]")
                        if vcp_strict_ok: tags.append("VCP_3Steps_Validated")

                        lp_score = calculate_launchpad_score(df, ticker, tags, index_change)
                        
                        stock = yf.Ticker(ticker)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        if 0 < mkt_cap <= 100 * 1e9:
                            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
                            op_cf = info.get('operatingCashflow')
                            ema10_val = c.ewm(span=10, adjust=False).mean().iloc[-1]
                            
                            f_label = "【超優秀】クリア" if (rev_g or 0) >= 0.25 and (eps_g or 0) >= 0.25 else "【良好】一部" if (rev_g or 0) >= 0.25 or (eps_g or 0) >= 0.25 else "【不足】低成長"
                            
                            results.append({
                                "銘柄": ticker, "価格": round(curr_p, 2), "パターン": ", ".join(tags),
                                "成長性判定": f_label, "売上成長(%)": round(rev_g*100, 1) if rev_g else "不明",
                                "営業利益成長(EBITDA)%": "不明",
                                "純利益成長(%)": round(eps_g*100, 1) if eps_g else "不明",
                                "営業CF(M)": round(op_cf/1e6, 2) if op_cf else "不明",
                                "時価総額(B)": round(mkt_cap/1e9, 2), "発射台スコア": lp_score,
                                "10EMA": round(ema10_val, 2),
                                "20SMA": round(sma20.iloc[-1], 2),
                                "50SMA": round(sma50.iloc[-1], 2)
                            })
                            log(f"      > 【的中】: {ticker} (Score: {lp_score})")
                except: continue
        except Exception as e: log(f"    [エラー] バッチ {i}: {e}")
        time.sleep(BATCH_SLEEP_BASE + random.uniform(0, 10))

    ad_ratio = round(advances/max(1, declines), 2)
    jp_ad_ratio = round(jp_advances/max(1, jp_declines), 2)
    
    final_mkt_summary = f"{mkt_status} (売り抜け:{dist_count}日 / 安値から:{low_days}日目) | A/D比:{ad_ratio} (↑{advances} ↓{declines})"
    jp_market_summary = get_jp_market_summary(adv=jp_advances, dec=jp_declines, ad_ratio=jp_ad_ratio)

    df_final = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    with open(LOCAL_SAVE_PATH, 'w', encoding='utf-8-sig') as f:
        f.write(f"REPORT_METADATA,{final_mkt_summary} /// {jp_market_summary}\n")
        df_final.to_csv(f, index=False)
    
    date_str = datetime.now().strftime('%Y%m%d')
    upload_to_drive(LOCAL_SAVE_PATH, f"minervini_final_results_{date_str}.csv")

if __name__ == "__main__":
    run_screener()
