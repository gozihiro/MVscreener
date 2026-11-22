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

        # 既存ファイルを検索
        query = f"'{folder_id}' in parents and name = '{drive_file_name}' and trashed = false"
        res = service.files().list(q=query, fields="files(id)").execute()
        files = res.get('files', [])

        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)

        if files:
            service.files().update(fileId=files[0]['id'], media_body=media).execute()
            log(f">> ✅ Driveファイルを更新: {drive_file_name}")
        else:
            file_metadata = {'name': drive_file_name, 'parents': [folder_id]}
            service.files().create(body=file_metadata, media_body=media).execute()
            log(f">> ✅ Driveファイルを新規作成: {drive_file_name}")
    except Exception as e:
        log(f">> ❌ Driveアップロード失敗: {e}")


def is_spac_or_buyout_peg(df, sp_returns=None):
    """
    SPAC横ばい張り付き・全額現金買収ペッグ銘柄を除外する3段階複合フィルタ。
    ※有機的なVCP（2〜5日の短期収縮）を誤除外しないよう、長期（8〜10日以上）の極小レンジ固定のみを対象とする。
    """
    if len(df) < 25:
        return False

    c = df['Close']
    h = df['High']
    l = df['Low']
    v = df['Volume']
    curr_p = float(c.iloc[-1])

    # 1. SPAC横ばい張り付き判定（$9.50〜$11.80帯で20日高安レンジ3.5%以内、または10日レンジ1.5%以内）
    if 9.50 <= curr_p <= 11.80:
        band_20 = (float(h.tail(20).max()) - float(l.tail(20).min())) / max(float(l.tail(20).min()), 1e-6)
        band_10 = (float(h.tail(10).max()) - float(l.tail(10).min())) / max(float(l.tail(10).min()), 1e-6)
        if band_20 <= 0.035 or band_10 <= 0.015:
            return True

    # 2. 買収ペッグ型①: 10営業日連続の極小レンジ固定（10日中8日以上が日足レンジ<=0.65% & 終値変化<=0.35% かつ10日総レンジ<=1.2%）
    h10, l10, c10 = h.tail(10), l.tail(10), c.tail(10)
    band_10 = (float(h10.max()) - float(l10.min())) / max(float(l10.min()), 1e-6)
    if band_10 <= 0.012:
        daily_rng = (h10 - l10) / l10.replace(0, np.nan)
        daily_ret = c.pct_change().tail(10).abs()
        pinned_days = int(((daily_rng <= 0.0065) & (daily_ret <= 0.0035)).sum())
        if pinned_days >= 8:
            return True

    # 3. 買収ペッグ型②: 急騰ギャップ（+12%以上 & 50日平均出来高3倍以上）直後に5営業日以上1.2%以内の直線張り付き
    vol_sma50 = v.rolling(50).mean()
    ret_all = c.pct_change()
    lookback_start = max(1, len(c) - 20)
    for k in range(lookback_start, len(c) - 5):
        v_ma = vol_sma50.iloc[k]
        if pd.notna(v_ma) and v_ma > 0:
            if ret_all.iloc[k] >= 0.12 and v.iloc[k] >= v_ma * 3.0:
                post_h = float(h.iloc[k + 1:].max())
                post_l = float(l.iloc[k + 1:].min())
                post_band = (post_h - post_l) / max(post_l, 1e-6)
                if post_band <= 0.012:
                    return True

    # 4. 買収ペッグ型③: 市場相関消失・ベータゼロ判定（10日レンジ<=1.5% かつ 対S&P500の10日相関|r|<0.10 & |beta|<0.05）
    if sp_returns is not None and band_10 <= 0.015:
        stock_ret = c.pct_change().tail(10).dropna()
        aligned = pd.concat([stock_ret.rename('stk'), sp_returns.rename('sp')], axis=1).dropna()
        if len(aligned) >= 8 and aligned['sp'].var() > 1e-10:
            corr = aligned['stk'].corr(aligned['sp'])
            beta = aligned['stk'].cov(aligned['sp']) / aligned['sp'].var()
            if pd.notna(corr) and abs(corr) < 0.10 and abs(beta) < 0.05:
                return True

    return False


def calculate_launchpad_score(df, ticker, tags, index_change):
    """
    ミネルヴィニ/エルダー流『発射台スコア』算出 (0-10点)
    """
    if len(df) < 20:
        return 0

    c_today = df['Close'].iloc[-1]
    o_today = df['Open'].iloc[-1]
    h_today = df['High'].iloc[-1]
    l_today = df['Low'].iloc[-1]
    v_today = df['Volume'].iloc[-1]

    day_range = h_today - l_today
    if day_range == 0:
        return 0

    # 【重要追加条件】陽線判定：終値が始値以下（陰線または同値）なら、即座に0点として除外
    if c_today <= o_today:
        return 0

    # --- A. 共通基礎点 (最大6点) ---
    base_score = 0
    closing_range = (c_today - l_today) / day_range
    if closing_range >= 0.8:
        base_score += 2

    avg_range_20 = (df['High'] - df['Low']).rolling(20).mean().iloc[-1]
    if day_range < (avg_range_20 * 0.8):
        base_score += 2

    upper_shadow = h_today - max(o_today, c_today)
    if upper_shadow < (day_range * 0.1):
        base_score += 2

    # --- B. パターン別ボーナス (最大4点) ---
    bonus_vcp = 0
    if "VCP_3Steps_Validated" in tags or "VCP_Original" in tags:
        vol_sma50 = df['Volume'].rolling(50).mean().iloc[-1]
        if v_today < (vol_sma50 * 0.5):
            bonus_vcp += 2
        last_3_closes = df['Close'].tail(3)
        if last_3_closes.std() < (c_today * 0.005):
            bonus_vcp += 2

    bonus_hb = 0
    if any("High-Base" in t for t in tags):
        sma20 = df['Close'].rolling(20).mean().iloc[-1]
        if l_today <= (sma20 * 1.015):
            bonus_hb += 2
        lower_shadow = min(o_today, c_today) - l_today
        body_size = abs(c_today - o_today)
        if lower_shadow > body_size:
            bonus_hb += 2

    bonus_pp = 0
    if "PowerPlay(70%+)" in tags:
        high_5d = df['High'].tail(5).max()
        if c_today >= (high_5d * 0.98):
            bonus_pp += 2
        stock_change = (c_today / df['Close'].iloc[-2]) - 1
        if stock_change >= 0 and index_change < 0:
            bonus_pp += 2

    final_score = base_score + max(bonus_vcp, bonus_hb, bonus_pp)
    return min(10, final_score)


def analyze_index_ftd_df(df, ticker="^GSPC", dd_threshold=-0.002, ftd_gain=0.0125):
    """
    単一指数のOHLCV DataFrameから、オニール流の売り抜け日・ラリー試行(Day 1〜)・
    FTD(Day 4〜10, DCR>=0.50, 出来高前日比増)・FTD失敗(Invalidation)を正確に判定する。
    """
    if df is None or df.empty or len(df) < 55:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)

    c = df['Close'].squeeze().dropna()
    h = df['High'].squeeze().reindex(c.index)
    l = df['Low'].squeeze().reindex(c.index)
    v = df['Volume'].squeeze().reindex(c.index)

    changes = c.pct_change()
    sma50 = c.rolling(50).mean()
    v_sma50 = v.rolling(50).mean()
    n = len(c)

    # 1. 売り抜け日 (Distribution Days: 直近25営業日、-0.2%以下 & 出来高前日比増、+5%上昇で失効)
    dd_indices = []
    for idx in range(max(1, n - 25), n):
        if changes.iloc[idx] <= dd_threshold and v.iloc[idx] > v.iloc[idx - 1]:
            if idx + 1 < n and (c.iloc[idx + 1:] >= c.iloc[idx] * 1.05).any():
                continue
            dd_indices.append(idx)
    dist_days = len(dd_indices)

    # 2. ラリー試行 (Day 1) と FTD (Day 4〜10) の判定（直近30営業日の日中安値ベース）
    window = min(30, n - 1)
    start_idx = max(1, n - window)
    low_pos_rel = int(np.argmin(l.iloc[start_idx:].values))
    low_idx = start_idx + low_pos_rel
    rally_low = float(l.iloc[low_idx])

    # Day 1 判定: 安値当日の終値が日中値幅の上位50%以上（反転引け）なら当日=Day 1、
    # そうでなければ rally_low を割らずに迎えた最初のプラス引け日を Day 1 とする
    rng_low = float(h.iloc[low_idx] - l.iloc[low_idx])
    dcr_low = (float(c.iloc[low_idx]) - rally_low) / rng_low if rng_low > 0 else 0.0

    day1_idx = None
    if dcr_low >= 0.50:
        day1_idx = low_idx
    else:
        for idx in range(low_idx + 1, n):
            if float(l.iloc[idx]) < rally_low:
                day1_idx = None
                break
            if changes.iloc[idx] > 0:
                day1_idx = idx
                break

    rally_day = 0
    ftd_idx = None
    ftd_failed = False

    if day1_idx is not None:
        # Day 1 以降に rally_low を日中安値で割っていないか検証（割ったら即リセット）
        if day1_idx + 1 < n and (l.iloc[day1_idx + 1:] < rally_low).any():
            day1_idx = None
        else:
            rally_day = (n - 1) - day1_idx + 1  # Day 1 = 1日目
            # Day 4 〜 Day 10 の範囲で FTD を正順走査（Day 1〜3の自律反発を確実に除外）
            ftd_start = day1_idx + 3   # Day 4
            ftd_end = min(n, day1_idx + 10)  # Day 10 まで
            for idx in range(ftd_start, ftd_end):
                rng = float(h.iloc[idx] - l.iloc[idx])
                dcr = (float(c.iloc[idx]) - float(l.iloc[idx])) / rng if rng > 0 else 0.0
                v_ma = v_sma50.iloc[idx]
                vol_ok = (v.iloc[idx] > v.iloc[idx - 1]) and (
                    pd.isna(v_ma) or v_ma <= 0 or v.iloc[idx] >= v_ma * 0.80
                )
                if changes.iloc[idx] >= ftd_gain and vol_ok and dcr >= 0.50:
                    ftd_idx = idx
                    break

            # FTD失敗 (Invalidation) 判定: 点灯後5営業日以内に FTD安値終値割れ or DD 2回以上
            if ftd_idx is not None:
                ftd_low = float(l.iloc[ftd_idx])
                after_ftd_end = min(n, ftd_idx + 6)
                undercut = (
                    (ftd_idx + 1 < after_ftd_end)
                    and (c.iloc[ftd_idx + 1:after_ftd_end] < ftd_low).any()
                )
                dd_after = sum(1 for d in dd_indices if ftd_idx < d < after_ftd_end)
                if undercut or dd_after >= 2:
                    ftd_failed = True

    curr_price = float(c.iloc[-1])
    curr_sma50 = float(sma50.iloc[-1])
    last_date = c.index[-1].strftime('%Y/%m/%d')

    return {
        "ticker": ticker,
        "last_date": last_date,
        "last_change": float(changes.iloc[-1]),
        "returns": changes,
        "dist_days": dist_days,
        "rally_day": int(rally_day),
        "ftd_active": (ftd_idx is not None) and (not ftd_failed),
        "ftd_failed": ftd_failed,
        "ftd_date": c.index[ftd_idx].strftime('%m/%d') if ftd_idx is not None else None,
        "above_sma50": curr_price > curr_sma50,
    }


def get_market_health_summary():
    """S&P500 & Nasdaq デュアル判定による米国市場環境解析"""
    log(">> ステップ1: 指数データ解析開始（S&P500 & Nasdaq デュアルFTD判定）...")
    try:
        sp_df = yf.download("^GSPC", period="120d", progress=False, auto_adjust=True)
        nq_df = yf.download("^IXIC", period="120d", progress=False, auto_adjust=True)

        sp = analyze_index_ftd_df(sp_df, "^GSPC")
        nq = analyze_index_ftd_df(nq_df, "^IXIC")
        if sp is None:
            return "判定不能", 0, 0, 0.0, datetime.now().strftime('%Y/%m/%d'), None

        ftd_sources = []
        if sp["ftd_active"]:
            ftd_sources.append(f"SP500:{sp['ftd_date']}")
        if nq and nq["ftd_active"]:
            ftd_sources.append(f"NDQ:{nq['ftd_date']}")

        dist_days = sp["dist_days"]
        rally_day = max(sp["rally_day"], nq["rally_day"] if nq else 0)
        any_failed = sp["ftd_failed"] or (nq["ftd_failed"] if nq else False)

        # 50MA下でもFTD点灯を明確に識別し、Downtrendへの誤逆戻りを防止
        if ftd_sources and dist_days < 6:
            tag = "/".join(ftd_sources)
            if sp["above_sma50"]:
                status = f"🚀 上昇確定 (Confirmed Uptrend | FTD:{tag})"
            else:
                status = f"🟢 FTD点灯・初動 (50MA下 | FTD:{tag})"
        elif any_failed and not ftd_sources:
            status = "⚠️ FTD失敗・警戒 (Failed FTD)"
        elif dist_days >= 6:
            status = "🔴 下落警戒 (Market Under Pressure)"
        elif 1 <= rally_day <= 10:
            status = "🟡 ラリー試行中 (Rally Attempt)"
        elif rally_day > 10:
            status = "🟠 ラリー長期化・失速警戒 (Extended Rally)" if not sp["above_sma50"] else "🔄 調整中 (Correcting)"
        else:
            status = "📉 下落トレンド (Downtrend)" if not sp["above_sma50"] else "🔄 調整中 (Correcting)"

        return status, dist_days, int(rally_day), sp["last_change"], sp["last_date"], sp["returns"]
    except Exception as e:
        log(f"❌ 市場判定エラー: {e}")
        return "エラー停止", 0, 0, 0.0, datetime.now().strftime('%Y/%m/%d'), None


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
    """日本市場の環境解析（1321.Tによる指数判定。period='1y'で200MAを正確に算出）"""
    log(">> ステップ1.2: 日本市場の解析開始...")
    try:
        idx = yf.download("1321.T", period="1y", progress=False, auto_adjust=True)
        if idx.empty or len(idx) < 60:
            return f"JP_METADATA,データ不足 | A/D比:{ad_ratio} (↑{adv} ↓{dec}) | 売り抜け:0日"
        if isinstance(idx.columns, pd.MultiIndex):
            idx.columns = idx.columns.get_level_values(0)
        c = idx['Close'].squeeze().dropna()
        v = idx['Volume'].squeeze().reindex(c.index)
        changes = c.pct_change()
        n = len(c)

        # 売り抜け日（直近25営業日、-0.4%以下 & 出来高前日比増、+5%上昇で失効）
        dist_days = 0
        for cur in range(max(1, n - 25), n):
            if changes.iloc[cur] <= -0.004 and v.iloc[cur] > v.iloc[cur - 1]:
                if cur + 1 < n and (c.iloc[cur + 1:] >= c.iloc[cur] * 1.05).any():
                    continue
                dist_days += 1

        ma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else float(c.mean())
        curr_p = float(c.iloc[-1])

        if curr_p < ma200 or dist_days >= 6:
            status = "🔴 下落警戒 (Market Under Pressure)"
        elif dist_days >= 4 or ad_ratio < 0.8:
            status = "🔶 上昇負担 (Uptrend Under Pressure)"
        else:
            status = "🚀 上昇確認 (Confirmed Uptrend)"

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
        log(f"【エラー】リスト取得失敗: {e}")
        return []


def run_screener():
    log("=== スクリーナー V3 起動（デュアルFTD・SPAC/買収ペッグ除外・発射台スコア搭載） ===")
    mkt_status, dist_count, low_days, index_change, us_market_date, sp_returns = get_market_health_summary()
    log(f">> 米国市場日: {us_market_date} | {mkt_status} (売り抜け:{dist_count}日 / 安値から:{low_days}日目)")

    # 1. ユニバース統合
    universe = get_full_universe()
    jp_universe = get_jp_full_universe()
    if jp_universe:
        universe += jp_universe
        log(f">> 日本市場 {len(jp_universe)} 銘柄をユニバースに追加しました。")

    if not universe:
        return

    results = []
    advances, declines = 0, 0
    jp_advances, jp_declines = 0, 0
    excluded_pegs = 0
    total = len(universe)

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i + BATCH_SIZE]
        try:
            log(f"    [進捗] {i}/{total} 分析中...")
            data = yf.download(batch, period="2y", interval="1d", progress=False, auto_adjust=True, threads=True)
            if data.empty:
                time.sleep(120)
                continue

            for ticker in batch:
                try:
                    if ticker not in data['Close'].columns:
                        continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    if len(df) < 252:
                        continue

                    # A/Dカウンター
                    is_up = df['Close'].iloc[-1] > df['Close'].iloc[-2]
                    if ticker.endswith('.T'):
                        if is_up:
                            jp_advances += 1
                        else:
                            jp_declines += 1
                        continue
                    else:
                        if is_up:
                            advances += 1
                        else:
                            declines += 1

                    # SPAC・買収ペッグ銘柄の除外（A/Dカウント後に実施）
                    if is_spac_or_buyout_peg(df, sp_returns):
                        excluded_pegs += 1
                        continue

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
                        d1 = (h.iloc[-lookback:-lookback + step].max() - l.iloc[-lookback:-lookback + step].min()) / h.iloc[-lookback:-lookback + step].max()
                        d2 = (h.iloc[-lookback + step:-step].max() - l.iloc[-lookback + step:-step].min()) / h.iloc[-lookback + step:-step].max()
                        d3 = (h.iloc[-step:].max() - l.iloc[-step:].min()) / h.iloc[-step:].max()
                        return (d1 > d3) and (d2 > d3) and (d3 < 0.10)

                    vcp_strict_ok = any([check_vcp_strict(lb) for lb in [60, 90, 120]])

                    # A. VCP_Original
                    if (curr_p > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1]) and \
                       (sma200.iloc[-1] > sma200.iloc[-20]) and \
                       (c.rolling(20).std() * 4 / sma20).iloc[-1] <= (c.rolling(20).std() * 4 / sma20).iloc[-20:].min() * 1.05:
                        tags.append("VCP_Original")

                    # B. PowerPlay & High-Base
                    if curr_p > sma20.iloc[-1] > sma50.iloc[-1]:
                        if (curr_p / c.iloc[-40] >= 1.70) and (curr_p / h.iloc[-40:].max() >= 0.75):
                            tags.append("PowerPlay(70%+)")

                        tight_duration = 0
                        for d_idx in range(1, 41):
                            if len(c) < d_idx:
                                break
                            win = c.tail(d_idx)
                            if (win.max() / win.min() <= 1.10):
                                tight_duration = d_idx
                            else:
                                break

                        if (1.10 <= curr_p / c.iloc[-20] <= 1.70) and (15 <= tight_duration <= 30):
                            if (c.iloc[-5:].pct_change() >= 0.10).any() and (v.iloc[-3:] < vol_sma50.iloc[-3:]).all():
                                tags.append(f"High-Base(Strict:{tight_duration}d)")
                            else:
                                tags.append(f"High-Base({tight_duration}d)")

                    # --- C. Micro-VCP (3-Day Silence) ---
                    p1, p2, p3 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
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
                    squeeze_history = []
                    for d_back in range(1, 11):
                        if len(c) < d_back:
                            break
                        m_val = [ema10.iloc[-d_back], sma20.iloc[-d_back], sma50.iloc[-d_back]]
                        squeeze_history.append(max(m_val) / min(m_val))

                    was_squeezed = any(val <= 1.015 for val in squeeze_history)

                    up_mas = []
                    if ema10.iloc[-1] > ema10.iloc[-2]:
                        up_mas.append("10E")
                    if sma20.iloc[-1] > sma20.iloc[-2]:
                        up_mas.append("20S")
                    if sma50.iloc[-1] > sma50.iloc[-2]:
                        up_mas.append("50S")

                    if was_squeezed and curr_p > max([ema10.iloc[-1], sma20.iloc[-1], sma50.iloc[-1]]) and up_mas:
                        up_label = "/".join(up_mas)
                        tags.append(f"MA_Squeeze(REED:{up_label}↑)")

                    if tags:
                        if template_ok:
                            tags.append("[Trend_OK]")
                        if vcp_strict_ok:
                            tags.append("VCP_3Steps_Validated")

                        lp_score = calculate_launchpad_score(df, ticker, tags, index_change)

                        stock = yf.Ticker(ticker)
                        info = stock.info
                        mkt_cap = info.get('marketCap', 0)
                        if 0 < mkt_cap <= 100 * 1e9:
                            rev_g, eps_g = info.get('revenueGrowth'), info.get('earningsGrowth')
                            op_cf = info.get('operatingCashflow')
                            ema10_val = ema10.iloc[-1]

                            f_label = (
                                "【超優秀】クリア" if (rev_g or 0) >= 0.25 and (eps_g or 0) >= 0.25
                                else "【良好】一部" if (rev_g or 0) >= 0.25 or (eps_g or 0) >= 0.25
                                else "【不足】低成長"
                            )

                            results.append({
                                "銘柄": ticker, "価格": round(curr_p, 2), "パターン": ", ".join(tags),
                                "成長性判定": f_label, "売上成長(%)": round(rev_g * 100, 1) if rev_g else "不明",
                                "営業利益成長(EBITDA)%": "不明",
                                "純利益成長(%)": round(eps_g * 100, 1) if eps_g else "不明",
                                "営業CF(M)": round(op_cf / 1e6, 2) if op_cf else "不明",
                                "時価総額(B)": round(mkt_cap / 1e9, 2), "発射台スコア": lp_score,
                                "10EMA": round(ema10_val, 2),
                                "20SMA": round(sma20.iloc[-1], 2),
                                "50SMA": round(sma50.iloc[-1], 2)
                            })
                            log(f"      > 【的中】: {ticker} (Score: {lp_score})")
                except Exception:
                    continue
        except Exception as e:
            log(f"    [エラー] バッチ {i}: {e}")
        time.sleep(BATCH_SLEEP_BASE + random.uniform(0, 10))

    ad_ratio = round(advances / max(1, declines), 2)
    jp_ad_ratio = round(jp_advances / max(1, jp_declines), 2)
    log(f">> SPAC/買収ペッグ除外数: {excluded_pegs} 銘柄")

    # 実市場日付（市場日:YYYY/MM/DD）をメタデータへ明示的に埋め込み、1営業日ラグを構造的に解消
    final_mkt_summary = (
        f"{mkt_status} (売り抜け:{dist_count}日 / 安値から:{low_days}日目) "
        f"| 市場日:{us_market_date} | A/D比:{ad_ratio} (↑{advances} ↓{declines})"
    )
    jp_market_summary = get_jp_market_summary(adv=jp_advances, dec=jp_declines, ad_ratio=jp_ad_ratio)

    df_final = pd.DataFrame(results if results else [{"結果": "的中なし"}])
    with open(LOCAL_SAVE_PATH, 'w', encoding='utf-8-sig') as f:
        f.write(f"REPORT_METADATA,{final_mkt_summary} /// {jp_market_summary}\n")
        df_final.to_csv(f, index=False)

    # ファイル名も米国実市場日（YYYYMMDD）に統一し、JST 07:00/15:30の再実行でも日付ズレを防止
    date_str = us_market_date.replace('/', '')
    upload_to_drive(LOCAL_SAVE_PATH, f"minervini_final_results_{date_str}.csv")


if __name__ == "__main__":
    run_screener()
