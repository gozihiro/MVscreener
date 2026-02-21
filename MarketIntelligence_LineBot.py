import yfinance as yf
import pandas as pd
import requests
import json
import os
import random
from datetime import datetime

# --- GitHub Secretsから環境変数を取得 ---
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_USER_ID = os.environ.get('LINE_USER_ID')

def send_line_message(message):
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_USER_ID:
        print(">> ❌ トークンまたはユーザーIDが設定されていません。")
        return
    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {LINE_CHANNEL_ACCESS_TOKEN}'
    }
    data = {
        'to': LINE_USER_ID,
        'messages': [{'type': 'text', 'text': message}]
    }
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code == 200:
        print(">> ✅ LINE送信成功")
    else:
        print(f">> ❌ LINE送信失敗: {res.text}")

def get_detailed_pulse():
    score = 0
    report = []
    report.append(f"⚖️ Market Intelligence ({datetime.now().strftime('%H:%M')})")
    
    # 1. 指数位置判定 (3.0pts)
    report.append("\n【1. Index vs Open】")
    indices = {"Nasdaq": "^IXIC", "S&P500": "^GSPC"}
    for name, ticker in indices.items():
        data = yf.download(ticker, period="1d", interval="1m", progress=False, auto_adjust=True)
        if not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            open_p = float(data['Open'].iloc[0])
            curr_p = float(data['Close'].iloc[-1])
            
            diff = (curr_p / open_p - 1) * 100
            if curr_p > open_p:
                score += 1.5
                status = "🟢陽線"
            else:
                status = "🔴陰線"
            report.append(f" ・{name}: {status} ({diff:+.2f}%)")

    # 2. RVOL判定 (3.0pts) - 【時刻スライスによる精度向上版】
    report.append("\n【2. Volume Energy (RVOL)】")
    etfs = {"SPY": "SPY", "QQQ": "QQQ"}
    for name, ticker in etfs.items():
        # 過去20日分の5分足を取得
        hist = yf.download(ticker, period="20d", interval="5m", progress=False, auto_adjust=True)
        if hist.empty: continue
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

        # 現在の時刻（時:分）と今日の日付を取得
        current_time = hist.index[-1].time()
        today_date = hist.index[-1].date()

        # 過去のユニークな日付リストを作成
        unique_dates = pd.Series(hist.index.date).unique()
        
        past_vols = []
        for d in unique_dates:
            if d == today_date: continue
            
            # 各日のデータを抽出し、寄り付き(09:30)から「現在と同じ時刻」までを厳密にスライス
            daily_data = hist[hist.index.date == d]
            # between_timeを使用することで、欠損があっても指定時刻までの出来高を正確に合計可能
            vol_until_now = daily_data.between_time("09:30", current_time)['Volume'].sum()
            
            if vol_until_now > 0:
                past_vols.append(vol_until_now)

        # 期待出来高（過去平均）の算出
        expected_vol = sum(past_vols) / len(past_vols) if past_vols else 0
        actual_vol = hist[hist.index.date == today_date].Volume.sum()

        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        
        if rvol >= 1.2: 
            score += 1.5
            emoji = "🔥" 
        elif rvol >= 1.0:
            emoji = "✅"
        else:
            emoji = "💤"
        report.append(f" ・{name} RVOL: {rvol:.2f}x {emoji}")

    # 3. 需給の質判定 (4.0pts)
    report.append("\n【3. Internal Strength】")
    sample_tickers = ["AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","AVGO","COST","PEP","ADBE","AMD","NFLX","INTC","TMUS","AMAT","QCOM","TXN","ISRG","HON","SBUX","AMGN","VRTX","MDLZ","PANW","REGN","LRCX","ADI","BKNG","MU"]
    sample_data = yf.download(sample_tickers, period="1d", interval="5m", progress=False, auto_adjust=True)
    
    if not sample_data.empty:
        adv, dec, adv_v, dec_v = 0, 0, 0, 0
        for t in sample_tickers:
            try:
                if isinstance(sample_data.columns, pd.MultiIndex):
                    t_data = sample_data.xs(t, axis=1, level=1).dropna()
                else:
                    t_data = sample_data.dropna()
                
                if t_data.empty: continue
                
                c_last = t_data['Close'].iloc[-1]
                o_first = t_data['Open'].iloc[0]
                v_total = t_data['Volume'].sum()
                
                if c_last > o_first:
                    adv += 1; adv_v += v_total
                else:
                    dec += 1; dec_v += v_total
            except: continue
        
        if dec > 0 and dec_v > 0:
            trin = (adv/dec) / (adv_v/dec_v) if (adv_v/dec_v) > 0 else 0
            adv_rate = adv / len(sample_tickers)
            if trin < 0.85: score += 2.0
            if adv_rate >= 0.7: score += 2.0
            report.append(f" ・TRIN近似: {trin:.2f} ({'強気' if trin < 1 else '弱気'})")
            report.append(f" ・値上がり比: {int(adv_rate*100)}% ({adv}/{len(sample_tickers)})")

    # 総合判定
    rank = "S [点火日]" if score >= 8.5 else "A [良好]" if score >= 6.5 else "B [拮抗]" if score >= 4.0 else "C [危険]"
    summary = f"\n━━━━━━━━━━━━\n総合スコア: {score:.1f} / 10.0\n判定ランク: {rank}\n━━━━━━━━━━━━"
    
    final_msg = "\n".join(report) + summary
    send_line_message(final_msg)

if __name__ == "__main__":
    get_detailed_pulse()
