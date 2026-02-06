import yfinance as yf
import pandas as pd
import requests
import json
import os
from datetime import datetime

# --- GitHub Secretsから環境変数を取得（コードには直接書きません） ---
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_USER_ID = os.environ.get('LINE_USER_ID')

def send_line_message(message):
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
    return res.status_code

def get_detailed_pulse():
    score = 0
    report = []
    report.append(f"⚖️ Market Intelligence ({datetime.now().strftime('%H:%M')})")
    
    # 1. 指数位置判定 (3.0pts)
    report.append("\n【1. Index vs Open】")
    report.append("→ 始値より上で推移 = 寄り付きの売りを吸収した証拠。")
    indices = {"Nasdaq": "^IXIC", "S&P500": "^GSPC"}
    for name, ticker in indices.items():
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if not data.empty:
            open_p, curr_p = data['Open'].iloc[0], data['Close'].iloc[-1]
            if curr_p > open_p:
                score += 1.5
                status = "🟢上"
            else:
                status = "🔴下"
            diff = (curr_p / open_p - 1) * 100
            report.append(f" ・{name}: {status} ({diff:+.2f}%)")

    # 2. RVOL判定 (3.0pts)
    report.append("\n【2. Volume Energy】")
    report.append("→ 同時刻比1.2x以上 = 機関投資家が『本気』で動いているサイン。")
    etfs = {"SPY": "SPY", "QQQ": "QQQ"}
    for name, ticker in etfs.items():
        hist = yf.download(ticker, period="10d", interval="5m", progress=False)
        if hist.empty: continue
        today = hist[hist.index.date == hist.index.date[-1]]
        past = hist[hist.index.date < hist.index.date[-1]]
        elapsed_bars = len(today)
        expected_vol = past.groupby(past.index.date)['Volume'].apply(lambda x: x.iloc[:elapsed_bars].sum()).mean()
        actual_vol = today['Volume'].sum()
        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        if rvol >= 1.2: score += 1.5
        report.append(f" ・{name} RVOL: {rvol:.2f}x {'🔥' if rvol > 1.2 else '⚪︎'}")

    # 3. 需給の質判定 (4.0pts)
    report.append("\n【3. Internal Strength】")
    report.append("→ TRIN 1.0未満 = 上昇銘柄に資金が集中する質の高い相場。")
    sample_tickers = ["AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","AVGO","COST","PEP","ADBE","AMD","NFLX","INTC","TMUS","AMAT","QCOM","TXN","ISRG","HON","SBUX","AMGN","VRTX","MDLZ","PANW","REGN","LRCX","ADI","BKNG","MU"]
    sample_data = yf.download(sample_tickers, period="1d", interval="1m", progress=False)
    
    if not sample_data.empty:
        adv, dec, adv_v, dec_v = 0, 0, 0, 0
        for t in sample_tickers:
            try:
                c, o, v = sample_data['Close'][t].dropna(), sample_data['Open'][t].dropna(), sample_data['Volume'][t].dropna()
                if not c.empty:
                    if c.iloc[-1] > o.iloc[0]:
                        adv += 1; adv_v += v.sum()
                    else:
                        dec += 1; dec_v += v.sum()
            except: continue
        
        if dec > 0 and dec_v > 0:
            trin = (adv/dec) / (adv_v/dec_v) if (adv_v/dec_v) > 0 else 0
            adv_rate = adv / len(sample_tickers)
            if trin < 1.0: score += 2.0
            if adv_rate >= 0.6: score += 2.0
            report.append(f" ・TRIN近似: {trin:.2f}")
            report.append(f" ・値上がり比: {int(adv_rate*100)}%")

    # 総合判定ランク
    rank = "S [点火日]" if score >= 8.5 else "A [良好]" if score >= 6.5 else "B [拮抗]" if score >= 4.0 else "C [危険]"
    summary = f"\n━━━━━━━━━━━━\n総合スコア: {score:.1f} / 10.0\n判定ランク: {rank}\n━━━━━━━━━━━━"
    
    final_msg = "\n".join(report) + summary
    print(final_msg)
    send_line_message(final_msg)

if __name__ == "__main__":
    get_detailed_pulse()
