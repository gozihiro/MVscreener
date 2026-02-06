import yf
import pandas as pd
import requests
import json
import os
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
    report.append("→ 始値より上で推移 = 寄り付きの売りを吸収した証拠。")
    indices = {"Nasdaq": "^IXIC", "S&P500": "^GSPC"}
    for name, ticker in indices.items():
        data = yf.download(ticker, period="1d", interval="1m", progress=False, auto_adjust=True)
        if not data.empty:
            # マルチインデックス対策: 列を平坦化し、確実にスカラー値（数値）を取得
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            open_p = float(data['Open'].iloc[0])
            curr_p = float(data['Close'].iloc[-1])
            
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
        hist = yf.download(ticker, period="10d", interval="5m", progress=False, auto_adjust=True)
        if hist.empty: continue
        
        # マルチインデックス対策
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
            
        today = hist[hist.index.date == hist.index.date[-1]]
        past = hist[hist.index.date < hist.index.date[-1]]
        elapsed_bars = len(today)
        expected_vol = past.groupby(past.index.date)['Volume'].apply(lambda x: x.iloc[:elapsed_bars].sum()).mean()
        actual_vol = float(today['Volume'].sum())
        
        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        if rvol >= 1.2: score += 1.5
        report.append(f" ・{name} RVOL: {rvol:.2f}x {'🔥' if rvol > 1.2 else '⚪︎'}")

    # 3. 需給の質判定 (4.0pts)
    report.append("\n【3. Internal Strength】")
    report.append("→ TRIN 1.0未満 = 上昇銘柄に資金が集中する質の高い相場。")
    sample_tickers = ["AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","AVGO","COST","PEP","ADBE","AMD","NFLX","INTC","TMUS","AMAT","QCOM","TXN","ISRG","HON","SBUX","AMGN","VRTX","MDLZ","PANW","REGN","LRCX","ADI","BKNG","MU"]
    sample_data = yf.download(sample_tickers, period="1d", interval="1m", progress=False, auto_adjust=True)
    
    if not sample_data.empty:
        adv, dec, adv_v, dec_v = 0, 0, 0, 0
        for t in sample_tickers:
            try:
                # 特定銘柄のデータをクロスセクションで抽出
                ticker_data = sample_data.xs(t, axis=1, level=1) if isinstance(sample_data.columns, pd.MultiIndex) else sample_data
                if ticker_data.empty: continue
                
                c = ticker_data['Close'].dropna()
                o = ticker_data['Open'].dropna()
                v = ticker_data['Volume'].dropna()
                
                if not c.empty and not o.empty:
                    if float(c.iloc[-1]) > float(o.iloc[0]):
                        adv += 1; adv_v += float(v.sum())
                    else:
                        dec += 1; dec_v += float(v.sum())
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
    send_line_message(final_msg)

if __name__ == "__main__":
    get_detailed_pulse()
