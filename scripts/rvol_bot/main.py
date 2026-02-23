import os
import yfinance as yf
import pandas as pd
import json
import random
from datetime import datetime
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, 
    ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import functions_framework

# 環境変数から取得
access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')

configuration = Configuration(access_token=access_token)
handler = WebhookHandler(channel_secret)

@functions_framework.http
def callback(request):
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        return 'Invalid signature', 400

    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_text = event.message.text.strip()
    
    # 「Market」入力判定 (大文字小文字を区別しない)
    if user_text.lower() == "market":
        reply_text = get_market_intelligence_report()
    else:
        # 銘柄名として処理
        ticker_symbol = user_text.upper()
        reply_text = calculate_ticker_rvol_report(ticker_symbol)
    
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

# --- 1. 銘柄別RVOLレポートロジック (統合解析版) ---
def calculate_ticker_rvol_report(ticker):
    try:
        # MVP/危険信号判定のため、期間を2y(日足)と25d(5分足)で取得
        hist_5m = yf.download(ticker, period="25d", interval="5m", progress=False, auto_adjust=True)
        hist_1d = yf.download(ticker, period="2y", interval="1d", progress=False, auto_adjust=True)

        if hist_5m.empty or hist_1d.empty:
            return f"⚠️ ${ticker}: 銘柄が見つかりません。"

        if isinstance(hist_5m.columns, pd.MultiIndex):
            hist_5m.columns = hist_5m.columns.get_level_values(0)
        if isinstance(hist_1d.columns, pd.MultiIndex):
            hist_1d.columns = hist_1d.columns.get_level_values(0)

        # A. RVOL算出ロジック (既存継承)
        latest_dt = hist_5m.index[-1]
        today_date = latest_dt.date()
        current_time = latest_dt.time()
        
        today_data_5m = hist_5m[hist_5m.index.date == today_date]
        actual_vol = today_data_5m['Volume'].sum()
        
        past_data_5m = hist_5m[hist_5m.index.date < today_date]
        unique_dates = pd.Series(past_data_5m.index.date).unique()[-20:]
        
        past_vols = []
        for d in unique_dates:
            day_slice = past_data_5m[past_data_5m.index.date == d]
            v = day_slice.between_time("09:30", current_time)['Volume'].sum()
            if v > 0: past_vols.append(v)

        expected_vol = sum(past_vols) / len(past_vols) if past_vols else 0
        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        
        # B. MVP指標判定 (直近15日)
        recent_15 = hist_1d.tail(15)
        prev_15 = hist_1d.iloc[-30:-15] # 比較用の直前15日間
        
        m_count = (recent_15['Close'] > recent_15['Close'].shift(1)).sum()
        v_ratio = recent_15['Volume'].mean() / prev_15['Volume'].mean() if not prev_15['Volume'].mean() == 0 else 0
        p_change = (recent_15['Close'].iloc[-1] / recent_15['Close'].iloc[0]) - 1

        # 各項目の合否判定
        m_ok = m_count >= 12
        v_ok = v_ratio >= 1.25
        p_ok = p_change >= 0.20
        mvp_all = m_ok and v_ok and p_ok

        # メッセージ表示用の詳細文字列
        mvp_details = (
            f"M: {'○' if m_ok else '×'} ({m_count}/15日上昇)\n"
            f"V: {'○' if v_ok else '×'} ({v_ratio:.2f}x 出来高)\n"
            f"P: {'○' if p_ok else '×'} ({p_change*100:+.1f}% 上昇)"
        )

        # C. テクニカル・危険信号判定
        c = hist_1d['Close']
        price_now = c.iloc[-1]
        ema10 = c.ewm(span=10, adjust=False).mean().iloc[-1]
        sma20 = c.rolling(window=20).mean().iloc[-1]
        sma200 = c.rolling(window=200).mean().iloc[-1]
        
        ema13 = c.ewm(span=13, adjust=False).mean()
        macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        is_red = (ema13.iloc[-1] < ema13.iloc[-2] and macd.iloc[-1] < macd.iloc[-2])
        extension = (price_now / sma200 - 1) * 100 if sma200 > 0 else 0
        
        dangers = []
        if price_now < ema10: dangers.append("短期10EMA割れ")
        if price_now < sma20: dangers.append("20SMA割れ(Stage脱落警戒)")
        if is_red: dangers.append("インパルス・赤(弱気転換)")
        if extension >= 50: dangers.append("200MA乖離過大(過熱)")

        # D. メッセージ構築
        if mvp_all:
            if extension >= 50:
                mvp_status = "🚨【MVP売り】クライマックス・トップ。"
            else:
                mvp_status = "🚀【MVP点火】強力なブレイクアウト初動。"
        elif dangers:
            mvp_status = "⚠️【危険信号】\n・" + "\n・".join(dangers)
        else:
            mvp_status = "✅【現状維持】特筆すべき過熱や崩れなし。"

        emoji = "🔥" if rvol >= 1.5 else "✅" if rvol >= 1.0 else "💤"
        change = (price_now / float(today_data_5m['Open'].iloc[0]) - 1) * 100

        return (f"【高精度RVOL・MVP解析: ${ticker}】\n"
                f"価格: ${price_now:.2f} ({change:+.2f}% vs Open)\n"
                f"RVOL: {rvol:.2f}x {emoji}\n"
                f"200MA乖離: {extension:.1f}%\n"
                f"----------\n"
                f"MVP詳細判定:\n{mvp_details}\n"
                f"----------\n"
                f"{mvp_status}\n\n"
                f"※過去20日同時刻平均比較")
    except Exception as e:
        return f"❌ エラー: {str(e)}"

# --- 2. 市場環境判定ロジック (統合分) ---
def get_market_intelligence_report():
    score = 0
    report = []
    report.append(f"⚖️ Market Intelligence ({datetime.now().strftime('%H:%M')})")
    
    try:
        # 1. 指数位置判定
        report.append("\n【1. Index vs Open】")
        report.append("→ 始値より上で推移 = 寄り付きの売りを吸収した証拠。")
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

        # 2. RVOL判定
        report.append("\n【2. Volume Energy】")
        report.append("→ 同時刻比1.2x以上 = 機関投資家が『本気』で動いているサイン。")
        etfs = {"SPY": "SPY", "QQQ": "QQQ"}
        for name, ticker in etfs.items():
            hist = yf.download(ticker, period="20d", interval="5m", progress=False, auto_adjust=True)
            if hist.empty: continue
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            current_time = hist.index[-1].time()
            today_date = hist.index[-1].date()
            unique_dates = pd.Series(hist.index.date).unique()
            
            past_vols = []
            for d in unique_dates:
                if d == today_date: continue
                daily_data = hist[hist.index.date == d]
                vol_until_now = daily_data.between_time("09:30", current_time)['Volume'].sum()
                if vol_until_now > 0:
                    past_vols.append(vol_until_now)

            expected_vol = sum(past_vols) / len(past_vols) if past_vols else 0
            actual_vol = hist[hist.index.date == today_date].Volume.sum()
            rvol = actual_vol / expected_vol if expected_vol > 0 else 0
            
            if rvol >= 1.2: score += 1.5; emoji = "🔥" 
            elif rvol >= 1.0: emoji = "✅"
            else: emoji = "💤"
            report.append(f" ・{name} RVOL: {rvol:.2f}x {emoji}")

        # 3. 需給の質判定
        report.append("\n【3. Internal Strength】")
        report.append("→ TRIN 1.0未満 = 上昇銘柄に資金が集中する質の高い相場。")
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
        return "\n".join(report) + summary

    except Exception as e:
        return f"❌ 市場データ取得エラー: {str(e)}"
