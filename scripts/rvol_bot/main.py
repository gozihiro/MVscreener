import os
import io
import json
# [修正] 起動高速化（タイムアウト対策）のため、pandas, yfinance のインポートを関数内へ移動
from datetime import datetime
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, 
    ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import functions_framework

# [追加] SAVE機能用のGoogle APIライブラリ (これらは軽量なためここでインポート)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account

# 環境変数の取得
access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')
# [追加] SAVE機能用の認証情報とフォルダID
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
DRIVE_FOLDER_ID = os.environ.get('RETROSPECTIVE_FOLDER_ID', '1IqEghqFq3eM2YyS-6K93U5Zf5_C0Z1kF')

configuration = Configuration(access_token=access_token)
handler = WebhookHandler(channel_secret)

# --- [新規] SAVEコマンド用ヘルパー関数群 (修正箇所) ---
def get_drive_service():
    """Google Drive API クライアントの初期化"""
    if not creds_json: return None
    scopes = ['https://www.googleapis.com/auth/drive.file']
    try:
        creds_dict = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return build('drive', 'v3', credentials=creds)
    except: return None

def normalize_date(date_str):
    """YYYY/M/D 等の形式を YYYY-MM-DD に正規化"""
    import pandas as pd # [遅延インポート]
    try:
        return pd.to_datetime(date_str.replace('/', '-')).strftime('%Y-%m-%d')
    except: return None

def upload_df_to_drive(df, file_name):
    """DataFrameをCSVとしてDriveにアップロード"""
    service = get_drive_service()
    if not service: return False
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)
        file_metadata = {'name': file_name, 'parents': [DRIVE_FOLDER_ID]}
        media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv', resumable=True)
        service.files().create(body=file_metadata, media_body=media).execute()
        return True
    except: return False

# --- エントリポイント (Cloud Functions / Cloud Run 用) ---
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
    reply_text = ""
    
    # 「Market」入力判定 (大文字小文字を区別しない)
    if user_text.upper().startswith("SAVE"):
        import yfinance as yf # [遅延インポート]
        parts = user_text.split()
        if len(parts) >= 4:
            ticker = parts[1].upper()
            if ticker.isdigit() and len(ticker) == 4: ticker += ".T"
            start = normalize_date(parts[2])
            end = normalize_date(parts[3])
            
            if start and end:
                df = yf.download(ticker, start=start, end=end, progress=False)
                if not df.empty:
                    file_name = f"{ticker}_history_{start}_{end}.csv"
                    if upload_df_to_drive(df, file_name):
                        reply_text = f"✅ {ticker} を保存しました。\n期間: {start} 〜 {end}\nファイル: {file_name}"
                    else:
                        reply_text = "❌ Google Driveへの保存に失敗しました。"
                else:
                    reply_text = f"⚠️ {ticker} のデータが見つかりませんでした。"
            else:
                reply_text = "❌ 日付形式が不正です (例: 2026/3/1)。"
        else:
            reply_text = "⚠️ 形式: SAVE [銘柄] [開始日] [終了日]"
    elif user_text.lower() == "market":
        reply_text = get_market_intelligence_report()
    else:
        # 銘柄名として処理
        ticker_symbol = user_text.upper()
        # 数字4桁のみの場合は日本株(.T)として扱う
        if ticker_symbol.isdigit() and len(ticker_symbol) == 4:
            ticker_symbol += ".T"
            
        reply_text = calculate_ticker_rvol_report(ticker_symbol)
    
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

# --- 1. 銘柄別RVOLレポートロジック (日米統合解析版) ---
def calculate_ticker_rvol_report(ticker):
    try:
        is_jp = ".T" in ticker
        currency_sym = "¥" if is_jp else "$"
        market_open_time = "09:00" if is_jp else "09:30"

        # MVP/危険信号判定のため、期間を2y(日足)と25d(5分足)で取得
        hist_5m = yf.download(ticker, period="25d", interval="5m", progress=False, auto_adjust=True)
        hist_1d = yf.download(ticker, period="2y", interval="1d", progress=False, auto_adjust=True)

        if hist_5m.empty or hist_1d.empty:
            return f"⚠️ {ticker}: 銘柄が見つかりません。市場が閉まっているか、ティッカーが誤っている可能性があります。"

        if isinstance(hist_5m.columns, pd.MultiIndex):
            hist_5m.columns = hist_5m.columns.get_level_values(0)
        if isinstance(hist_1d.columns, pd.MultiIndex):
            hist_1d.columns = hist_1d.columns.get_level_values(0)

        # A. RVOL算出ロジック
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
            # 市場開始時間に基づいて同時刻の出来高を比較
            v = day_slice.between_time(market_open_time, current_time)['Volume'].sum()
            if v > 0: past_vols.append(v)

        expected_vol = sum(past_vols) / len(past_vols) if past_vols else 0
        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        
        # B. MVP指標判定 (直近15日)
        recent_16 = hist_1d.tail(16)
        prev_15 = hist_1d.iloc[-31:-16]
        
        m_count = (recent_16['Close'] > recent_16['Close'].shift(1)).tail(15).sum()
        v_ratio = hist_1d['Volume'].tail(15).mean() / prev_15['Volume'].mean() if not prev_15['Volume'].mean() == 0 else 0
        p_change = (hist_1d['Close'].iloc[-1] / hist_1d['Close'].iloc[-16]) - 1

        m_ok = m_count >= 12
        v_ok = v_ratio >= 1.25
        p_ok = p_change >= 0.20
        mvp_all = m_ok and v_ok and p_ok

        mvp_details = (
            f"M: {'○' if m_ok else '×'} ({m_count}/15日上昇)\n"
            f"V: {'○' if v_ok else '×'} ({v_ratio:.2f}x 出来高)\n"
            f"P: {'○' if p_ok else '×'} ({p_change*100:+.1f}% 上昇)"
        )

        # C. テクニカル・防衛線判定
        c = hist_1d['Close']
        price_now = float(c.iloc[-1])
        sma200 = c.rolling(window=200).mean().iloc[-1]
        sma50 = c.rolling(window=50).mean().iloc[-1]
        sma20 = c.rolling(window=20).mean().iloc[-1]
        ema10 = c.ewm(span=10, adjust=False).mean().iloc[-1]
        
        ema13 = c.ewm(span=13, adjust=False).mean()
        macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        is_red = (ema13.iloc[-1] < ema13.iloc[-2] and macd.iloc[-1] < macd.iloc[-2])
        extension = (price_now / sma200 - 1) * 100 if sma200 > 0 else 0
        
        if extension < 50:
            supp_name, supp_price, phase = "50SMA", sma50, "初動〜巡航"
            advice = "トレンド初動。50SMAを割らない限り、大きなトレンド継続を期待。"
        elif extension < 80:
            supp_name, supp_price, phase = "20SMA", sma20, "加速"
            advice = "加速フェーズ。20SMAをベースに、利益を最大限伸ばしてください。"
        else:
            supp_name, supp_price, phase = "10EMA", ema10, "クライマックス（過熱）"
            advice = "過熱局面。10EMAを割った場合は即座の利益確定を強く推奨。"

        dangers = []
        if price_now < ema10: dangers.append("短期10EMA割れ")
        if price_now < sma20: dangers.append("20SMA割れ(中期トレンド変質)")
        if is_red: dangers.append("インパルス・赤(弱気転換)")
        if extension >= 80: dangers.append("歴史的乖離(クライマックス警戒)")

        # D. メッセージ構築
        if mvp_all:
            mvp_status_title = "🚨【MVP売り】" if extension >= 80 else "🚀【MVP点火】"
            mvp_result = f"{mvp_status_title}\nMVP条件をすべて満たしました。"
        elif dangers:
            mvp_result = f"⚠️【危険信号】\n・" + "\n・".join(dangers)
        else:
            mvp_result = "✅【現状維持】特筆すべき過熱や崩れなし。"

        emoji = "🔥" if rvol >= 1.5 else "✅" if rvol >= 1.0 else "💤"
        
        # 当日始値との比較
        today_open = float(today_data_5m['Open'].iloc[0])
        change_vs_open = (price_now / today_open - 1) * 100

        return (f"【高精度RVOL・MVP解析: {ticker}】\n"
                f"価格: {currency_sym}{price_now:,.2f} ({change_vs_open:+.2f}% vs Open)\n"
                f"RVOL: {rvol:.2f}x {emoji}\n"
                f"200MA乖離: {extension:.1f}%\n"
                f"----------\n"
                f"MVP詳細判定:\n{mvp_details}\n"
                f"----------\n"
                f"💡 アドバイス:\n"
                f"現在は **{phase}** の局面にあります。\n"
                f"[戦略] {advice}\n"
                f"[防衛線] **{supp_name} ({currency_sym}{supp_price:,.2f})**\n\n"
                f"判定: {mvp_result}\n\n"
                f"※過去20日同時刻平均比較")
    except Exception as e:
        return f"❌ エラー: {str(e)}"

# --- 2. 市場環境判定ロジック (日米統合分) ---
def get_market_intelligence_report():
    score = 0
    report = []
    report.append(f"⚖️ Market Intelligence ({datetime.now().strftime('%H:%M')})")
    
    try:
        # 1. 指数位置判定
        report.append("\n【1. Index vs Open】")
        report.append("→ 始値より上で推移 = 寄り付きの売りを吸収した証拠。")
        indices = {"Nasdaq": "^IXIC", "S&P500": "^GSPC", "日経225": "1321.T"}
        for name, ticker in indices.items():
            period_str = "1d" if ".T" not in ticker else "2d"
            data = yf.download(ticker, period=period_str, interval="1m", progress=False, auto_adjust=True)
            if not data.empty:
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                latest_day = data.index[-1].date()
                day_data = data[data.index.date == latest_day]
                
                open_p = float(day_data['Open'].iloc[0])
                curr_p = float(day_data['Close'].iloc[-1])
                diff = (curr_p / open_p - 1) * 100
                if curr_p > open_p:
                    score += 1.0
                    status = "🟢陽線"
                else:
                    status = "🔴陰線"
                report.append(f" ・{name}: {status} ({diff:+.2f}%)")

        # 2. RVOL判定
        report.append("\n【2. Volume Energy】")
        report.append("→ 同時刻比1.2x以上 = 機関投資家が動いているサイン。")
        etfs = {"SPY": "SPY", "QQQ": "QQQ", "1321(JP)": "1321.T"}
        for name, ticker in etfs.items():
            hist = yf.download(ticker, period="20d", interval="5m", progress=False, auto_adjust=True)
            if hist.empty: continue
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            current_time = hist.index[-1].time()
            today_date = hist.index[-1].date()
            unique_dates = pd.Series(hist.index.date).unique()
            
            past_vols = []
            start_time = "09:00" if ".T" in ticker else "09:30"
            for d in unique_dates:
                if d == today_date: continue
                daily_data = hist[hist.index.date == d]
                vol_until_now = daily_data.between_time(start_time, current_time)['Volume'].sum()
                if vol_until_now > 0: past_vols.append(vol_until_now)

            expected_vol = sum(past_vols) / len(past_vols) if past_vols else 0
            actual_vol = hist[hist.index.date == today_date].Volume.sum()
            rvol = actual_vol / expected_vol if expected_vol > 0 else 0
            
            if rvol >= 1.2: score += 1.0; emoji = "🔥" 
            elif rvol >= 1.0: emoji = "✅"
            else: emoji = "💤"
            report.append(f" ・{name} RVOL: {rvol:.2f}x {emoji}")

        # 3. 需給の質判定 (US主要銘柄で代用)
        report.append("\n【3. Internal Strength (US)】")
        report.append("→ TRIN 1.0未満 = 上昇銘柄に資金が集中。")
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

        # 総合判定 (日米統合のため満点を10点に調整)
        rank = "S [点火日]" if score >= 8.5 else "A [良好]" if score >= 6.5 else "B [拮抗]" if score >= 4.0 else "C [危険]"
        summary = f"\n━━━━━━━━━━━━\n総合スコア: {score:.1f} / 10.0\n判定ランク: {rank}\n━━━━━━━━━━━━"
        return "\n".join(report) + summary

    except Exception as e:
        return f"❌ 市場データ取得エラー: {str(e)}"
