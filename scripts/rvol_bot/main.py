import os
import yfinance as yf
import pandas as pd
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
    ticker_symbol = event.message.text.upper().strip()
    
    # RVOL算出ロジック (高精度版)
    reply_text = calculate_rvol_report(ticker_symbol)
    
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

def calculate_rvol_report(ticker):
    try:
        # 過去25日分の5分足を取得
        hist = yf.download(ticker, period="25d", interval="5m", progress=False, auto_adjust=True)
        if hist.empty:
            return f"⚠️ ${ticker}: 銘柄が見つかりません。"

        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

        # 直近データと時刻の特定
        latest_dt = hist.index[-1]
        today_date = latest_dt.date()
        current_time = latest_dt.time()
        
        # 1. 今日の出来高積算
        today_data = hist[hist.index.date == today_date]
        actual_vol = today_data['Volume'].sum()
        
        # 2. 過去20日間の同時刻平均
        past_data = hist[hist.index.date < today_date]
        unique_dates = pd.Series(past_data.index.date).unique()[-20:]
        
        past_vols = []
        for d in unique_dates:
            day_slice = past_data[past_data.index.date == d]
            # 寄り付き(09:30)から現在と同じ時刻までを合計
            vol_until_now = day_slice.between_time("09:30", current_time)['Volume'].sum()
            if vol_until_now > 0:
                past_vols.append(vol_until_now)

        if not past_vols:
            return f"⚠️ ${ticker}: 比較用データが不足しています。"

        expected_vol = sum(past_vols) / len(past_vols)
        rvol = actual_vol / expected_vol if expected_vol > 0 else 0
        
        # レポート整形
        emoji = "🔥" if rvol >= 1.5 else "✅" if rvol >= 1.0 else "💤"
        price = float(hist['Close'].iloc[-1])
        change = (price / float(today_data['Open'].iloc[0]) - 1) * 100

        return (f"【高精度RVOL解析】\n"
                f"銘柄: ${ticker}\n"
                f"価格: ${price:.2f} ({change:+.2f}% vs Open)\n"
                f"RVOL: {rvol:.2f}x {emoji}\n\n"
                f"※過去20日間の同時刻平均({current_time.strftime('%H:%M')}時点)と比較")

    except Exception as e:
        return f"❌ エラー: {str(e)}"
