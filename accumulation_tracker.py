import os
import io
import pandas as pd
import yfinance as yf
from datetime import datetime
import time
import requests
import random
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- 環境変数 ---
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')
ACCUMULATION_FOLDER_ID = os.environ.get('ACCUMULATION_FOLDER_ID')

# --- ミネルヴィニ流・中型株までのレンジ設定 ---
# スーパーパフォーマーの初動を捉えるため、1.5億ドル〜200億ドルに設定
MIN_MARKET_CAP = 150_000_000      # 150M (機関投資家の最低ライン)
MAX_MARKET_CAP = 20_000_000_000   # 20B (中型株の上限目安)

# --- 実行制限回避の設定 (既存スクリーナーを継承) ---
BATCH_SIZE = 50
BATCH_SLEEP_BASE = 85

def get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def get_all_us_tickers():
    """全米上場銘柄リストの取得（ここを特定の監視リストCSVに置き換えることも可能）"""
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        # 403エラー回避のためUser-Agentを付与
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return []
        
        # 記号（$）等を含む不正なティッカーを除外してエラー回避・高速化
        tickers = [s.strip() for s in response.text.splitlines() if s.strip().isalpha()]
        return sorted(list(set(tickers)))
    except:
        return []

def is_accumulation_stealth(df, ticker):
    """JMIA型 判定ロジック：10日中7日陽線 ＋ 7.7%幅 ＋ 短期序列 ＋ 10EMA密着"""
    if len(df) < 60: return False # MA50の安定計算のため期間を確保
    
    recent = df.tail(10).copy()
    
    # 1. 陽線頻度 (10日中7日以上)
    up_days = (recent['Close'] > recent['Open']).sum()
    if up_days < 7: return False
    
    # 2. 値幅のタイトネス (日次7.7%以内)
    daily_ranges = (recent['High'] - recent['Low']) / recent['Close']
    if daily_ranges.max() > 0.077: return False
    
    # 3. 移動平均線の序列 (10EMA > 20SMA > 50SMA)
    ema10 = df['Close'].ewm(span=10, adjust=False).mean()
    sma20 = df['Close'].rolling(window=20).mean()
    sma50 = df['Close'].rolling(window=50).mean()
    
    if not (ema10.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1]):
        return False
    
    # 4. 10EMAとの密着度 (乖離3%以内)
    last_p = df['Close'].iloc[-1]
    last_e10 = ema10.iloc[-1]
    if not (last_e10 < last_p < last_e10 * 1.03):
        return False

    # 5. 時価総額フィルター (テクニカル合格後にのみ実行して通信を節約)
    try:
        info = yf.Ticker(ticker).fast_info
        m_cap = info['market_cap']
        if not (MIN_MARKET_CAP <= m_cap <= MAX_MARKET_CAP):
            return False
    except:
        return False

    return True

def get_current_accumulation_states(service):
    """既存フォルダの生存カウントを取得"""
    states = {}
    page_token = None
    query = f"'{ACCUMULATION_FOLDER_ID}' in parents and trashed = false"
    
    while True:
        results = service.files().list(
            q=query, 
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        
        for f in results.get('files', []):
            if f['name'].startswith('['):
                try:
                    parts = f['name'].split('_')
                    count = int(parts[0][1:3])
                    ticker = parts[1]
                    states[ticker] = {'id': f['id'], 'count': count}
                except: continue
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    return states

def run_tracker():
    service = get_drive_service()
    watchlist = get_all_us_tickers()
    
    print(f"Watchlist count: {len(watchlist)}")
    if not watchlist: return

    current_states = get_current_accumulation_states(service)
    print(f"Current states: {len(current_states)}")
    
    today_str = datetime.now().strftime('%Y%m%d')
    processed_tickers = set()
    scanned_tickers = set()

    # 既存スクリーナーと同様のバッチ処理
    for i in range(0, len(watchlist), BATCH_SIZE):
        batch = watchlist[i:i + BATCH_SIZE]
        try:
            # バッチ一括ダウンロード
            data = yf.download(batch, period="6mo", interval="1d", progress=False, auto_adjust=True, threads=True)
            if data.empty:
                time.sleep(BATCH_SLEEP_BASE)
                continue

            for ticker in batch:
                scanned_tickers.add(ticker)
                try:
                    # バッチデータから個別銘柄を抽出
                    if ticker not in data['Close'].columns: continue
                    df = data.xs(ticker, axis=1, level=1).dropna()
                    
                    # カラム名補正（yf.downloadはOpen, High, Low, Closeを出力）
                    if df.empty or len(df) < 25: continue
                    
                    # 条件判定
                    if is_accumulation_stealth(df, ticker):
                        prev_state = current_states.get(ticker)
                        new_count = (prev_state['count'] + 1) if prev_state else 1
                        new_filename = f"[{new_count:02d}]_{ticker}_{today_str}.csv"
                        
                        # フォルダ内の古い同銘柄ファイルを削除
                        if prev_state:
                            service.files().delete(fileId=prev_state['id']).execute()
                        
                        # 保存
                        output = io.StringIO()
                        df.tail(20).to_csv(output)
                        fh = io.BytesIO(output.getvalue().encode('utf-8'))
                        media = MediaIoBaseUpload(fh, mimetype='text/csv')
                        meta = {'name': new_filename, 'parents': [ACCUMULATION_FOLDER_ID]}
                        service.files().create(body=meta, media_body=media).execute()
                        
                        print(f"Update: {ticker} (Day {new_count})")
                        processed_tickers.add(ticker)
                except:
                    continue

        except Exception as e:
            print(f"Batch Error {i}: {e}")
        
        # バッチごとの待機 (既存スクリーナーの設定)
        print(f"Progress: {min(i + BATCH_SIZE, len(watchlist))} tickers scanned...")
        time.sleep(BATCH_SLEEP_BASE + random.uniform(0, 10))

    # 脱落銘柄のクリーンアップ
    for ticker, state in current_states.items():
        if ticker in scanned_tickers and ticker not in processed_tickers:
            service.files().delete(fileId=state['id']).execute()
            print(f"Drop: {ticker}")

if __name__ == "__main__":
    run_tracker()
