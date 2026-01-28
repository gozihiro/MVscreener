import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- 環境変数の取得 ---
def get_env(name):
    return os.environ.get(name)

CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
FOLDER_ID = get_env('GDRIVE_FOLDER_ID')

# 必須項目の定義
REQUIRED_COLS = [
    '銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)', 
    '営業利益成長(EBITDA)%', '純利益成長(%)', '営業CF(M)', '時価総額(B)'
]

def get_drive_service():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def get_target_time_ranges():
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    ranges = []
    for i in range(7):
        day = now - timedelta(days=i)
        if day.weekday() in [1, 2, 3, 4, 5]: # 火〜土(JST) = 月〜金(Market)
            start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            end = day.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges.append((start, end))
        if len(ranges) == 5: break
    return sorted(ranges)

def fetch_weekly_data():
    service = get_drive_service()
    ranges = get_target_time_ranges()
    weekly_dfs = []

    for start, end in ranges:
        market_date = (start - timedelta(days=1)).strftime('%m/%d')
        q = f"'{FOLDER_ID}' in parents and name = 'minervini_final_results.csv' and createdTime >= '{start.isoformat()}' and createdTime <= '{end.isoformat()}' and trashed = false"
        res = service.files().list(q=q, fields="files(id, createdTime)", orderBy="createdTime").execute()
        files = res.get('files', [])

        if files:
            req = service.files().get_media(fileId=files[0]['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, skiprows=1)
            
            # 列の正規化（存在しない列は「不明」で埋める）
            for col in REQUIRED_COLS:
                if col not in df.columns:
                    df[col] = "不明"
            
            df = df[REQUIRED_COLS].copy()
            df['Date'] = market_date
            weekly_dfs.append(df)
            print(f"✅ {market_date} のデータを読み込みました")
    return weekly_dfs

def analyze_trend(dfs):
    if not dfs: return None
    
    # 全日程の銘柄リストを作成
    all_data = pd.concat(dfs, ignore_index=True)
    tickers = all_data['銘柄'].unique()
    
    # 銘柄ごとの出現回数
    persistence = all_data.groupby('銘柄').size().reset_index(name='出現回数')
    
    # 横持ち（Pivot）形式でトレンドを作成
    # 各日付のデータを列に展開
    trend_df = persistence.copy()
    
    for df in dfs:
        date_str = df['Date'].iloc[0]
        # その日のデータを抽出し、列名を日付付きに変更
        daily_df = df.set_index('銘柄').add_suffix(f'_{date_str}')
        trend_df = trend_df.merge(daily_df, on='銘柄', how='left')

    # カラムの並び替え（銘柄, 出現回数, 日付ごとの全項目...）
    return trend_df.fillna('－')

if __name__ == "__main__":
    dfs = fetch_weekly_data()
    result = analyze_trend(dfs)
    
    if result is not None:
        save_name = "weekly_detailed_trend.csv"
        result.to_csv(save_name, index=False, encoding='utf-8-sig')
        print(f"\n>> {save_name} に詳細な遷移データを保存しました。")
        # 上位5件のみ表示
        print(result.head(5).to_string())
