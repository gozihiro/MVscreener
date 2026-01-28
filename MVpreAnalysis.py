import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- 設定（既存の環境変数を流用） ---
FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')

def get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def get_target_dates():
    """直近の月〜金（マーケット日）に対応する、JST火〜土の朝の取得期間を生成"""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    
    # 直近の土曜日（金曜マーケット分）を起点に5日分遡る
    last_sat = now - timedelta(days=(now.weekday() - 5) % 7)
    if now.weekday() == 5 and now.hour < 6: # 土曜朝6時前なら先週分
        last_sat -= timedelta(days=7)
        
    dates = []
    for i in range(5): # 土、金、木、水、火（JST）
        target_day = last_sat - timedelta(days=i)
        start_time = target_day.replace(hour=6, minute=0, second=0, microsecond=0)
        end_time = target_day.replace(hour=15, minute=0, second=0, microsecond=0)
        dates.append((start_time, end_time))
    return sorted(dates) # 火〜土の順にソート

def fetch_weekly_csvs():
    service = get_drive_service()
    target_periods = get_target_dates()
    weekly_data = {}

    print(f"=== 週次データ特定フェーズ (JST) ===")
    for start, end in target_periods:
        # RFC3339形式に変換
        s_str = start.isoformat()
        e_str = end.isoformat()
        
        query = f"'{FOLDER_ID}' in parents and name = 'minervini_final_results.csv' and createdTime >= '{s_str}' and createdTime <= '{e_str}' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name, createdTime)", orderBy="createdTime").execute()
        files = results.get('files', [])

        market_date = (start - timedelta(days=1)).strftime('%Y-%m-%d (Mon-Fri)')
        if files:
            # 期間内の一番最初のファイルを正データとする
            file_id = files[0]['id']
            created_time = files[0]['createdTime']
            print(f"✅ {market_date} 用ファイル発見: {created_time}")
            
            # ダウンロード
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            fh.seek(0)
            # REPORT_METADATAをスキップして読み込み
            df = pd.read_csv(fh, skiprows=1)
            weekly_data[market_date] = df
        else:
            print(f"❌ {market_date} 用ファイルが見つかりません (期間: {s_str} - {e_str})")

    return weekly_data

def analyze_persistence(weekly_data):
    """銘柄の出現頻度を集計"""
    all_tickers = []
    for date, df in weekly_data.items():
        if '銘柄' in df.columns:
            all_tickers.extend(df['銘柄'].unique().tolist())
    
    summary = pd.Series(all_tickers).value_counts().reset_index()
    summary.columns = ['銘柄', '出現回数']
    return summary

if __name__ == "__main__":
    data = fetch_weekly_csvs()
    if data:
        persistence = analyze_persistence(data)
        print("\n=== 銘柄出現頻度（Top 20） ===")
        print(persistence.head(20))
        
        # CSVとして保存
        persistence.to_csv("weekly_persistence_summary.csv", index=False)
        print("\n>> weekly_persistence_summary.csv を保存しました。")
    else:
        print("分析対象のデータが取得できませんでした。")
