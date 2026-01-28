import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import re

# --- 環境変数の取得 ---
def get_env(name):
    return os.environ.get(name)

CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
PARENT_FOLDER_ID = get_env('GDRIVE_FOLDER_ID')

REQUIRED_COLS = [
    '銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)', 
    '営業利益成長(EBITDA)%', '純利益成長(%)', '営業CF(M)', '時価総額(B)'
]

def get_drive_service():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def get_or_create_summary_folder(service):
    """現在のフォルダ内にSummaryフォルダを特定または作成"""
    query = f"'{PARENT_FOLDER_ID}' in parents and name = 'Summary' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    res = service.files().list(q=query, fields="files(id)").execute()
    files = res.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        file_metadata = {
            'name': 'Summary',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [PARENT_FOLDER_ID]
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
        print(f"📁 Summaryフォルダを新規作成しました。")
        return folder.get('id')

def get_target_time_ranges():
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    ranges = []
    for i in range(7):
        day = now - timedelta(days=i)
        if day.weekday() in [1, 2, 3, 4, 5]:
            start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            end = day.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges.append((start, end))
        if len(ranges) == 5: break
    return sorted(ranges)

def fetch_weekly_data():
    service = get_drive_service()
    ranges = get_target_time_ranges()
    weekly_dfs = []
    market_metadatas = []

    print("=== Phase 1: データ収集と市場環境解析 (JST) ===")
    for start, end in ranges:
        market_date = (start - timedelta(days=1)).strftime('%m/%d')
        q = f"'{PARENT_FOLDER_ID}' in parents and name = 'minervini_final_results.csv' and createdTime >= '{start.isoformat()}' and createdTime <= '{end.isoformat()}' and trashed = false"
        res = service.files().list(q=q, fields="files(id, createdTime)", orderBy="createdTime").execute()
        files = res.get('files', [])

        if files:
            req = service.files().get_media(fileId=files[0]['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = downloader.next_chunk()
            
            fh.seek(0)
            raw_content = fh.read().decode('utf-8-sig').splitlines()
            
            # 1行目の市場環境データを解析
            metadata_line = raw_content[0] if raw_content else ""
            market_metadatas.append({'Date': market_date, 'Metadata': metadata_line})
            
            # データ本体を読み込み
            df = pd.read_csv(io.StringIO("\n".join(raw_content[1:])))
            for col in REQUIRED_COLS:
                if col not in df.columns: df[col] = "不明"
            
            df = df[REQUIRED_COLS].copy()
            df['Date'] = market_date
            weekly_dfs.append(df)
            print(f"✅ {market_date}: 収集完了 (市場データ含む)")
        else:
            print(f"❌ {market_date}: 未検出")

    return weekly_dfs, market_metadatas

def analyze_detailed_trend(dfs, metadatas):
    if not dfs: return None
    
    print("=== Phase 2: 市場・銘柄トレンド分析 ===")
    all_raw = pd.concat(dfs, ignore_index=True)
    trend_df = all_raw.groupby('銘柄').size().reset_index(name='出現回数')

    # 市場環境行（特殊行）の作成
    market_row = {'銘柄': '### MARKET_ENVIRONMENT ###', '出現回数': '-'}
    
    for meta in metadatas:
        date = meta['Date']
        # 市場ステータス等の抽出（"ラリー試行中"などを取得）
        market_row[f'価格_{date}'] = meta['Metadata']
        
        # 銘柄ごとの各項目を結合
        daily_df = [d for d in dfs if d['Date'] == date][0].set_index('銘柄').add_suffix(f'_{date}')
        trend_df = trend_df.merge(daily_df.drop(columns=[f'Date_{date}']), on='銘柄', how='left')

    # 市場環境を1行目に挿入
    result = pd.concat([pd.DataFrame([market_row]), trend_df], ignore_index=True)
    
    # ソート（市場環境行を最上部に固定し、以降は出現回数順）
    latest_date = dfs[-1]['Date']
    sort_col = f'売上成長(%)_{latest_date}'
    if sort_col in result.columns:
        result['_sort'] = pd.to_numeric(result[sort_col], errors='coerce').fillna(-999)
        # 1行目以外をソート
        top = result.iloc[:1]
        others = result.iloc[1:].sort_values(by=['出現回数', '_sort'], ascending=False)
        result = pd.concat([top, others]).drop(columns=['_sort'])
    
    return result.fillna('－')

def upload_result_to_drive(file_path):
    service = get_drive_service()
    summary_folder_id = get_or_create_summary_folder(service)
    
    file_name = f"weekly_detailed_trend_{datetime.now().strftime('%Y%m%d')}.csv"
    file_metadata = {'name': file_name, 'parents': [summary_folder_id]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    
    query = f"'{summary_folder_id}' in parents and name = '{file_name}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])

    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"✅ Summaryフォルダ内のファイルを更新しました: {file_name}")
    else:
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ Summaryフォルダに新規保存しました: {file_name}")

if __name__ == "__main__":
    if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, PARENT_FOLDER_ID]):
        print("❌ 認証情報の不足。")
        sys.exit(1)

    weekly_data, metadatas = fetch_weekly_data()
    if weekly_data:
        trend_result = analyze_detailed_trend(weekly_data, metadatas)
        if trend_result is not None:
            output_file = "weekly_detailed_trend.csv"
            trend_result.to_csv(output_file, index=False, encoding='utf-8-sig')
            upload_result_to_drive(output_file)
    else:
        print("⚠️ データ不足により中断")
