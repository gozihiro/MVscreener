import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# --- 環境変数の取得 ---
def get_env(name):
    return os.environ.get(name)

CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
# ソースファイル（minervini_final_results_YYYYMMDD.csv）があるフォルダ
SOURCE_FOLDER_ID = get_env('GDRIVE_FOLDER_ID')
# 分析結果を保存する Summary フォルダのID
SUMMARY_FOLDER_ID = get_env('SUMMARY_FOLDER_ID')

# 【重要】集計対象項目に「発射台スコア」を追加
REQUIRED_COLS = [
    '銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)', 
    '営業利益成長(EBITDA)%', '純利益成長(%)', '営業CF(M)', '時価総額(B)',
    '発射台スコア'
]

def get_drive_service():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def get_target_time_ranges():
    """過去7日間から市場が稼働していた直近5日分（火〜土 JST）を特定"""
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
    """Driveから対象期間のCSVを収集"""
    service = get_drive_service()
    ranges = get_target_time_ranges()
    weekly_dfs = []
    market_metadatas = []

    print(f"=== Phase 1: データ収集 (Source: {SOURCE_FOLDER_ID}) ===")
    for start, end in ranges:
        market_date = (start - timedelta(days=1)).strftime('%m/%d')
        
        # 日付付きファイル名に対応した検索クエリ
        query = (f"'{SOURCE_FOLDER_ID}' in parents and name contains 'minervini_final_results' "
                 f"and createdTime >= '{start.isoformat()}' and createdTime <= '{end.isoformat()}' "
                 f"and trashed = false")
        
        res = service.files().list(q=query, fields="files(id, createdTime)", orderBy="createdTime").execute()
        files = res.get('files', [])

        if files:
            # 該当期間内で最も新しいファイルを取得
            file_id = files[-1]['id']
            req = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = downloader.next_chunk()
            
            fh.seek(0)
            raw_data = fh.read().decode('utf-8-sig').splitlines()
            
            # 1行目の市場環境メタデータ (REPORT_METADATA)
            metadata_line = raw_data[0] if raw_data else "No Metadata"
            market_metadatas.append({'Date': market_date, 'Metadata': metadata_line})
            
            # 2行目以降の銘柄データ
            df = pd.read_csv(io.StringIO("\n".join(raw_data[1:])))
            
            # 必要な列が欠落している場合に備えた補完処理
            for col in REQUIRED_COLS:
                if col not in df.columns: df[col] = "不明"
            
            df = df[REQUIRED_COLS].copy()
            df['Date'] = market_date
            weekly_dfs.append(df)
            print(f"✅ {market_date}: 収集完了")
        else:
            print(f"❌ {market_date}: 未検出")

    return weekly_dfs, market_metadatas

def analyze_detailed_trend(dfs, metadatas):
    """収集したデータを横持ちの時系列データへ変換"""
    if not dfs: return None
    
    print("=== Phase 2: トレンド集計 (Launchpad Score含む) ===")
    all_raw = pd.concat(dfs, ignore_index=True)
    
    # 全期間を通じての出現回数をカウント
    trend_df = all_raw.groupby('銘柄').size().reset_index(name='出現回数')
    
    # 市場環境用の行を作成
    market_row = {'銘柄': '### MARKET_ENVIRONMENT ###', '出現回数': '-'}

    for df, meta in zip(dfs, metadatas):
        date = meta['Date']
        market_row[f'価格_{date}'] = meta['Metadata']
        
        # 日付ごとの各項目をマージ
        daily_data = df.set_index('銘柄').drop(columns=['Date']).add_suffix(f'_{date}')
        trend_df = trend_df.merge(daily_data, on='銘柄', how='left')

    result = pd.concat([pd.DataFrame([market_row]), trend_df], ignore_index=True)

    # 既存のソートロジックを維持（最新日の売上成長で簡易ソート）
    latest_date = dfs[-1]['Date']
    sort_col = f'売上成長(%)_{latest_date}'
    
    if sort_col in result.columns:
        result['_sort'] = pd.to_numeric(result[sort_col], errors='coerce').fillna(-999)
        market_header = result.iloc[:1]
        stock_data = result.iloc[1:].sort_values(by=['出現回数', '_sort'], ascending=False)
        result = pd.concat([market_header, stock_data]).drop(columns=['_sort'])
    
    return result.fillna('－')

def upload_result_to_drive(file_path):
    """Summaryフォルダへ日付付きで保存"""
    service = get_drive_service()
    
    file_name = f"weekly_detailed_trend_{datetime.now().strftime('%Y%m%d')}.csv"
    file_metadata = {'name': file_name, 'parents': [SUMMARY_FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{file_name}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])

    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"✅ Summary内の既存ファイルを更新しました")
    else:
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ Summary内に新規ファイルを保存しました")

if __name__ == "__main__":
    if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, SOURCE_FOLDER_ID, SUMMARY_FOLDER_ID]):
        print("❌ エラー: 環境変数が不足しています。")
        sys.exit(1)

    weekly_data, metadatas = fetch_weekly_data()
    if weekly_data:
        trend_result = analyze_detailed_trend(weekly_data, metadatas)
        if trend_result is not None:
            output_file = "weekly_detailed_trend.csv"
            trend_result.to_csv(output_file, index=False, encoding='utf-8-sig')
            upload_result_to_drive(output_file)
    else:
        print("⚠️ 有効データなし。処理を中止します。")
