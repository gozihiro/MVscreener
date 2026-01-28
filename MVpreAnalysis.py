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
    val = os.environ.get(name)
    if not val or val.strip() == "":
        return None
    return val

CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
FOLDER_ID = get_env('GDRIVE_FOLDER_ID')

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, FOLDER_ID]):
    print("❌ エラー: 必要な認証情報（CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, GDRIVE_FOLDER_ID）が不足しています。")
    sys.exit(1)

def get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def get_target_time_ranges():
    """今日(JST)から遡り、直近5営業日分(JST火〜土)の検索範囲を生成"""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    
    ranges = []
    # 過去7日間をスキャンし、マーケット結果が生成される「火〜土」の5日分を確保
    for i in range(7):
        day = now - timedelta(days=i)
        # 火(1)〜土(5) の朝に生成されたファイルが、月〜金のマーケット結果
        if day.weekday() in [1, 2, 3, 4, 5]:
            # 時間枠を広げ、その日のうちに作成されたものを対象にする
            start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            end = day.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges.append((start, end))
        
        if len(ranges) == 5:
            break
            
    return sorted(ranges) # 日付の古い順にソート

def fetch_weekly_data():
    service = get_drive_service()
    ranges = get_target_time_ranges()
    weekly_dfs = []

    print("=== 週次データ特定フェーズ (JST) ===")
    for start, end in ranges:
        # 市場が動いた日付（JST朝のファイル作成日の前日）を表示用にする
        market_date = (start - timedelta(days=1)).strftime('%Y-%m-%d')
        
        s_str = start.isoformat()
        e_str = end.isoformat()
        
        query = (f"'{FOLDER_ID}' in parents and name = 'minervini_final_results.csv' "
                 f"and createdTime >= '{s_str}' and createdTime <= '{e_str}' "
                 f"and trashed = false")
        
        results = service.files().list(q=query, fields="files(id, name, createdTime)", orderBy="createdTime").execute()
        files = results.get('files', [])

        if files:
            # その日の期間内で最初に作成されたファイルを正データとする
            file_id = files[0]['id']
            created_time = files[0]['createdTime']
            print(f"✅ {market_date}: 発見 (作成日時: {created_time})")
            
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            fh.seek(0)
            # 1行目のREPORT_METADATAをスキップ
            df = pd.read_csv(fh, skiprows=1)
            if '銘柄' in df.columns:
                df['Market_Date'] = market_date
                weekly_dfs.append(df)
        else:
            print(f"❌ {market_date}: 期間内にファイルが見つかりません ({start.date()})")

    return weekly_dfs

def analyze_weekly_persistence(dfs):
    """銘柄の出現頻度と直近データを集計"""
    if not dfs:
        return None
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # 出現頻度のカウント
    persistence = combined_df.groupby('銘柄').size().reset_index(name='出現回数')
    
    # 最新の情報を取得（Market_Dateが最新のものを採用）
    latest_info = combined_df.sort_values('Market_Date').groupby('銘柄').tail(1)
    
    # 結合して、出現回数と最新情報をまとめる
    result = pd.merge(persistence, latest_info[['銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)']], on='銘柄')
    
    # 出現回数、次いで売上成長率でソート
    return result.sort_values(by=['出現回数', '売上成長(%)'], ascending=False)

if __name__ == "__main__":
    dfs = fetch_weekly_data()
    
    if dfs:
        result_df = analyze_weekly_persistence(dfs)
        if result_df is not None:
            save_name = "weekly_persistence_summary.csv"
            result_df.to_csv(save_name, index=False, encoding='utf-8-sig')
            
            print(f"\n=== 週次銘柄定着率集計 (Top 20) ===")
            print(result_df.head(20).to_string(index=False))
            print(f"\n>> {save_name} に保存しました。")
    else:
        print("\n⚠️ 分析対象のデータが1日分も見つかりませんでした。")
