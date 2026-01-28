import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# --- 環境変数の取得（GitHub Secretsから注入） ---
def get_env(name):
    return os.environ.get(name)

CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
FOLDER_ID = get_env('GDRIVE_FOLDER_ID')

# 分析対象とする全カラムの定義
REQUIRED_COLS = [
    '銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)', 
    '営業利益成長(EBITDA)%', '純利益成長(%)', '営業CF(M)', '時価総額(B)'
]

def get_drive_service():
    """Google Drive APIへの接続"""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def get_target_time_ranges():
    """今日(JST)から遡り、直近5営業日分の検索範囲を生成"""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    
    ranges = []
    # 過去7日間をスキャンし、マーケット結果が生成される「火〜土」の5日分を確保
    for i in range(7):
        day = now - timedelta(days=i)
        if day.weekday() in [1, 2, 3, 4, 5]: # 火(1)〜土(5)
            start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            end = day.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges.append((start, end))
        if len(ranges) == 5:
            break
    return sorted(ranges)

def fetch_weekly_data():
    """Driveから5日分のCSVを取得してDF化"""
    service = get_drive_service()
    ranges = get_target_time_ranges()
    weekly_dfs = []

    print("=== Phase 1: データ収集フェーズ (JST) ===")
    for start, end in ranges:
        market_date = (start - timedelta(days=1)).strftime('%m/%d')
        q = (f"'{FOLDER_ID}' in parents and name = 'minervini_final_results.csv' "
             f"and createdTime >= '{start.isoformat()}' and createdTime <= '{end.isoformat()}' "
             f"and trashed = false")
        
        res = service.files().list(q=q, fields="files(id, createdTime)", orderBy="createdTime").execute()
        files = res.get('files', [])

        if files:
            file_id = files[0]['id']
            print(f"✅ {market_date}: 発見 (作成日時: {files[0]['createdTime']})")
            
            req = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = downloader.next_chunk()
            
            fh.seek(0)
            # 1行目のREPORT_METADATAを読み飛ばし
            df = pd.read_csv(fh, skiprows=1)
            
            # 列の存在チェックと正規化
            for col in REQUIRED_COLS:
                if col not in df.columns:
                    df[col] = "不明"
            
            df = df[REQUIRED_COLS].copy()
            df['Date'] = market_date
            weekly_dfs.append(df)
        else:
            print(f"❌ {market_date}: ファイル未検出")

    return weekly_dfs

def analyze_detailed_trend(dfs):
    """全項目の日次推移表を作成"""
    if not dfs: return None
    
    print("=== Phase 2: トレンド分析フェーズ ===")
    # 全日程をマージ
    all_raw = pd.concat(dfs, ignore_index=True)
    
    # 銘柄ごとの出現回数を集計
    summary = all_raw.groupby('銘柄').size().reset_index(name='出現回数')
    
    # 日付ごとにデータを横に並べる (Pivot)
    trend_df = summary.copy()
    for df in dfs:
        date_label = df['Date'].iloc[0]
        daily_part = df.set_index('銘柄').add_suffix(f'_{date_label}')
        # 出現回数データの横に、その日の全項目を結合
        trend_df = trend_df.merge(daily_part.drop(columns=[f'Date_{date_label}']), on='銘柄', how='left')

    # 出現回数が多く、かつ直近の売上成長が高い順にソート
    # 最新日付の「売上成長(%)」カラムを探す
    latest_date = dfs[-1]['Date'].iloc[0]
    sort_col = f'売上成長(%)_{latest_date}'
    if sort_col in trend_df.columns:
        # '不明'を考慮して数値変換
        trend_df['_sort_val'] = pd.to_numeric(trend_df[sort_col], errors='coerce').fillna(-999)
        trend_df = trend_df.sort_values(by=['出現回数', '_sort_val'], ascending=False).drop(columns=['_sort_val'])
    
    return trend_df.fillna('－')

def upload_result_to_drive(file_path):
    """分析結果をGoogle Driveに保存"""
    service = get_drive_service()
    file_name = f"weekly_detailed_trend_{datetime.now().strftime('%Y%m%d')}.csv"
    
    file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    
    # 同名ファイルの上書きチェック
    query = f"'{FOLDER_ID}' in parents and name = '{file_name}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])

    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"✅ Drive上の既存ファイルを更新しました: {file_name}")
    else:
        new_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ Driveに新規保存しました: {file_name} (ID: {new_file.get('id')})")

if __name__ == "__main__":
    if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, FOLDER_ID]):
        print("❌ 認証情報の不足。GitHub Secretsを確認してください。")
        sys.exit(1)

    weekly_data = fetch_weekly_data()
    if weekly_data:
        trend_result = analyze_detailed_trend(weekly_data)
        if trend_result is not None:
            output_file = "weekly_detailed_trend.csv"
            trend_result.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"✅ ローカル保存完了: {output_file}")
            
            # Driveへアップロード
            upload_result_to_drive(output_file)
    else:
        print("⚠️ 処理対象のデータがありませんでした。")
