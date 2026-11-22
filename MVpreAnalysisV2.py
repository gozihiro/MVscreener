import os
import sys
import re
import io
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- 環境変数の取得 ---
def get_env(name):
    return os.environ.get(name)


CLIENT_ID = get_env('CLIENT_ID')
CLIENT_SECRET = get_env('CLIENT_SECRET')
REFRESH_TOKEN = get_env('REFRESH_TOKEN')
SOURCE_FOLDER_ID = get_env('GDRIVE_FOLDER_ID')
SUMMARY_FOLDER_ID = get_env('SUMMARY_FOLDER_ID')

REQUIRED_COLS = [
    '銘柄', '価格', 'パターン', '成長性判定', '売上成長(%)',
    '純利益成長(%)', '時価総額(B)', '発射台スコア', '10EMA', '20SMA', '50SMA'
]


def get_drive_service():
    creds = Credentials(
        token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)


def extract_market_date(metadata_line, file_name, created_time_iso):
    """
    優先順位で米国実市場日 (YYYY/MM/DD) を特定する:
    1. REPORT_METADATA 内の '市場日:YYYY/MM/DD'（新形式・最優先・タイムゾーン誤差ゼロ）
    2. ファイル名 'minervini_final_results_YYYYMMDD.csv'（新形式）
    3. フォールバック: Driveの createdTime (JST変換後に-1日)（旧形式CSVとの後方互換用）
    """
    m = re.search(r'市場日:\s*(\d{4}/\d{2}/\d{2})', metadata_line or "")
    if m:
        return m.group(1)

    m_name = re.search(r'(\d{4})(\d{2})(\d{2})', file_name or "")
    if m_name:
        return f"{m_name.group(1)}/{m_name.group(2)}/{m_name.group(3)}"

    jst = timezone(timedelta(hours=9))
    dt_utc = datetime.fromisoformat(created_time_iso.replace('Z', '+00:00'))
    dt_jst = dt_utc.astimezone(jst)
    return (dt_jst - timedelta(days=1)).strftime('%Y/%m/%d')


def fetch_weekly_data(max_days=5):
    """
    Drive上の最新 minervini_final_results CSV を modifiedTime 降順で取得し、
    実市場日（YYYY/MM/DD）ごとに最新1件へ重複排除した上で直近 max_days 営業日分を返す。
    ※createdTimeウィンドウ検索による1営業日ラグやJST/UTCズレを完全に解消。
    """
    service = get_drive_service()
    print(f"=== Phase 1: データ収集 (Source: {SOURCE_FOLDER_ID}) ===")

    query = (
        f"'{SOURCE_FOLDER_ID}' in parents and name contains 'minervini_final_results' "
        f"and trashed = false"
    )
    res = service.files().list(
        q=query,
        fields="files(id, name, createdTime, modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=20
    ).execute()
    files = res.get('files', [])

    by_date = {}
    for f in files:
        file_id = f['id']
        file_name = f.get('name', '')
        created_time = f.get('createdTime', '')

        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        raw_data = fh.read().decode('utf-8-sig').splitlines()
        if not raw_data:
            continue

        metadata_raw = raw_data[0]
        metadata_line = metadata_raw.split(',', 1)[1] if ',' in metadata_raw else metadata_raw
        full_date = extract_market_date(metadata_line, file_name, created_time)

        # 同じ市場日のファイルが複数ある場合は modifiedTime が最新（先頭）のものを採用
        if full_date in by_date:
            continue

        df = pd.read_csv(io.StringIO("\n".join(raw_data[1:])))
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = "不明"

        df = df[REQUIRED_COLS].copy()
        df['Date'] = full_date
        by_date[full_date] = (df, {'Date': full_date, 'Metadata': metadata_line})
        print(f"✅ {full_date} ({file_name}): 収集完了")

        if len(by_date) >= max_days:
            break

    sorted_dates = sorted(by_date.keys())
    weekly_dfs = [by_date[d][0] for d in sorted_dates]
    market_metadatas = [by_date[d][1] for d in sorted_dates]
    return weekly_dfs, market_metadatas


def analyze_detailed_trend(dfs, metadatas):
    if not dfs:
        return None
    print("=== Phase 2: トレンド集計 ===")
    all_raw = pd.concat(dfs, ignore_index=True)
    trend_df = all_raw.groupby('銘柄').size().reset_index(name='出現回数')
    market_row = {'銘柄': '### MARKET_ENVIRONMENT ###', '出現回数': '-'}

    for df, meta in zip(dfs, metadatas):
        date = meta['Date']
        market_row[f'価格_{date}'] = meta['Metadata']
        daily_data = df.drop_duplicates(subset=['銘柄']).set_index('銘柄').drop(columns=['Date']).add_suffix(f'_{date}')
        trend_df = trend_df.merge(daily_data, on='銘柄', how='left')

    result = pd.concat([pd.DataFrame([market_row]), trend_df], ignore_index=True)

    # 【修正】dfs[-1]['Date']（Series）ではなく文字列の metadatas[-1]['Date'] を使用
    latest_date = metadatas[-1]['Date']
    sort_col = f'売上成長(%)_{latest_date}'
    if sort_col in result.columns:
        result['_sort'] = pd.to_numeric(result[sort_col], errors='coerce').fillna(-999)
        market_header = result.iloc[:1]
        stock_data = result.iloc[1:].sort_values(by=['出現回数', '_sort'], ascending=False)
        result = pd.concat([market_header, stock_data]).drop(columns=['_sort'])
    return result.fillna('－')


def upload_result_to_drive(file_path, latest_market_date=None):
    service = get_drive_service()
    if latest_market_date:
        date_tag = latest_market_date.replace('/', '')
    else:
        date_tag = datetime.now(timezone(timedelta(hours=9))).strftime('%Y%m%d')
    file_name = f"weekly_detailed_trend_{date_tag}.csv"
    file_metadata = {'name': file_name, 'parents': [SUMMARY_FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{file_name}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"✅ Summary内の既存ファイルを更新しました: {file_name}")
    else:
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ Summary内に新規ファイルを保存しました: {file_name}")


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
            upload_result_to_drive(output_file, latest_market_date=metadatas[-1]['Date'])
    else:
        print("⚠️ 有効データなし")
