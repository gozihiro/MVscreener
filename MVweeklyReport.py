import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import io
import re
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- 環境変数 ---
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')
SUMMARY_FOLDER_ID = os.environ.get('SUMMARY_FOLDER_ID')

def get_drive_service():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def fetch_latest_summary():
    service = get_drive_service()
    # 最新の週次トレンドCSVを検索
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    files = res.get('files', [])
    if not files:
        print("❌ 分析対象のCSVが見つかりません。")
        sys.exit(1)
    
    print(f"📂 分析対象ファイル: {files[0]['name']}")
    req = service.files().get_media(fileId=files[0]['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh), files[0]['name']

def create_interactive_report(df):
    # 日付列の抽出 (価格_MM/DD の形式から日付部分だけ取り出す)
    dates = sorted(list(set([c.split('_')[-1] for c in df.columns if '価格_' in c and '/' in c])))
    
    # 1. 市場の「健康状態」トレンドチャート
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    ad_ratios = []
    dist_days = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け日:\s*(\d+)', meta)
        ad_ratios.append(float(ad.group(1)) if ad else 1.0)
        dist_days.append(int(dist.group(1)) if dist else 0)

    fig_market = make_subplots(specs=[[{"secondary_y": True}]])
    fig_market.add_trace(go.Bar(x=dates, y=ad_ratios, name="A/D比", marker_color='lightblue'), secondary_y=False)
    fig_market.add_trace(go.Scatter(x=dates, y=dist_days, name="売り抜け日", line=dict(color='red', width=3)), secondary_y=True)
    fig_market.update_layout(title_text="📡 市場環境トレンド (A/D比 vs 売り抜け日)", xaxis_title="日付")
    
    # 2. 有望株の「タイトネス（収束）」確認チャート
    latest_date = dates[-1]
    # 出現回数が多く、最新の売上成長が高い上位5銘柄をピックアップ
    leading_stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].sort_values(
        by=['出現回数', f'売上成長(%)_{latest_date}'], ascending=False).head(5)
    
    fig_tight = go.Figure()
    for _, row in leading_stocks.iterrows():
        prices = []
        for d in dates:
            p = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            prices.append(p)
        
        # 初日の価格を100%として正規化推移を表示
        base_p = next((p for p in prices if pd.notnull(p)), None)
        if base_p:
            norm_prices = [((p / base_p) - 1) * 100 if pd.notnull(p) else None for p in prices]
            fig_tight.add_trace(go.Scatter(x=dates, y=norm_prices, mode='lines+markers', name=row['銘柄']))

    fig_tight.update_layout(title_text="📉 有望株のボラティリティ収束 (%推移)", yaxis_title="変化率 (%)")

    # 3. 超優秀銘柄の成長率分布 (ヒートマップ風)
    growth_data = df[(df['銘柄'] != '### MARKET_ENVIRONMENT ###') & 
                     (df[f'成長性判定_{latest_date}'].str.contains('超優秀', na=False))]
    
    if not growth_data.empty:
        fig_heat = px.bar(growth_data, x='銘柄', y=f'売上成長(%)_{latest_date}', 
                          color=f'パターン_{latest_date}',
                          hover_data=[f'価格_{latest_date}', f'成長性判定_{latest_date}'],
                          title="💎 今週の【超優秀】銘柄：成長率ランキング")
    else:
        fig_heat = go.Figure().update_layout(title_text="💎 今週は【超優秀】判定銘柄がありませんでした")

    # HTML統合
    report_html = f"<html><head><meta charset='utf-8'><title>MV Analysis Report</title></head><body>"
    report_html += f"<h1>📊 週次投資判断レポート: {datetime.now().strftime('%Y-%m-%d')}</h1>"
    report_html += fig_market.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += fig_tight.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += fig_heat.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += "</body></html>"
    
    return report_html

def upload_to_drive(content, filename):
    service = get_drive_service()
    file_metadata = {'name': filename, 'parents': [SUMMARY_FOLDER_ID], 'mimeType': 'text/html'}
    
    # BytesIOを使用してメモリ上のHTMLデータをアップロード (MediaIoBaseUploadを使用)
    fh = io.BytesIO(content.encode('utf-8'))
    media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
    
    # 同名ファイルがあるか確認（あれば更新、なければ新規）
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{filename}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])

    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"✅ レポートを更新しました: {filename}")
    else:
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ レポートを新規保存しました: {filename}")

if __name__ == "__main__":
    trend_df, csv_name = fetch_latest_summary()
    html_report = create_interactive_report(trend_df)
    
    # ファイル名を weekly_detailed_trend_YYYYMMDD.csv -> interactive_report_YYYYMMDD.html に変換
    report_filename = csv_name.replace('weekly_detailed_trend', 'interactive_report').replace('.csv', '.html')
    
    # ローカル保存（Artifact用）
    with open("weekly_report.html", "w", encoding="utf-8") as f:
        f.write(html_report)
    
    # Drive保存
    upload_to_drive(html_report, report_filename)
