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
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

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
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    files = res.get('files', [])
    if not files:
        print("❌ 分析対象のCSVが見つかりません。")
        sys.exit(1)
    
    req = service.files().get_media(fileId=files[0]['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh), files[0]['name']

def create_interactive_report(df):
    # 日付列の抽出
    dates = sorted(list(set([c.split('_')[-1] for c in df.columns if '_' in c and '/' in c])))
    
    # 1. 市場の「健康状態」トレンドチャート (A/D比 & 売り抜け日)
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    ad_ratios = []
    dist_days = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        # A/D比: 1.05 のような形式を抽出
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け日:\s*(\d+)', meta)
        ad_ratios.append(float(ad.group(1)) if ad else 1.0)
        dist_days.append(int(dist.group(1)) if dist else 0)

    fig_market = make_subplots(specs=[[{"secondary_y": True}]])
    fig_market.add_trace(go.Bar(x=dates, y=ad_ratios, name="A/D比", marker_color='lightblue'), secondary_y=False)
    fig_market.add_trace(go.Scatter(x=dates, y=dist_days, name="売り抜け日", line=dict(color='red', width=3)), secondary_y=True)
    fig_market.update_layout(title_text="📡 市場環境トレンド (A/D比 vs 売り抜け日)", xaxis_title="日付")
    fig_market.update_yaxes(title_text="A/D比 (1.0以上が良好)", secondary_y=False)
    fig_market.update_yaxes(title_text="売り抜け日数 (多いほど危険)", secondary_y=True)

    # 2. 有望株の「タイトネス（収束）」確認チャート
    # 定着率が高く、かつ最新価格がある上位5銘柄
    latest_date = dates[-1]
    leading_stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].sort_values(by=['出現回数', f'売上成長(%)_{latest_date}'], ascending=False).head(5)
    
    fig_tight = go.Figure()
    for _, row in leading_stocks.iterrows():
        prices = []
        for d in dates:
            p = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            prices.append(p)
        
        # 初日の価格で標準化（%変化を表示）
        base_p = next((p for p in prices if pd.notnull(p)), None)
        if base_p:
            norm_prices = [((p / base_p) - 1) * 100 if pd.notnull(p) else None for p in prices]
            fig_tight.add_trace(go.Scatter(x=dates, y=norm_prices, mode='lines+markers', name=row['銘柄'], line=dict(width=2)))

    fig_tight.update_layout(title_text="📉 有望株のボラティリティ収束 (週次%推移)", yaxis_title="価格変化率 (%)", xaxis_title="日付")

    # 3. セクター別「超優秀」銘柄分布 (ヒートマップ)
    # ※CSVに'セクター'列がない場合、'パターン'で代用（要件に合わせて拡張可能）
    sector_col = next((c for c in df.columns if 'セクター' in c or '業種' in c), None)
    
    if sector_col:
        # セクターがある場合
        growth_data = df[df[f'成長性判定_{latest_date}'] == '【超優秀】クリア']
        fig_heat = px.treemap(growth_data, path=[sector_col, '銘柄'], values=f'時価総額(B)_{latest_date}', 
                              color=f'売上成長(%)_{latest_date}', title="💎 セクター別：超優秀銘柄分布 (時価総額サイズ)")
    else:
        # セクターがない場合はパターン別の分布を表示
        growth_data = df[df[f'成長性判定_{latest_date}'] == '【超優秀】クリア']
        fig_heat = px.bar(growth_data, x='銘柄', y=f'売上成長(%)_{latest_date}', color=f'パターン_{latest_date}',
                          title="💎 超優秀銘柄：成長率 vs チャートパターン (セクター列未検出)")

    # HTMLに統合
    report_html = f"<html><head><title>Weekly Analysis Report</title></head><body>"
    report_html += f"<h1>📊 週次投資判断レポート: {datetime.now().strftime('%Y-%m-%d')}</h1>"
    report_html += fig_market.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += fig_tight.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += fig_heat.to_html(full_html=False, include_plotlyjs='cdn')
    report_html += "</body></html>"
    
    return report_html

def upload_to_drive(content, filename):
    service = get_drive_service()
    file_metadata = {'name': filename, 'parents': [SUMMARY_FOLDER_ID], 'mimeType': 'text/html'}
    media = MediaFileUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/html', resumable=True)
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"✅ インタラクティブ・レポートを保存しました: {filename}")

if __name__ == "__main__":
    trend_df, base_name = fetch_latest_summary()
    html_report = create_interactive_report(trend_df)
    
    report_filename = base_name.replace('weekly_detailed_trend', 'interactive_report').replace('.csv', '.html')
    with open("weekly_report.html", "w", encoding="utf-8") as f:
        f.write(html_report)
    
    upload_to_drive(html_report, report_filename)
