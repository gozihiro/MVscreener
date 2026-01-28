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
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    files = res.get('files', [])
    if not files:
        sys.exit(1)
    req = service.files().get_media(fileId=files[0]['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh), files[0]['name']

def create_rich_report(df):
    dates = sorted(list(set([c.split('_')[-1] for c in df.columns if '価格_' in c and '/' in c])))
    latest_date = dates[-1]
    
    # --- 1. 市場環境分析 (Elder's View) ---
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    ad_list, dist_list = [], []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け日:\s*(\d+)', meta)
        ad_list.append(float(ad.group(1)) if ad else 1.0)
        dist_list.append(int(dist.group(1)) if dist else 0)
    
    current_ad = ad_list[-1]
    ad_trend = "改善" if len(ad_list) > 1 and ad_list[-1] > ad_list[-2] else "停滞"
    market_status = "【警戒】" if dist_list[-1] >= 4 else "【健全】" if current_ad > 1.1 else "【中立】"

    # --- 2. 注目銘柄ランキング選出 (Minervini's View) ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    for d in dates: # 数値化
        stocks[f'価格_{d}'] = pd.to_numeric(stocks[f'価格_{d}'], errors='coerce')
        stocks[f'売上成長(%)_{d}'] = pd.to_numeric(stocks[f'売上成長(%)_{d}'], errors='coerce').fillna(0)

    # ランキングロジック: 出現頻度 × 成長率 × タイトネス
    ranked_list = []
    for _, row in stocks.iterrows():
        prices = [row[f'価格_{d}'] for d in dates if pd.notnull(row[f'価格_{d}'])]
        if len(prices) < 3: continue
        
        volatility = (max(prices) - min(prices)) / min(prices)
        is_tight = volatility < 0.08 # 8%以内をタイトと定義
        is_super = "超優秀" in str(row[f'成長性判定_{latest_date}'])
        
        score = (row['出現回数'] * 20) + (row[f'売上成長(%)_{latest_date}'] * 0.5)
        if is_tight: score += 30
        if is_super: score += 50
        
        ranked_list.append({
            'ticker': row['銘柄'],
            'score': score,
            'is_tight': is_tight,
            'is_super': is_super,
            'growth': row[f'売上成長(%)_{latest_date}'],
            'pattern': row[f'パターン_{latest_date}'],
            'count': row['出現回_数'] if '出現回_数' in row else row.get('出現回数', 0)
        })
    
    top_stocks = sorted(ranked_list, key=lambda x: x['score'], reverse=True)[:5]

    # --- HTML & Plotly 生成 ---
    # (チャート作成部分は前回同様、ただし配置を調整)
    fig_market = make_subplots(specs=[[{"secondary_y": True}]])
    fig_market.add_trace(go.Bar(x=dates, y=ad_list, name="A/D比"), secondary_y=False)
    fig_market.add_trace(go.Scatter(x=dates, y=dist_list, name="売り抜け日", line=dict(color='red')), secondary_y=True)
    
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: sans-serif; margin: 40px; line-height: 1.6; color: #333; }}
            .section {{ background: #f9f9f9; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 5px solid #2c3e50; }}
            .highlight {{ color: #e74c3c; font-weight: bold; }}
            .ticker-card {{ background: white; border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            .badge {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 12px; margin-right: 5px; color: white; }}
            .badge-super {{ background: #f1c40f; color: black; }}
            .badge-tight {{ background: #2ecc71; }}
        </style>
    </head>
    <body>
        <h1>📊 週次戦略レポート: {datetime.now().strftime('%Y-%m-%d')}</h1>
        
        <div class="section">
            <h2>🌍 市場環境の洞察 (Alexander Elder's View)</h2>
            <p>現在の市場ステータス: <span class="highlight">{market_status}</span></p>
            <ul>
                <li><strong>市場の広がり (A/D比):</strong> 現在 {current_ad:.2f}。傾向は <b>{ad_trend}</b> です。1.05を超えて維持されている場合、上昇の質は健全です。</li>
                <li><strong>機関投資家の動き:</strong> 売り抜け日は現在 {dist_list[-1]} 日。5日を超えると天井圏のサインですが、現在は{'許容範囲内' if dist_list[-1] < 5 else '警戒レベル'}です。</li>
                <li><strong>総評:</strong> {market_row.get(f'価格_{latest_date}', 'データなし')}。この数値に基づくと、現在は「{'積極的に買いを検討すべき' if market_status == '【健全】' else 'キャッシュ比率を高めるべき'}」局面です。</li>
            </ul>
        </div>

        <div class="section">
            <h2>🏆 注目銘柄ランキング Top 5 (Minervini's Focus)</h2>
            {"".join([f'''
            <div class="ticker-card">
                <b>第{i+1}位: {s['ticker']}</b> 
                {"<span class='badge badge-super'>超優秀</span>" if s['is_super'] else ""}
                {"<span class='badge badge-tight'>VCP兆候</span>" if s['is_tight'] else ""}
                <br>
                <ul>
                    <li><b>根拠:</b> 出現頻度 {s['count']}/5日。売上成長率 {s['growth']:.1f}%。</li>
                    <li><b>テクニカル:</b> {s['pattern']}。{'価格が収束しており、ブレイクアウト目前のタイトネスが見られます。' if s['is_tight'] else 'ボラティリティはまだ高めですが、強いトレンドの中にあります。'}</li>
                </ul>
            </div>
            ''' for i, s in enumerate(top_stocks)])}
        </div>

        <div class="section">
            <h2>📈 視覚的分析 (チャート)</h2>
            {fig_market.to_html(full_html=False, include_plotlyjs='cdn')}
            <p><i>※A/D比が伸びながら売り抜け日が横ばい、または減少している状態が理想的な上昇トレンドです。</i></p>
        </div>
    </body>
    </html>
    """
    return report_html

def upload_to_drive(content, filename):
    service = get_drive_service()
    fh = io.BytesIO(content.encode('utf-8'))
    media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{filename}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        service.files().create(body={'name': filename, 'parents': [SUMMARY_FOLDER_ID]}, media_body=media).execute()

if __name__ == "__main__":
    trend_df, csv_name = fetch_latest_summary()
    html_report = create_rich_report(trend_df)
    report_filename = csv_name.replace('weekly_detailed_trend', 'investment_intelligence').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
