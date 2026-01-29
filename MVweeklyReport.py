import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import io
import re
import math
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

def get_latest_non_empty(row, base_col, dates):
    for d in reversed(dates):
        val = str(row.get(f"{base_col}_{d}", ""))
        if val and val not in ["－", "-", "不明", "nan", "None"]:
            return val
    return "データ不足"

def create_intelligence_report(df):
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    latest_date = dates[-1]

    # --- 1. 市場環境の事実解析 ---
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_history = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け:\s*(\d+)', meta)
        market_history.append({
            'date': d, 
            'ad': float(ad.group(1)) if ad else 1.0,
            'dist': int(dist.group(1)) if dist else 0,
            'raw': meta
        })
    
    # 週末の変化判定
    ad_change = market_history[-1]['ad'] - market_history[0]['ad']
    dist_change = market_history[-1]['dist'] - market_history[0]['dist']
    market_status = market_history[-1]['raw'].split('|')[0]

    # --- 2. 銘柄解析（全数統計） ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if not prices: continue

        persistence = int(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        weekly_change = ((prices[-1] / prices[0]) - 1) * 100 if prices[0] != 0 else 0
        latest_growth = float(pd.to_numeric(get_latest_non_empty(row, "売上成長(%)", dates), errors='coerce') or 0)
        pattern = get_latest_non_empty(row, "パターン", dates)
        
        # 統計的優位性スコア（定着1万点、騰落100点、成長1点）
        score = (persistence * 10000.0) + (weekly_change * 100.0) + (latest_growth * 1.0)
        
        analysis_list.append({
            'ticker': row['銘柄'], 'score': score, 'persistence': persistence,
            'change': weekly_change, 'growth': latest_growth, 'pattern': pattern,
            'judgment': get_latest_non_empty(row, "成長性判定", dates)
        })
    
    analysis_df = pd.DataFrame(analysis_list)
    
    # --- 3. 要件に基づいたランキング選出 ---
    # ① 総合 Top 5
    top_overall = analysis_df.sort_values(by=['score'], ascending=False).head(5)

    # ② パターン別 Top 3 (主要3カテゴリ)
    patterns_to_watch = ["High-Base", "VCP_Original", "PowerPlay"]
    pattern_leaders = {}
    for p in patterns_to_watch:
        pattern_leaders[p] = analysis_df[analysis_df['pattern'].str.contains(p, na=False)].sort_values(by='score', ascending=False).head(3)

    # ③ チャート右上のアウトライヤー (定着率が上位25% かつ 騰落率が上位25%)
    q_pers = analysis_df['persistence'].quantile(0.75)
    q_chng = analysis_df['change'].quantile(0.75)
    outliers = analysis_df[(analysis_df['persistence'] >= q_pers) & (analysis_df['change'] >= q_chng)].sort_values(by='change', ascending=False).head(5)

    # --- 4. チャート作成 ---
    # Chart 1: 市場環境
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=4, color='#3498db')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.3, marker_color='#e74c3c'), secondary_y=True)
    fig1.update_layout(title="📉 Chart 1: 市場の広がりと供給圧力", template="plotly_white", height=400)

    # Chart 2: 定着マトリクス
    fig2 = px.scatter(analysis_df, x="persistence", y="change", text="ticker", color="persistence",
                     labels={"persistence": "定着日数", "change": "週次騰落率(%)"},
                     title="📉 Chart 2: 定着度と強さの相関（母集団分布）")
    fig2.update_traces(textposition='top center')
    fig2.add_hline(y=q_chng, line_dash="dash", line_color="gray", annotation_text="騰落上位25%")
    fig2.update_layout(height=600, template="plotly_white")

    # --- 5. HTML構築 ---
    report_html = f"""
    <html>
    <head><meta charset='utf-8'><style>
        body {{ font-family: sans-serif; max-width: 1200px; margin: auto; padding: 20px; background: #f4f7f9; }}
        .card {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 25px; }}
        h1 {{ color: #2c3e50; text-align: center; border-bottom: 4px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #2980b9; border-left: 6px solid #2980b9; padding-left: 15px; margin-top: 30px; }}
        .fact-box {{ display: flex; justify-content: space-around; background: #2c3e50; color: white; padding: 15px; border-radius: 8px; }}
        .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; }}
        .rank-card {{ border: 1px solid #dee2e6; border-radius: 8px; padding: 12px; background: #fff; position: relative; }}
        .badge {{ background: #e74c3c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .explanation {{ background: #eef7fd; border-left: 5px solid #3498db; padding: 15px; font-size: 0.95em; margin: 10px 0; }}
        .outlier-card {{ border: 2px solid #f1c40f; background: #fff9c4; }}
    </style></head>
    <body>
        <h1>📊 週次：事実に基づく市場・銘柄インテリジェンス</h1>

        <div class="card">
            <h2>🌍 市場環境の事実 (Fact-Check)</h2>
            <div class="fact-box">
                <div>ステータス: <b>{market_status}</b></div>
                <div>週間のA/D変化: <b>{ad_change:+.2f}</b></div>
                <div>週間の売り抜け変化: <b>{dist_change:+.0f}日</b></div>
            </div>
            <div class="explanation">
                <b>💡 エルダー博士の視点：</b><br>
                A/D比（青線）が上昇し、売り抜け日（赤棒）が横ばい・減少しているなら「機関投資家の買い集め」です。
                逆に、反発しているのに売り抜け日が累積している場合、それは「偽りの上昇（ブルトラップ）」を意味します。
            </div>
            {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>

        <div class="card">
            <h2>🏆 総合定着率 Top 5 (Survival Leaders)</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card">
                    <span class="badge">{s['persistence']}/5日 定着</span>
                    <h3>{s['ticker']}</h3>
                    <p>週次: {s['change']:+.1f}% / 成長: {s['growth']}%<br><small>{s['pattern']}</small></p>
                </div>
                ''' for _, s in top_overall.iterrows()])}
            </div>
            <div class="explanation">
                <b>💡 ミネルヴィニの視点：</b><br>
                週を通じてリストに残り続ける銘柄は、価格の揺さぶりに関わらず機関投資家が買い支えている「相対的強さ(RS)」の塊です。
            </div>
        </div>

        <div class="card">
            <h2>🚀 チャート右上のはみ出し銘柄 (Statistical Outliers)</h2>
            <p>※全銘柄の中で「定着率」と「騰落率」が共に上位25%に入っている、最も勢いのある群です。</p>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card outlier-card">
                    <span class="badge">LEADER</span>
                    <h3>{s['ticker']}</h3>
                    <p>週次: {s['change']:+.1f}% / 定着: {s['persistence']}日</p>
                </div>
                ''' for _, s in outliers.iterrows()])}
            </div>
        </div>

        <div class="card">
            <h2>📂 パターン別リーダー (Category Leaders)</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div style="border-right: 1px solid #eee; padding: 10px;">
                    <h4>📍 {p}</h4>
                    {"<br>".join([f"<b>{s['ticker']}</b> ({s['persistence']}日, {s['change']:+.1f}%)" for _, s in leaders.iterrows()])}
                </div>
                ''' for p, leaders in pattern_leaders.items() if not leaders.empty])}
            </div>
        </div>

        <div class="card">
            <h2>📊 定着度とパフォーマンスの相関図</h2>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
            <div class="explanation">
                <b>💡 グラフの読み方：</b><br>
                ・<b>右上の領域：</b> 最も有望。買われ続け、かつ価格も伸びているリーダー株。<br>
                ・<b>右下の領域：</b> 蓄積中。買われ続けているが価格は横ばい。ミネルヴィニ流の「タイトなベース」を形成している可能性。<br>
                ・<b>左上の領域：</b> 一時的。騰落は激しいが定着していない。ニュースによる短期的なノイズの可能性。
            </div>
        </div>
    </body></html>
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
    service = get_drive_service()
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    if not res.get('files'): sys.exit(1)
    
    file_id = res['files'][0]['id']
    req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    trend_df = pd.read_csv(fh, dtype=str)
    
    html_report = create_intelligence_report(trend_df)
    report_filename = res['files'][0]['name'].replace('weekly_detailed_trend', 'fact_intelligence').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
