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

# --- 環境変数設定 ---
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')
SUMMARY_FOLDER_ID = os.environ.get('SUMMARY_FOLDER_ID')

def get_drive_service():
    """Google Drive API 認可"""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def get_latest_non_empty(row, base_col, dates):
    """最新日付から遡って有効な文字列（パターンや判定）を取得する"""
    for d in reversed(dates):
        val = str(row.get(f"{base_col}_{d}", ""))
        if val and val not in ["－", "-", "不明", "nan", "None", ""]:
            return val
    return "データ不足"

def render_enhanced_grid(target_df, is_total=False):
    """ランキング銘柄をカード形式のHTMLにする補助関数（エビデンス数値付き）"""
    if target_df.empty:
        return "<p style='color:#999; padding:20px;'>該当銘柄なし（条件を満たすデータが不足しています）</p>"
    
    cards = []
    for i, (_, s) in enumerate(target_df.iterrows()):
        # 総合ランキングの場合は順位バッジを表示
        rank_badge = f'<div class="rank-number-badge">{i+1}</div>' if is_total else ""
        
        cards.append(f'''
        <div class="rank-card">
            {rank_badge}
            <div style="text-align:right;">
                <span class="persistence-label">{s['persistence']}日定着</span>
            </div>
            <h3 style="margin:5px 0; color:#2c3e50; font-size:1.4em;">{s['ticker']}</h3>
            <div class="metric-container">
                <div class="metric-row"><span>週次騰落率</span> <b>{s['change']:+.1f}%</b></div>
                <div class="metric-row"><span>値幅(Vol)</span> <b>{s['vol']:.1f}%</b></div>
                <div class="metric-row"><span>売上成長</span> <b>{s['growth']}%</b></div>
            </div>
            <div class="pattern-tag">{s['pattern']}</div>
        </div>
        ''')
    return f'<div class="rank-grid">{"".join(cards)}</div>'

def create_intelligence_report(df):
    """メインレポート生成ロジック"""
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
    
    ad_change = market_history[-1]['ad'] - market_history[0]['ad']
    dist_change = market_history[-1]['dist'] - market_history[0]['dist']
    market_status = market_history[-1]['raw'].split('|')[0]

    # --- 2. 銘柄データの数値化と正規化 ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if not prices: continue

        persistence = int(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        weekly_change = ((prices[-1] / prices[0]) - 1) * 100 if prices[0] != 0 else 0
        vol = ((max(prices) - min(prices)) / min(prices) * 100) if min(prices) > 0 else 99.9
        growth = float(pd.to_numeric(get_latest_non_empty(row, "売上成長(%)", dates), errors='coerce') or 0)
        pattern = get_latest_non_empty(row, "パターン", dates)

        analysis_list.append({
            'ticker': row['銘柄'], 'persistence': persistence, 'change': weekly_change, 
            'growth': growth, 'vol': vol, 'pattern': pattern
        })
    
    all_df = pd.DataFrame(analysis_list)

    # --- 3. パターン別・多段階ソート実行 (Top 5) ---
    # 総合: 定着 > 騰落 > 成長 > ボラ低
    top_overall = all_df.sort_values(['persistence', 'change', 'growth', 'vol'], ascending=[False, False, False, True]).head(5)
    
    # HB Strict: 定着 > ボラ低 > 騰落 > 成長
    hb_strict = all_df[all_df['pattern'].str.contains(r'Strict', na=False)].sort_values(
        ['persistence', 'vol', 'change', 'growth'], ascending=[False, True, False, False]).head(5)
    
    # HB Normal: 定着 > ボラ低 > 騰落 > 成長
    hb_normal = all_df[all_df['pattern'].str.contains('High-Base', na=False) & ~all_df['pattern'].str.contains('Strict', na=False)].sort_values(
        ['persistence', 'vol', 'change', 'growth'], ascending=[False, True, False, False]).head(5)
    
    # VCP: 定着 > ボラ低 > 成長 > 騰落
    vcp_top = all_df[all_df['pattern'].str.contains('VCP', na=False)].sort_values(
        ['persistence', 'vol', 'growth', 'change'], ascending=[False, True, False, False]).head(5)
    
    # PowerPlay: 定着 > 騰落高 > 成長 > ボラ低
    power_top = all_df[all_df['pattern'].str.contains('PowerPlay', na=False)].sort_values(
        ['persistence', 'change', 'growth', 'vol'], ascending=[False, False, False, True]).head(5)

    # --- 4. チャート作成 ---
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=4, color='#3498db')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.3, marker_color='#e74c3c'), secondary_y=True)
    fig1.update_layout(title="📈 市場環境：A/D比と売り抜けカウントの推移", template="plotly_white", height=400)

    fig2 = px.scatter(all_df, x="persistence", y="change", text="ticker", color="persistence",
                     labels={"persistence": "定着日数", "change": "週次騰落率(%)"},
                     title="📉 銘柄収束解析：定着日数 vs パフォーマンス（母集団分布）")
    fig2.update_traces(textposition='top center')
    fig2.update_layout(height=500, template="plotly_white")

    # --- 5. HTML構築 ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; max-width: 1200px; margin: auto; padding: 30px; background: #f4f7f9; color: #333; }}
            .card {{ background: white; padding: 30px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.08); margin-bottom: 35px; border-top: 5px solid #2980b9; }}
            h1 {{ text-align: center; color: #2c3e50; font-size: 2.2em; margin-bottom: 30px; }}
            .section-header {{ border-left: 8px solid #2980b9; padding-left: 15px; margin-top: 40px; color: #2c3e50; }}
            .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 15px; margin-top: 15px; }}
            .rank-card {{ border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; background: #fff; position: relative; transition: 0.3s; }}
            .rank-card:hover {{ transform: translateY(-5px); box-shadow: 0 6px 12px rgba(0,0,0,0.1); }}
            .rank-number-badge {{ position: absolute; top: -10px; left: -10px; background: #2c3e50; color: white; width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; border: 2px solid white; }}
            .persistence-label {{ background: #e74c3c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75em; font-weight: bold; }}
            .metric-container {{ font-size: 0.85em; margin: 10px 0; background: #f8f9fa; padding: 10px; border-radius: 6px; }}
            .metric-row {{ display: flex; justify-content: space-between; padding: 2px 0; border-bottom: 1px solid #eee; }}
            .metric-row:last-child {{ border-bottom: none; }}
            .pattern-tag {{ font-size: 0.75em; color: #7f8c8d; margin-top: 8px; font-style: italic; }}
            .explanation-box {{ background: #eef7fd; border-left: 5px solid #3498db; padding: 15px; margin-bottom: 15px; font-size: 0.95em; line-height: 1.6; }}
            .market-summary {{ display: flex; gap: 20px; background: #2c3e50; color: white; padding: 15px; border-radius: 8px; justify-content: center; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>📊 週次：戦略的銘柄解析レポート</h1>

        <div class="card">
            <h2 class="section-header">🌍 市場環境の事実解析 (Fact-Check)</h2>
            <div class="market-summary">
                <span>現状: {market_status}</span>
                <span>A/D変化: {ad_change:+.2f}</span>
                <span>売り抜け変化: {dist_change:+.0f}日</span>
            </div>
            <div class="explanation-box" style="margin-top:15px;">
                <b>📈 チャートの見方（エルダー博士の視点）:</b><br>
                青い実線（A/D比）が右肩上がり、赤い棒グラフ（売り抜け日）が横ばいまたは減少していれば、市場の「中身」は健康です。
                反発局面であっても売り抜け日数が6日を超えている場合は、機関投資家の上値売りに注意が必要です。
            </div>
            {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>

        <div class="card">
            <h2 class="section-header">🏆 総合・サバイバルリーダー (Total Leaders Top 5)</h2>
            <div class="explanation-box">
                <b>🔍 選出ロジック:</b><br>
                市場の混乱に負けず一貫してリストに残る<b>定着日数</b>を最優先し、同数の場合は<b>週次騰落率</b>で相対的な強さ(RS)を、次いで<b>成長性</b>、最後に<b>値幅のタイトさ</b>を評価して1位〜5位を決定しています。
            </div>
            {render_enhanced_grid(top_overall, is_total=True)}
        </div>

        <div class="card">
            <h2 class="section-header">📐 High-Base (Strict) リーダー Top 5</h2>
            <div class="explanation-box">
                <b>🔍 選出ロジック:</b><br>
                スクリーニング時点で厳格な値をパスした銘柄。定着が並んだ場合は、ブレイクアウト直前のエネルギー凝縮を示す<b>値幅（Vol）の低さ（タイトネス）</b>を最優先しています。
            </div>
            {render_enhanced_grid(hb_strict)}
        </div>

        <div class="card">
            <h2 class="section-header">📉 High-Base (Normal) リーダー Top 5</h2>
            <div class="explanation-box">
                <b>🔍 選出ロジック:</b><br>
                通常のHigh-Base銘柄。Strict同様に、定着後の<b>ボラティリティの収束度</b>を重視したランキングです。
            </div>
            {render_enhanced_grid(hb_normal)}
        </div>

        <div class="card">
            <h2 class="section-header">🌀 VCP・ボラティリティ収束リーダー Top 5</h2>
            <div class="explanation-box">
                <b>🔍 選出ロジック:</b><br>
                ミネルヴィニのVCP。定着が同じなら<b>値幅の低さ</b>と、質の裏付けである<b>売上成長率</b>を優先して、振るい落としが完了に近い順に並べています。
            </div>
            {render_enhanced_grid(vcp_top)}
        </div>

        <div class="card">
            <h2 class="section-header">⚡ PowerPlay・勢い重視リーダー Top 5</h2>
            <div class="explanation-box">
                <b>🔍 選出ロジック:</b><br>
                短期の暴騰銘柄。このカテゴリのみ、定着が並んだ場合は<b>週次騰落率（勢い）</b>を最優先評価としています。
            </div>
            {render_enhanced_grid(power_top)}
        </div>

        <div class="card">
            <h2 class="section-header">📊 銘柄収束解析：視覚的分布</h2>
            <div class="explanation-box">
                <b>📈 グラフの読み方（ミネルヴィニの視点）:</b><br>
                ・<b>右上の領域：</b> 最も有望。買われ続け、かつ価格も伸びているリーダー株。<br>
                ・<b>右下の領域：</b> 蓄積中。買われ続けているが価格は横ばい。理想的な「タイトなベース」を形成中。<br>
                ・<b>中央の水平ライン：</b> 全銘柄の平均騰落率。このラインより上の銘柄がRS（相対的強さ）を持っています。
            </div>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>

        <div style="text-align:center; color:#95a5a6; font-size:0.8em; padding-bottom:30px;">
            ※本レポートはDrive上の最新CSVデータに基づき、数学的ソートロジックを用いて自動生成されています。
        </div>
    </body>
    </html>
    """
    return report_html

def upload_to_drive(content, filename):
    """HTMLレポートをGoogle Driveへ保存"""
    service = get_drive_service()
    fh = io.BytesIO(content.encode('utf-8'))
    media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
    
    # 既存ファイルのチェック
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{filename}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])
    
    if files:
        # 上書き更新
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"Update completed: {filename}")
    else:
        # 新規作成
        file_metadata = {'name': filename, 'parents': [SUMMARY_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"Create completed: {filename}")

if __name__ == "__main__":
    service = get_drive_service()
    
    # 最新の weekly_detailed_trend CSVを検索
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    
    if not res.get('files'):
        print("CSV file not found.")
        sys.exit(1)
    
    file_id = res['files'][0]['id']
    csv_name = res['files'][0]['name']
    print(f"Analyzing: {csv_name}")
    
    # ダウンロード
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    
    fh.seek(0)
    trend_df = pd.read_csv(fh, dtype=str)
    
    # レポート生成
    html_report = create_intelligence_report(trend_df)
    
    # アップロード
    report_filename = csv_name.replace('weekly_detailed_trend', 'strategic_ranking').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
