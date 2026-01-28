import os
import sys
import pandas as pd
import plotly.graph_objects as go
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
    if not files: sys.exit(1)
    req = service.files().get_media(fileId=files[0]['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    # dtype=str で読み込み、後で個別に数値変換することで型エラーを防止
    return pd.read_csv(fh, dtype=str), files[0]['name']

def create_intelligence_report(df):
    # 日付列の抽出
    date_cols = [c for c in df.columns if '価格_' in c]
    dates = sorted([c.split('_')[-1] for c in date_cols])
    latest_date = dates[-1]

    # --- 1. 市場環境：メタデータの時系列解析 ---
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_history = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け:\s*(\d+)', meta)
        low_days = re.search(r'安値から:\s*(\d+)', meta)
        market_history.append({
            'date': d,
            'ad': float(ad.group(1)) if ad else 1.0,
            'dist': int(dist.group(1)) if dist else 0,
            'low_days': int(low_days.group(1)) if low_days else 0,
            'raw': meta
        })

    # --- 2. 有望銘柄の動的スコアリング ---
    # 市場環境行を除外
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    ranked_list = []
    
    for _, row in stocks.iterrows():
        # 価格データの数値化
        prices = []
        for d in dates:
            p_val = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            if pd.notnull(p_val):
                prices.append(p_val)
        
        if len(prices) < 2: continue # 変化を追うため、2日以上出現した銘柄のみ

        # タイトネス判定（直近の値幅が収束しているか）
        volatility = (max(prices) - min(prices)) / min(prices)
        is_tight = volatility < 0.08 # 8%以内をタイトと定義
        
        # 出現回数と成長率の数値化（ここで型エラーを防止）
        persistence = pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0
        growth = pd.to_numeric(row.get(f'売上成長(%)_{latest_date}'), errors='coerce') or 0
        
        # スコアリング（定着率、成長率、収束度を重視）
        score = (float(persistence) * 30.0) + (float(growth) * 0.5)
        if is_tight: score += 50.0 # VCP兆候への強力な加点
        if "超優秀" in str(row.get(f'成長性判定_{latest_date}')): score += 60.0
        
        ranked_list.append({
            'ticker': row['銘柄'],
            'score': score,
            'persistence': int(persistence),
            'is_tight': is_tight,
            'growth': growth,
            'pattern': row.get(f'パターン_{latest_date}', '不明'),
            'price_change': ((prices[-1]/prices[0])-1)*100
        })
    
    top_stocks = sorted(ranked_list, key=lambda x: x['score'], reverse=True)[:5]

    # --- 3. チャート作成 ---
    # チャート1: 市場環境（A/D比と売り抜け日）
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=4, color='dodgerblue')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.3, marker_color='red'), secondary_y=True)
    fig1.update_layout(title="📈 市場の質：A/D比の推移と供給圧力", height=450, template="plotly_white")

    # チャート2: ボラティリティ収束比較
    fig2 = go.Figure()
    for s in top_stocks:
        p_history = []
        for d in dates:
            p_val = pd.to_numeric(stocks[stocks['銘柄']==s['ticker']][f'価格_{d}'].values[0], errors='coerce')
            p_history.append(p_val)
        
        base_p = next((p for p in p_history if pd.notnull(p)), None)
        if base_p:
            norm_p = [((p/base_p)-1)*100 if pd.notnull(p) else None for p in p_history]
            fig2.add_trace(go.Scatter(x=dates, y=norm_p, name=s['ticker'], mode='lines+markers'))
    fig2.update_layout(title="📉 有望株のタイトネス比較（週初比 %）", yaxis_title="変化率 (%)", height=450, template="plotly_white")

    # --- 4. HTMLレポート生成 ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; max-width: 1100px; margin: auto; padding: 20px; background-color: #f8f9fa; }}
            .card {{ background: white; padding: 25px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 25px; }}
            h1 {{ color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #2980b9; border-left: 5px solid #2980b9; padding-left: 15px; }}
            .insight-box {{ background: #eef7fd; border-left: 5px solid #3498db; padding: 20px; margin: 15px 0; border-radius: 0 5px 5px 0; }}
            .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
            .rank-card {{ border: 1px solid #e1e8ed; padding: 15px; border-radius: 8px; position: relative; }}
            .badge {{ background: #2ecc71; color: white; padding: 3px 10px; border-radius: 20px; font-size: 0.8em; }}
            .badge-vcp {{ background: #f1c40f; color: #333; }}
        </style>
    </head>
    <body>
        <h1>📊 週次・深層投資判断レポート</h1>
        
        <div class="card">
            <h2>🌍 市場環境の「変化」に対するGeminiの洞察</h2>
            <div class="insight-box">
                {generate_market_insight(market_history)}
            </div>
            {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
            <p><b>💡 グラフの読み方と洞察:</b> 青いライン（A/D比）が右肩上がりで、赤いバー（売り抜け日）が低い水準を維持しているのが理想です。
               もし売り抜け日が増えているのに、安値からの日数が進んでいる場合は、<b>「機関投資家が上昇を利用して持ち株を処分している」</b>リスクを示唆します。</p>
        </div>

        <div class="card">
            <h2>🏆 注目銘柄ランキング：定着率とVCP重視</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card">
                    <h3>第{i+1}位: {s['ticker']}</h3>
                    <p><span class="badge">定着率: {s['persistence']}/5日</span> 
                    { "<span class='badge badge-vcp'>VCP兆候</span>" if s['is_tight'] else "" }</p>
                    <p><b>成長性:</b> 売上成長 {s['growth']:.1f}%<br>
                    <b>パターン:</b> {s['pattern']}<br>
                    <b>週次推移:</b> 週初比 {s['price_change']:+.2f}%</p>
                    <p style="font-size: 0.9em; color: #666;"><b>【洞察】</b> 5日間のリスト維持は強い支持の証拠です。価格推移がフラットに近いほど、機関投資家の買い集めが完了し、爆発的上昇の準備が整っている可能性を示唆します。</p>
                </div>
                ''' for i, s in enumerate(top_stocks)])}
            </div>
        </div>

        <div class="card">
            <h2>📉 有望株のタイトネス解析チャート</h2>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
            <div class="insight-box">
                <b>💡 ボラティリティ収束の洞察:</b> ミネルヴィニ流の「タイトネス」は、このグラフで線が「水平」に近い銘柄に現れます。
                上昇後に価格が崩れず、狭いレンジで推移している銘柄は、売り圧力が枯渇しており、最小の買いで新高値を抜ける準備ができています。
            </div>
        </div>
    </body>
    </html>
    """
    return report_html

def generate_market_insight(history):
    """市場データの「変化」からGeminiの視点で洞察を生成"""
    start = history[0]
    end = history[-1]
    
    # 売り抜け日数と経過日数の変化
    dist_change = end['dist'] - start['dist']
    days_passed = end['low_days'] - start['low_days']
    
    insight = f"<b>📅 分析期間の推移:</b> 安値から {start['low_days']}日目 → {end['low_days']}日目への遷移<br><br>"
    
    # 洞察ロジック
    if end['ad'] > start['ad'] and dist_change <= 0:
        insight += "🟢 <b>【極めて健全】</b> 市場の広がり（A/D比）が改善し、かつ供給（売り抜け日）が増えていません。上昇トレンドの質が非常に高く、積極的にリスクを取れる局面です。"
    elif dist_change > 0 and days_passed > 0:
        insight += f"🟡 <b>【注意】</b> 安値から日数が進むにつれ、売り抜け日が {end['dist']}日に増加しました。上昇は続いていますが、機関投資家の利益確定売りが入り始めています。"
    elif end['ad'] < start['ad']:
        insight += "🔴 <b>【警戒】</b> 指数の動きに対してA/D比が低下しています。一部の大型株のみが指数を牽引しており、個別株の「買いの質」は低下傾向にあります。銘柄選別をより厳しくすべきです。"
    else:
        insight += "⚪ <b>【中立】</b> 指標に大きな変化はありません。現在のトレンドが維持されていますが、ブレイクアウトの成功率を注視してください。"

    return insight

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
    html_report = create_intelligence_report(trend_df)
    report_filename = csv_name.replace('weekly_detailed_trend', 'investment_intelligence').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
