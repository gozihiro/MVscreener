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
    return pd.read_csv(fh), files[0]['name']

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
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    ranked_list = []
    for _, row in stocks.iterrows():
        # 5日間の価格をリスト化
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        
        if len(prices) < 2: continue # 1日しか出ていないものは除外（変化を追うため）

        # 変化の指標
        volatility = (max(prices) - min(prices)) / min(prices) if prices else 1.0
        is_tight = volatility < 0.07 # 7%以内
        is_improving = prices[-1] >= prices[0] # 初日より価格が維持または上昇
        
        # スコア計算
        persistence = row.get('出現回数', 0)
        growth = pd.to_numeric(row.get(f'売上成長(%)_{latest_date}'), errors='coerce') or 0
        
        score = (persistence * 25) + (growth * 0.3)
        if is_tight: score += 40
        if "超優秀" in str(row.get(f'成長性判定_{latest_date}')): score += 50
        
        ranked_list.append({
            'ticker': row['銘柄'],
            'score': score,
            'persistence': persistence,
            'is_tight': is_tight,
            'growth': growth,
            'pattern': row.get(f'パターン_{latest_date}', '不明'),
            'price_change': ((prices[-1]/prices[0])-1)*100 if prices else 0
        })
    
    top_stocks = sorted(ranked_list, key=lambda x: x['score'], reverse=True)[:5]

    # --- 3. チャート作成 (複数描画) ---
    # チャート1: 市場環境トレンド
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=4, color='dodgerblue')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.3, marker_color='red'), secondary_y=True)
    fig1.update_layout(title="📈 市場環境：A/D比と供給（売り抜け日）の相関", height=400)

    # チャート2: 有望銘柄の「タイトネス」比較
    fig2 = go.Figure()
    for s in top_stocks:
        p_history = [pd.to_numeric(stocks[stocks['銘柄']==s['ticker']][f'価格_{d}'].values[0], errors='coerce') for d in dates]
        base_p = next((p for p in p_history if pd.notnull(p)), None)
        if base_p:
            norm_p = [((p/base_p)-1)*100 if pd.notnull(p) else None for p in p_history]
            fig2.add_trace(go.Scatter(x=dates, y=norm_p, name=s['ticker'], mode='lines+markers'))
    fig2.update_layout(title="📉 選抜5銘柄の相対価格推移（VCP収束の確認）", yaxis_title="変化率 (%)", height=400)

    # --- 4. 生成されるレポート (HTML) ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.7; color: #333; max-width: 1000px; margin: auto; padding: 40px; background: #f4f7f6; }}
            .card {{ background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 30px; }}
            h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
            .status-box {{ display: flex; justify-content: space-around; background: #2c3e50; color: white; padding: 20px; border-radius: 8px; }}
            .insight {{ background: #e8f4fd; border-left: 5px solid #3498db; padding: 15px; font-style: italic; }}
            .rank-item {{ border-bottom: 1px solid #eee; padding: 15px 0; }}
            .badge {{ background: #27ae60; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <h1>📊 週次：市場環境と銘柄の深層分析</h1>
        
        <div class="card">
            <h2>🌍 市場環境の変遷とGeminiの洞察</h2>
            <div class="status-box">
                <div>最新A/D比: <b>{market_history[-1]['ad']:.2f}</b></div>
                <div>売り抜け日: <b>{market_history[-1]['dist']}日</b></div>
                <div>安値から: <b>{market_history[-1]['low_days']}日目</b></div>
            </div>
            <p></p>
            <div class="insight">
                <b>🔍 市場の「質の変化」への洞察:</b><br>
                {self_generate_insight(market_history)}
            </div>
        </div>

        <div class="card">
            <h2>🏆 注目銘柄ランキング（定着率・収束重視）</h2>
            <p>※1日のスナップショットではなく、週を通じてリストに残り続け、かつ値動きがタイト（VCP）な銘柄を上位に選出しています。</p>
            {"".join([f'''
            <div class="rank-item">
                <b>{i+1}. {s['ticker']}</b> <span class="badge">定着率: {s['persistence']}/5日</span> 
                { " <span class='badge' style='background:#f1c40f; color:black;'>VCP兆候</span>" if s['is_tight'] else "" }
                <ul>
                    <li><b>テクニカル洞察:</b> 5日間の値幅変動が {abs(s['price_change']):.1f}% 以内に抑えられており、{s['pattern']} の中で機関投資家の「静かな買い」が推測されます。</li>
                    <li><b>ファンダメンタルズ:</b> 最新の売上成長率は {s['growth']:.1f}%。リストへの高い定着率は、一時的なニュースではなくトレンドとしての強さを示唆しています。</li>
                </ul>
            </div>
            ''' for i, s in enumerate(top_stocks)])}
        </div>

        <div class="card">
            <h2>📊 チャート解説と視覚的分析</h2>
            {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
            <div class="insight">
                <b>💡 グラフ1の読み方:</b> A/D比（青線）が上昇し、売り抜け日（赤棒）が減少している状態が最強の買いシグナルです。
                逆に「安値からの日数」が増えているのにA/D比が低下している場合は、上昇のエネルギーが枯渇している「チャーニング（空回り）」を警戒してください。
            </div>
            <br>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
            <div class="insight">
                <b>💡 グラフ2の読み方:</b> 各銘柄の価格推移を週初を0%として比較しています。線が水平に近く、かつ細かく上下している銘柄は、ミネルヴィニ氏の言う「タイトネス」が形成されており、次のブレイクアウトの準備が整っている可能性が高いです。
            </div>
        </div>
    </body>
    </html>
    """
    return report_html

def self_generate_insight(history):
    """市場データの変化から洞察を生成するロジック"""
    start = history[0]
    end = history[-1]
    
    insight = ""
    # A/D比の変化
    if end['ad'] > start['ad']:
        insight += f"・A/D比が {start['ad']:.2f} から {end['ad']:.2f} へ改善。市場の広がりが強まっており、買いの質が向上しています。<br>"
    else:
        insight += f"・A/D比が低下傾向にあります。指数の上昇に対して個別銘柄の追随が弱まっており、選別色を強める必要があります。<br>"

    # 売り抜けと安値からの日数
    if end['dist'] > start['dist']:
        insight += f"・安値から {end['low_days']} 日が経過しましたが、売り抜け日が {end['dist']} 日に増加。上昇の初期段階としては供給（売り）がやや強すぎます。<br>"
    elif end['low_days'] > start['low_days'] and end['dist'] == start['dist']:
        insight += f"・安値から {end['low_days']} 日目。売り抜け日が増えていないことは、上昇トレンドが機関投資家にサポートされている健全な証拠です。<br>"
    
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
