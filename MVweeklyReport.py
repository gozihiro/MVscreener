import os
import sys
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
import re
from datetime import datetime
import google.generativeai as genai
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- 環境変数 ---
CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')
SUMMARY_FOLDER_ID = os.environ.get('SUMMARY_FOLDER_ID')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

def get_drive_service():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def ask_gemini_for_insight(market_history, top_stocks):
    """Gemini APIを使用して、データから人間のような投資洞察を生成する"""
    if not GEMINI_API_KEY:
        return "⚠️ GEMINI_API_KEYが設定されていないため、アルゴリズムによる簡易分析を表示します。"

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')

    market_text = "\n".join([f"- {m['date']}: {m['raw']}" for m in market_history])
    stocks_text = "\n".join([f"- {s['ticker']}: 定着率{s['persistence']}/5日, 最新売上成長{s['growth']:.1f}%, パターン:{s['pattern']}" for s in top_stocks])

    prompt = f"""
    あなたはマーク・ミネルヴィニとアレキサンダー・エルダー博士の視点を持つプロの投資助言AIです。
    以下の1週間分の市場データと、5日間のスクリーニングを生き残った注目銘柄リストを分析してください。

    ### 1週間の市場データ推移
    {market_text}

    ### 週次スクリーニング残留銘柄（重要）
    {stocks_text}

    ### 依頼事項（日本語で回答）
    1. 市場の質の変化: A/D比の変化と、「安値からの日数」に対する「売り抜け日」の蓄積から、現在のマーケットの「真の強さ」を考察してください。
    2. 定着銘柄の評価: 5日間リストに残り続けた銘柄の「定着率」が意味する需給バランスと、ミネルヴィニ流のVCP（収束）の予兆について触れてください。
    3. 来週への戦略的提言: 攻めるべきか、キャッシュを守るべきか、具体的な根拠と共に提示してください。

    ※HTMLタグ（<br>, <b>等）を使って読みやすく構造化して出力してください。
    """
    try:
        response = model.generate_content(prompt)
        return response.text.replace('\n', '<br>')
    except Exception as e:
        return f"Gemini分析エラー: {str(e)}"

def create_intelligence_report(df):
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    latest_date = dates[-1]

    # --- 市場環境解析 ---
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_history = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け:\s*(\d+)', meta)
        low_days = re.search(r'安値から:\s*(\d+)', meta)
        market_history.append({
            'date': d, 'ad': float(ad.group(1)) if ad else 1.0,
            'dist': int(dist.group(1)) if dist else 0,
            'low_days': int(low_days.group(1)) if low_days else 0, 'raw': meta
        })

    # --- 銘柄ランキング解析 ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    ranked_list = []
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if len(prices) < 2: continue
        
        volatility = (max(prices) - min(prices)) / min(prices)
        persistence = pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0
        growth = pd.to_numeric(row.get(f'売上成長(%)_{latest_date}'), errors='coerce') or 0
        
        score = (float(persistence) * 40.0) + (float(growth) * 0.5)
        is_tight = volatility < 0.08
        if is_tight: score += 50.0
        if "超優秀" in str(row.get(f'成長性判定_{latest_date}')): score += 60.0
        
        ranked_list.append({
            'ticker': row['銘柄'], 'score': score, 'persistence': int(persistence),
            'is_tight': is_tight, 'growth': growth, 'pattern': row.get(f'パターン_{latest_date}', '不明'),
            'price_change': ((prices[-1]/prices[0])-1)*100
        })
    
    top_stocks = sorted(ranked_list, key=lambda x: x['score'], reverse=True)[:5]
    gemini_insight = ask_gemini_for_insight(market_history, top_stocks)

    # --- チャート生成 ---
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=4, color='dodgerblue')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.3, marker_color='red'), secondary_y=True)
    fig1.update_layout(title="📈 市場の質：A/D比と供給圧力の相関", height=400, template="plotly_white")

    fig2 = go.Figure()
    for s in top_stocks:
        p_history = [pd.to_numeric(stocks[stocks['銘柄']==s['ticker']][f'価格_{d}'].values[0], errors='coerce') for d in dates]
        base_p = next((p for p in p_history if pd.notnull(p)), None)
        if base_p:
            norm_p = [((p/base_p)-1)*100 if pd.notnull(p) else None for p in p_history]
            fig2.add_trace(go.Scatter(x=dates, y=norm_p, name=s['ticker'], mode='lines+markers'))
    fig2.update_layout(title="📉 注目銘柄のボラティリティ収束推移 (週初比 %)", height=400, template="plotly_white")

    # --- HTML 生成 ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1100px; margin: auto; padding: 20px; background: #f4f7f6; }}
            .card {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 25px; }}
            h1 {{ color: #2c3e50; text-align: center; border-bottom: 4px solid #3498db; }}
            .insight-box {{ background: #e8f4fd; border-left: 6px solid #3498db; padding: 20px; font-size: 1.05em; border-radius: 0 8px 8px 0; }}
            .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; }}
            .rank-card {{ border: 1px solid #ddd; padding: 15px; border-radius: 8px; background: #fff; }}
            .badge {{ background: #27ae60; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <h1>📊 週次・戦略投資判断レポート by Gemini</h1>
        <div class="card">
            <h2>🧠 Gemini による深層インサイト</h2>
            <div class="insight-box">{gemini_insight}</div>
        </div>
        <div class="card">
            <h2>🏆 注目銘柄ランキング Top 5</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card">
                    <h3>{s['ticker']}</h3>
                    <span class="badge">定着率: {s['persistence']}/5日</span> 
                    {"<span class='badge' style='background:#f1c40f; color:black;'>VCP</span>" if s['is_tight'] else ""}
                    <p>売上成長: {s['growth']:.1f}% / パターン: {s['pattern']}<br>週次推移: {s['price_change']:+.2f}%</p>
                </div>
                ''' for i, s in enumerate(top_stocks)])}
            </div>
        </div>
        <div class="card">
            <h2>📈 市場トレンド & 銘柄収束解析</h2>
            {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
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
    report_filename = res['files'][0]['name'].replace('weekly_detailed_trend', 'investment_intelligence').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
