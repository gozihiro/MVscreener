import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
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

def ask_gemini_advanced_analysis(market_history, top_stocks_data):
    """Gemini 3 に『定着率』と『需給の質』を分析させる"""
    if not GEMINI_API_KEY: return "Gemini API Key Error"
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-3-flash-preview')

    market_text = "\n".join([f"- {m['date']}: {m['raw']}" for m in market_history])
    stocks_text = "\n".join([f"- {s['ticker']}: 定着{s['persistence']}/5日, 週次騰落:{s['change']:.1f}%, 成長:{s['growth']}%" for s in top_stocks_data])

    prompt = f"""
    あなたはミネルヴィニとエルダー博士の視点を持つプロアナリストです。
    週次データに基づき、「定着率（Persistence）」に焦点を当てた深層分析を行ってください。

    ### 1. 市場環境
    {market_text}

    ### 2. 有望銘柄（定着率順）
    {stocks_text}

    ### 指示
    1. 【定着率の意義】: 5日間のうち高頻度で出現した銘柄群について、それが「機関投資家の買い集め」をどう示唆しているか解説してください。
    2. 【マトリクスチャートの解説】: 「出現回数（横軸）」と「騰落率（縦軸）」のチャートから、どの銘柄が『真のリーダー』で、どの銘柄が『一時的なノイズ』か断定してください。
    3. 【週次戦略】: 週末のデータを踏まえ、月曜からの具体的なトレード姿勢を提言してください。

    HTML形式（<h3>, <b>, <br>）で出力してください。
    """
    try:
        response = model.generate_content(prompt)
        return response.text.replace('```html', '').replace('```', '')
    except Exception as e:
        return f"Gemini分析エラー: {str(e)}"

def create_intelligence_report(df):
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    latest_date = dates[-1]

    # --- 1. 市場解析 ---
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_history = [{'date': d, 'raw': str(market_row.get(f'価格_{d}', ""))} for d in dates]

    # --- 2. 銘柄解析（定着率と騰落率の算出） ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if not prices: continue

        persistence = int(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        growth = float(pd.to_numeric(row.get(f'売上成長(%)_{latest_date}'), errors='coerce') or 0)
        weekly_change = ((prices[-1] / prices[0]) - 1) * 100

        # 定着率を最優先したスコアリング
        score = (persistence * 50) + (weekly_change * 0.5) + (growth * 0.2)
        
        analysis_list.append({
            'ticker': row['銘柄'], 'score': score, 'persistence': persistence,
            'change': weekly_change, 'growth': growth, 'pattern': row.get(f'パターン_{latest_date}', '不明')
        })
    
    # 全銘柄データ（チャート2用）と上位5件
    top_stocks = sorted(analysis_list, key=lambda x: x['score'], reverse=True)[:5]
    gemini_insight = ask_gemini_advanced_analysis(market_history, top_stocks)

    # --- 3. チャート生成 ---
    # Chart 1: Market Breadth (従来通り)
    # Chart 2: Persistence vs Performance Matrix (新機軸)
    fig2 = px.scatter(
        pd.DataFrame(analysis_list),
        x="persistence", y="change", text="ticker",
        size=[10]*len(analysis_list), color="growth",
        labels={"persistence": "定着日数 (Days in Screen)", "change": "週次騰落率 (%)"},
        title="📉 Chart 2: Persistence vs Performance (銘柄の『定着度』と『強さ』の相関)"
    )
    fig2.update_traces(textposition='top center')
    fig2.add_hline(y=0, line_dash="dash", line_color="gray")
    fig2.update_layout(height=600, template="plotly_white")

    # --- 4. HTML構築 ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: sans-serif; max-width: 1100px; margin: auto; padding: 20px; background: #f8f9fa; }}
            .card {{ background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 25px; }}
            .rank-box {{ display: flex; flex-wrap: wrap; gap: 15px; margin-top: 15px; }}
            .rank-card {{ flex: 1; min-width: 180px; border: 2px solid #3498db; border-radius: 8px; padding: 15px; text-align: center; }}
            .persistence-badge {{ background: #e74c3c; color: white; padding: 2px 8px; border-radius: 4px; font-weight: bold; }}
            .insight {{ border-left: 6px solid #8e44ad; padding-left: 20px; background: #f3e5f5; padding: 15px; }}
        </style>
    </head>
    <body>
        <h1>🔭 週次：定着率分析と需給インテリジェンス</h1>
        
        <div class="card">
            <h2>🧠 Gemini 3 による深層インサイト</h2>
            <div class="insight">{gemini_insight}</div>
        </div>

        <div class="card">
            <h2>🏆 注目銘柄 Top 5（定着率ランキング）</h2>
            <div class="rank-box">
                {"".join([f'''
                <div class="rank-card">
                    <span class="persistence-badge">{s['persistence']}/5日 定着</span>
                    <h3>{s['ticker']}</h3>
                    <p>週次騰落: {s['change']:+.1f}%<br>売上成長: {s['growth']}%</p>
                    <small>{s['pattern']}</small>
                </div>
                ''' for s in top_stocks])}
            </div>
            <p style="margin-top:20px; font-size:0.9em; color:#666;">
                ※5日間すべてに出現する銘柄は、価格の揺さぶりに関わらず機関投資家が一定の条件下で買い増しを続けている可能性が高い「コア候補」です。
            </p>
        </div>

        <div class="card">
            <h2>📊 視覚的分析：定着度とパフォーマンスの相関</h2>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
            <div style="background:#fff9c4; padding:15px; border-radius:5px; margin-top:10px;">
                <b>💡 チャートの読み方:</b><br>
                ・<b>右上の銘柄:</b> 定着率が高く、価格も強い。今週の真のリーダーです。<br>
                ・<b>右下の銘柄:</b> 定着率は高いが、価格は横ばい。ミネルヴィニ流の「タイトなベース（VCP）」を形成している可能性があり、ブレイクアウト直前かもしれません。<br>
                ・<b>左上の銘柄:</b> 騰落率は高いが、定着率が低い。一時的なニュースによる「飛び乗り」の可能性があり、注意が必要です。
            </div>
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
    res = service.files().list(q=query, orderBy="createdTime desc").execute()
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
