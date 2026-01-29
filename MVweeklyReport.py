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

def get_latest_non_empty(row, base_col, dates):
    """最新の日付から遡って、空でない有効な値を返す"""
    for d in reversed(dates):
        val = str(row.get(f"{base_col}_{d}", ""))
        if val and val not in ["－", "-", "不明", "nan", "None"]:
            return val
    return "不明"

def ask_gemini_comprehensive_analysis(market_history, top_stocks, universe_stats):
    """Gemini 3 に『市場の全母集団』と『上位銘柄』の比較分析を行わせる"""
    if not GEMINI_API_KEY: return "Gemini API Key Error"
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-3-flash-preview')

    market_text = "\n".join([f"- {m['date']}: {m['raw']}" for m in market_history])
    stocks_text = "\n".join([f"- {s['ticker']}: 定着{s['persistence']}日, 週次騰落:{s['change']:+.1f}%, 成長:{s['growth']}%, パターン:{s['pattern']}" for s in top_stocks])

    prompt = f"""
    あなたはミネルヴィニとエルダー博士の視点を持つプロアナリストです。
    今週スクリーニングを通過した全銘柄の統計データと、その中から厳選された上位銘柄を比較し、市場の『真の姿』を浮き彫りにしてください。

    ### 1. 市場環境
    {market_text}

    ### 2. スクリーニング母集団（全銘柄）の統計
    {universe_stats}

    ### 3. 上位選出銘柄（リーダー群）
    {stocks_text}

    ### 分析と提言の指示（日本語、HTML形式）
    1. 【母集団とリーダーの比較分析】: 全銘柄の平均的な定着率や騰落率に対し、上位銘柄がどのように突出しているか、その「相対的強さ(RS)」の価値を断定してください。
    2. 【需給の質】: 3日以上定着している銘柄数や、共通して現れているパターンの意味（例: High-Baseの多発が意味する強気感）を解説してください。
    3. 【個別銘柄の急所】: 上位5銘柄が「なぜ今、リストの頂点にいるのか」を、全銘柄の中での立ち位置を踏まえて鋭く突いてください。
    4. 【来週の指針】: Gemini 3の高度な推論により、月曜日の寄り付きから取るべき姿勢を具体的に提示してください。
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

    # --- 2. 銘柄解析（全数調査） ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    
    for _, row in stocks.iterrows():
        # 数値データの安全な抽出
        prices = []
        for d in dates:
            p_val = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            if pd.notnull(p_val): prices.append(float(p_val))
        
        if not prices: continue

        # 定着日数、騰落率、最新成長率を算出
        persistence = int(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        weekly_change = ((prices[-1] / prices[0]) - 1) * 100 if prices[0] != 0 else 0
        
        # 成長率とパターンは「最新の有効な値」を取得
        latest_growth = float(pd.to_numeric(get_latest_non_empty(row, "売上成長(%)", dates), errors='coerce') or 0)
        final_pattern = get_latest_non_empty(row, "パターン", dates)
        
        # 定着日数を最優先（100点/日）としたスコアリング
        score = (persistence * 100.0) + (weekly_change * 1.0) + (latest_growth * 0.5)
        
        analysis_list.append({
            'ticker': row['銘柄'], 'score': score, 'persistence': persistence,
            'change': weekly_change, 'growth': latest_growth, 'pattern': final_pattern,
            'prices': prices
        })
    
    # 統計用データフレーム
    analysis_df = pd.DataFrame(analysis_list)
    
    # --- 3. 全銘柄の統計（Geminiへのインプット用） ---
    p_dist = analysis_df['persistence'].value_counts().sort_index().to_dict()
    universe_stats = f"""
    - 総スクリーニング通過銘柄数: {len(analysis_df)}件
    - 定着日数の分布 (日数:件数): {p_dist}
    - 全体の平均週次騰落率: {analysis_df['change'].mean():.2f}%
    - 全体の平均売上成長率: {analysis_df['growth'].mean():.1f}%
    - 主要パターン分布: {analysis_df['pattern'].value_counts().head(5).to_dict()}
    """

    # 上位5銘柄選出
    top_stocks = sorted(analysis_list, key=lambda x: x['score'], reverse=True)[:5]
    gemini_insight = ask_comprehensive_analysis(market_history, top_stocks, universe_stats)

    # --- 4. チャート生成（定着マトリクス） ---
    fig2 = px.scatter(
        analysis_df, x="persistence", y="change", text="ticker",
        size=[10]*len(analysis_df), color="growth",
        labels={"persistence": "定着日数", "change": "週次騰落率(%)"},
        title="📈 全銘柄分析：定着日数 vs パフォーマンス（母集団の分布）"
    )
    fig2.update_traces(textposition='top center')
    fig2.update_layout(height=600, template="plotly_white")

    # --- 5. HTML構築 ---
    report_html = f"""
    <html>
    <head><meta charset='utf-8'><style>
        body {{ font-family: sans-serif; max-width: 1100px; margin: auto; padding: 20px; background: #f8f9fa; }}
        .card {{ background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 25px; }}
        .insight-box {{ border-left: 8px solid #8e44ad; padding: 20px; background: #f3e5f5; font-size: 1.1em; line-height: 1.8; }}
        .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .rank-card {{ border: 2px solid #3498db; border-radius: 10px; padding: 15px; text-align: center; background: #fff; }}
        .badge {{ background: #e74c3c; color: white; padding: 4px 10px; border-radius: 5px; font-weight: bold; font-size: 0.9em; }}
    </style></head>
    <body>
        <h1>📊 週次・全銘柄網羅的深層レポート (Gemini 3)</h1>
        <div class="card">
            <h2>🧠 全母集団の分析に基づく Gemini インサイト</h2>
            <div class="insight-box">{gemini_insight}</div>
        </div>
        <div class="card">
            <h2>🏆 注目銘柄ランキング Top 5</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card">
                    <div class="badge">{s['persistence']}/5日 定着</div>
                    <h3>{s['ticker']}</h3>
                    <p>週次変化: {s['change']:+.1f}%<br>売上成長: {s['growth']}%</p>
                    <small><b>パターン:</b><br>{s['pattern']}</small>
                </div>
                ''' for s in top_stocks])}
            </div>
        </div>
        <div class="card">
            <h2>📊 市場の分布可視化</h2>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>
    </body></html>
    """
    return report_html

# upload_to_drive, main 等は前回と同様（略）
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
    report_filename = res['files'][0]['name'].replace('weekly_detailed_trend', 'intelligence_full').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
