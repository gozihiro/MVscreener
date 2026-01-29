import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import io
import re
import math
from datetime import datetime
from google import genai
from google.genai import types
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
    """最新から遡って有効な値を返す。すべて無効なら'データ不足'を返す"""
    for d in reversed(dates):
        val = str(row.get(f"{base_col}_{d}", ""))
        if val and val not in ["－", "-", "不明", "nan", "None"]:
            return val
    return "データ不足"

def ask_gemini_comprehensive_analysis(market_history, top_stocks, universe_stats):
    """制限の緩い gemini-1.5-flash を使用してクォータエラーを回避"""
    if not GEMINI_API_KEY: return "Gemini API Key Error"
    
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        # 無料枠で最も安定しているモデルを指定
        model_id = "gemini-1.5-flash" 

        market_text = "\n".join([f"- {m['date']}: {m['raw']}" for m in market_history])
        stocks_text = "\n".join([f"- {s['ticker']}: 定着{s['persistence']}日, 週次騰落:{s['change']:+.1f}%, 成長:{s['growth']}, パターン:{s['pattern']}" for s in top_stocks])

        prompt = f"""
        あなたはプロのアナリストです。全銘柄の統計と上位銘柄を比較し、市場の深層を分析してください。

        ### 市場環境
        {market_text}

        ### 母集団の統計
        {universe_stats}

        ### 上位リーダー群
        {stocks_text}

        ### 指示
        1. 【母集団分析】: 全体の中で上位5銘柄がいかに特異で強いか(RS)を評価。
        2. 【需給】: 定着日数が3日以上の銘柄の希少性と意味を解説。
        3. 【戦略】: 月曜からの具体的なトレード姿勢。
        日本語、HTML形式（<h3>, <b>, <br>）で出力。
        """
        
        response = client.models.generate_content(model=model_id, contents=prompt)
        return response.text.replace('```html', '').replace('```', '')
    except Exception as e:
        return f"Gemini分析エラー (クォータ制限等の可能性): {str(e)}"

def create_intelligence_report(df):
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    latest_date = dates[-1]

    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_history = [{'date': d, 'raw': str(market_row.get(f'価格_{d}', ""))} for d in dates]

    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    
    for _, row in stocks.iterrows():
        # 価格の取得と数値化
        prices = []
        for d in dates:
            p_val = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            if pd.notnull(p_val): prices.append(float(p_val))
        
        if not prices: continue

        # --- 数値の安全な算出 ---
        persistence = float(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        
        # 週次騰落率 (NaNなら0)
        weekly_change = 0.0
        if len(prices) >= 2:
            weekly_change = ((prices[-1] / prices[0]) - 1) * 100
        if math.isnan(weekly_change): weekly_change = 0.0

        # 成長率 (NaNなら0)
        growth_str = get_latest_non_empty(row, "売上成長(%)", dates)
        growth_val = float(pd.to_numeric(growth_str, errors='coerce') or 0.0)
        
        # --- 【重要】ソート不全の解決策：圧倒的な重み付け ---
        # 1. 定着日数が最優先（1万倍）
        # 2. 騰落率が次点（100倍）
        # 3. 成長率は補助（1倍）
        # すべてを float で計算し、NaNを排除
        score = (persistence * 10000.0) + (weekly_change * 100.0) + (growth_val * 1.0)
        
        analysis_list.append({
            'ticker': row['銘柄'], 
            'score': score, 
            'persistence': int(persistence),
            'change': weekly_change, 
            'growth': growth_str if growth_str != "データ不足" else "N/A", 
            'pattern': get_latest_non_empty(row, "パターン", dates),
            'prices': prices
        })
    
    analysis_df = pd.DataFrame(analysis_list)
    
    # 統計情報の生成
    p_dist = analysis_df['persistence'].value_counts().sort_index().to_dict()
    universe_stats = f"通過数: {len(analysis_df)}, 定着分布: {p_dist}, 平均騰落: {analysis_df['change'].mean():.2f}%"

    # --- ソートの実行 ---
    # スコアで降順、スコアが同じなら騰落率で降順
    analysis_df = analysis_df.sort_values(by=['score', 'change'], ascending=False)
    top_stocks = analysis_df.head(5).to_dict('records')
    
    gemini_insight = ask_gemini_comprehensive_analysis(market_history, top_stocks, universe_stats)

    # チャート生成
    fig2 = px.scatter(
        analysis_df, x="persistence", y="change", text="ticker",
        color="persistence",
        labels={"persistence": "定着日数", "change": "週次騰落率(%)"},
        title="📈 全銘柄：定着日数 vs パフォーマンス"
    )
    fig2.update_traces(textposition='top center')
    fig2.update_layout(height=600, template="plotly_white")

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
        <h1>📊 週次・戦略深層レポート</h1>
        <div class="card">
            <h2>🧠 Gemini 分析インサイト</h2>
            <div class="insight-box">{gemini_insight}</div>
        </div>
        <div class="card">
            <h2>🏆 注目銘柄ランキング Top 5</h2>
            <div class="rank-grid">
                {"".join([f'''
                <div class="rank-card">
                    <div class="badge">{s['persistence']}/5日 定着</div>
                    <h3>{s['ticker']}</h3>
                    <p>週次騰落: {s['change']:+.1f}%<br>売上成長: {s['growth']}%</p>
                    <small><b>パターン:</b><br>{s['pattern']}</small>
                </div>
                ''' for s in top_stocks])}
            </div>
        </div>
        <div class="card">
            <h2>📊 市場の分布</h2>
            {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
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
    report_filename = res['files'][0]['name'].replace('weekly_detailed_trend', 'intelligence_v2.1').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
