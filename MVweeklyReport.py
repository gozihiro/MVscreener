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

def ask_gemini_advanced_analysis(market_history, top_stocks_data):
    """Gemini 3 にデータから深層心理と戦略を読み解かせる"""
    if not GEMINI_API_KEY:
        return {"market": "Key Error", "stocks": "Key Error"}

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-3-flash-preview')

    # 市場データの要約
    market_summary = "\n".join([f"- {m['date']}: {m['raw']}" for m in market_history])
    
    # 銘柄データの要約 (値幅収束度などを付加)
    stocks_summary = ""
    for s in top_stocks_data:
        stocks_summary += f"""
        - 銘柄: {s['ticker']}
          定着率: {s['persistence']}/5日
          最新成長率: {s['growth']}%
          5日間の値幅(Volatility): {s['vol']:.2f}% (低いほどタイト)
          チャートパターン: {s['pattern']}
        """

    prompt = f"""
    あなたはマーク・ミネルヴィニとアレキサンダー・エルダー博士の投資哲学をマスターしたシニア・ストラテジストです。
    以下の1週間のマーケット推移と、厳選されたトップ5銘柄のデータを元に、表面的な数値を超えた「深層分析」を行ってください。

    ### 1. 市場環境データ
    {market_summary}

    ### 2. 有望銘柄データ
    {stocks_summary}

    ### 指示事項 (日本語、HTML形式で出力)
    1. 【市場チャートの深層解析】: 
       A/D比の変化と売り抜け日の蓄積をどう見るべきか。「安値からの日数」と絡めて、現在の市場が『機関投資家の買い集め』なのか『逃げの局面』なのか、チャートの読み方と共に断定してください。
    2. 【注目銘柄Top5の個別解説】:
       5銘柄それぞれについて、なぜこの順位なのか、定着率とタイトネス(VCP)から「買いの急所(ピボットポイント)」がどこにあるかをミネルヴィニ流に解説してください。
    3. 【チャートから得られる洞察】:
       提示された2つのチャート（市場トレンド・銘柄収束）を投資家はどう解釈し、来週の月曜日にどのような姿勢でマーケットに臨むべきか、具体的かつ厳しく提言してください。

    ※読みやすさのため、<h3>, <b>, <br> を多用し、鋭い表現でお願いします。
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

    # --- 1. 市場環境：変化のパース ---
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

    # --- 2. 有望銘柄：収束度と質の計算 ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    ranked_candidates = []
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if len(prices) < 3: continue # 3日以上出現しているものを評価対象に
        
        # ボラティリティ（タイトネス）の算出
        vol = ((max(prices) - min(prices)) / min(prices)) * 100
        persistence = pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0
        growth = pd.to_numeric(row.get(f'売上成長(%)_{latest_date}'), errors='coerce') or 0
        
        # スコアリング（2026年最新基準: 収束度を最優先）
        score = (float(persistence) * 30) + (float(growth) * 0.4)
        if vol < 6.0: score += 100.0 # 5日間で6%以内の値動きは極めてタイト
        elif vol < 10.0: score += 50.0
        
        ranked_candidates.append({
            'ticker': row['銘柄'], 'score': score, 'persistence': int(persistence),
            'vol': vol, 'growth': growth, 'pattern': row.get(f'パターン_{latest_date}', '不明'),
            'prices': prices
        })
    
    top_stocks = sorted(ranked_candidates, key=lambda x: x['score'], reverse=True)[:5]
    
    # --- 3. Gemini に深層分析を依頼 ---
    deep_insight = ask_gemini_advanced_analysis(market_history, top_stocks)

    # --- 4. チャート生成 (Plotly) ---
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Scatter(x=dates, y=[m['ad'] for m in market_history], name="A/D比", line=dict(width=5, color='#3498db')), secondary_y=False)
    fig1.add_trace(go.Bar(x=dates, y=[m['dist'] for m in market_history], name="売り抜け日", opacity=0.4, marker_color='#e74c3c'), secondary_y=True)
    fig1.update_layout(title="📈 Chart 1: Market Breadth & Distribution Trend", height=450, template="plotly_white")

    fig2 = go.Figure()
    for s in top_stocks:
        base_p = s['prices'][0]
        norm_p = [((p/base_p)-1)*100 for p in s['prices']]
        fig2.add_trace(go.Scatter(x=dates[-len(norm_p):], y=norm_p, name=s['ticker'], mode='lines+markers', line=dict(width=3)))
    fig2.update_layout(title="📉 Chart 2: Top 5 Relative Tightness (VCP Check)", yaxis_title="Relative Change (%)", height=450, template="plotly_white")

    # --- 5. HTML レポート構築 ---
    report_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; line-height: 1.7; color: #333; max-width: 1200px; margin: auto; padding: 30px; background: #f0f2f5; }}
            .card {{ background: white; padding: 35px; border-radius: 15px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); margin-bottom: 30px; border-top: 6px solid #2c3e50; }}
            .gemini-section {{ background: #ffffff; border-left: 8px solid #8e44ad; padding: 25px; border-radius: 0 10px 10px 0; }}
            h1 {{ text-align: center; color: #2c3e50; font-size: 2.5em; }}
            h2 {{ color: #2980b9; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
            h3 {{ color: #8e44ad; margin-top: 30px; }}
            .chart-wrapper {{ padding: 10px; background: #fff; border: 1px solid #eee; border-radius: 10px; }}
        </style>
    </head>
    <body>
        <h1>📊 週次・深層戦略インテリジェンス (2026)</h1>
        
        <div class="card">
            <h2>🧠 Gemini 3: 戦略的深層分析レポート</h2>
            <div class="gemini-section">
                {deep_insight}
            </div>
        </div>

        <div class="card">
            <h2>📈 視覚的データ・エビデンス</h2>
            <div class="chart-wrapper">
                {fig1.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
            <br>
            <div class="chart-wrapper">
                {fig2.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
            <p style="color:#666; font-size:0.9em; text-align:center;">
                ※Chart 1: A/D比の上昇と売り抜け日の減少が一致すれば「全力買い」、逆なら「退避」を意味します。<br>
                ※Chart 2: 0%ライン付近で線が密集・水平化している銘柄こそが、ミネルヴィニ流のVCP（ボラティリティ収束）です。
            </p>
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
