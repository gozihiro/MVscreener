import os
import sys
import pandas as pd
import json
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
    """Google Drive API 認可"""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build('drive', 'v3', credentials=creds)

def create_intelligence_report(df):
    """HTMLレポート生成（JS演算エンジン搭載）"""
    # 1. 日付列の特定 (MM/DD 形式)
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    
    # 2. 市場データの抽出
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_data = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        ad_match = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist_match = re.search(r'売り抜け:\s*(\d+)', meta)
        market_data.append({
            "date": f"2026/{d}",
            "status": meta.split('|')[0].strip() if '|' in meta else "不明",
            "ad": float(ad_match.group(1)) if ad_match else 1.0,
            "dist": int(dist_match.group(1)) if dist_match else 0
        })

    # 3. 銘柄データの抽出
    stock_rows = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    stocks_json = []
    for _, row in stock_rows.iterrows():
        prices = {}
        patterns = {}
        growths = {}
        for d in dates:
            p_val = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            prices[f"2026/{d}"] = float(p_val) if pd.notnull(p_val) else None
            patterns[f"2026/{d}"] = str(row.get(f'パターン_{d}', ""))
            growths[f"2026/{d}"] = float(pd.to_numeric(row.get(f'売上成長(%)_{d}'), errors='coerce') or 0)

        stocks_json.append({
            "ticker": str(row['銘柄']),
            "prices": prices,
            "patterns": patterns,
            "growths": growths
        })

    # データペイロードの作成
    full_data_payload = {
        "dates": [f"2026/{d}" for d in dates],
        "market": market_data,
        "stocks": stocks_json
    }

    # 4. HTML/JS テンプレート (二重波括弧はJS/CSSの保護用、一重はPythonの埋め込み用)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>Dynamic Strategy Intelligence</title>
        <script src="https://cdn.plot.ly/plotly-2.24.1.min.js"></script>
        <style>
            body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #f0f4f8; margin: 0; padding: 20px; color: #2c3e50; }}
            .container {{ max-width: 1200px; margin: auto; }}
            .control-panel {{ background: #1a2a3a; color: white; padding: 25px; border-radius: 15px; display: flex; align-items: center; gap: 30px; margin-bottom: 30px; position: sticky; top: 10px; z-index: 1000; box-shadow: 0 8px 20px rgba(0,0,0,0.15); }}
            .date-input {{ background: #2c3e50; border: 1px solid #455a64; color: white; padding: 10px; border-radius: 8px; font-size: 1em; cursor: pointer; }}
            .card {{ background: white; border-radius: 20px; padding: 30px; margin-bottom: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); border-top: 6px solid #3498db; }}
            .market-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); text-align: center; font-size: 1.2em; font-weight: bold; background: #f8f9fa; padding: 20px; border-radius: 12px; }}
            .section-title {{ font-size: 1.8em; margin: 40px 0 20px 0; border-left: 10px solid #3498db; padding-left: 20px; color: #1a2a3a; }}
            .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; }}
            .rank-card {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 15px; padding: 20px; position: relative; transition: 0.3s; }}
            .rank-card:hover {{ transform: translateY(-5px); box-shadow: 0 8px 16px rgba(0,0,0,0.1); }}
            .rank-badge {{ position: absolute; top: -12px; left: -12px; background: #1a2a3a; color: white; width: 35px; height: 35px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; border: 3px solid #fff; }}
            .persistence-tag {{ float: right; background: #e74c3c; color: white; padding: 4px 10px; border-radius: 6px; font-size: 0.8em; font-weight: bold; }}
            .metric-box {{ background: #f1f3f5; padding: 12px; border-radius: 10px; margin: 15px 0; font-size: 0.9em; }}
            .metric-row {{ display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #dee2e6; }}
            .metric-row:last-child {{ border-bottom: none; }}
            .priority-hint {{ font-size: 0.85em; color: #7f8c8d; font-style: italic; margin-bottom: 15px; }}
            .pattern-tag {{ color: #95a5a6; font-size: 0.8em; font-style: italic; border-top: 1px solid #eee; padding-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 戦略的銘柄解析インテリジェンス</h1>
            
            <div class="control-panel">
                <div>📅 <b>分析開始日を選択:</b> <input type="date" id="start-date-picker" class="date-input" onchange="handleDateChange()"></div>
                <div id="period-info"></div>
            </div>

            <div class="card">
                <h2 style="margin-top:0;">🌍 市場環境の変遷 (Fact-Check)</h2>
                <div class="market-grid" id="market-stats"></div>
                <div id="chart-market" style="height:380px;"></div>
            </div>

            <div id="dynamic-rankings-area"></div>

            <div class="card">
                <h2 style="margin-top:0;">📈 銘柄収束解析（出現日数 vs 騰落率）</h2>
                <div id="chart-scatter" style="height:600px;"></div>
            </div>
            
            <div style="text-align:center; color:#95a5a6; font-size:0.85em; padding: 50px;">
                ※本レポートはDrive上の最新データをブラウザ側でリアルタイムに演算しています。
            </div>
        </div>

        <script>
            const data = {json.dumps(full_data_payload)};
            
            const datePicker = document.getElementById('start-date-picker');
            const sortedDates = data.dates;

            // 初期設定: カレンダーの範囲
            const toInputFormat = (dStr) => dStr.replace(/\//g, '-');
            datePicker.min = toInputFormat(sortedDates[0]);
            datePicker.max = toInputFormat(sortedDates[sortedDates.length - 2]);
            // デフォルトは直近5日前
            datePicker.value = toInputFormat(sortedDates[Math.max(0, sortedDates.length - 5)]);

            function handleDateChange() {{
                const selected = datePicker.value.replace(/-/g, '/');
                const targetDates = sortedDates.filter(d => new Date(d) >= new Date(selected));
                updateDashboard(targetDates);
            }}

            function updateDashboard(targetDates) {{
                const periodLen = targetDates.length;
                const latestDate = targetDates[periodLen - 1];
                document.getElementById('period-info').innerHTML = `期間: <b>${{periodLen}}</b> 日間 <br><small>対象: ${{targetDates[0]}} ～ ${{latestDate}}</small>`;

                // 1. 市場環境
                const mStart = data.market.find(m => m.date === targetDates[0]);
                const mEnd = data.market.find(m => m.date === latestDate);
                const adDiff = mEnd.ad - mStart.ad;
                const distDiff = mEnd.dist - mStart.dist;
                
                document.getElementById('market-stats').innerHTML = `
                    <div>現状<br><span style="color:#2c3e50">${{mEnd.status}}</span></div>
                    <div>A/D変化<br><span style="color:${{adDiff >= 0 ? '#27ae60':'#e74c3c'}}">${{adDiff >= 0 ? '+':''}}${{adDiff.toFixed(2)}}</span></div>
                    <div>売り抜け変化<br><span style="color:${{distDiff <= 0 ? '#27ae60':'#e74c3c'}}">${{distDiff >= 0 ? '+':''}}${{distDiff}}日</span></div>
                `;

                // 2. 銘柄集計
                const analyzed = data.stocks.map(s => {{
                    const pricesInPeriod = targetDates.map(d => s.prices[d]).filter(p => p !== null);
                    if (pricesInPeriod.length < 2) return null;
                    
                    const persistence = pricesInPeriod.length;
                    const change = ((pricesInPeriod[pricesInPeriod.length - 1] / pricesInPeriod[0]) - 1) * 100;
                    const vol = ((Math.max(...pricesInPeriod) - Math.min(...pricesInPeriod)) / Math.min(...pricesInPeriod)) * 100;
                    
                    let growth = 0, pattern = "データ不足";
                    for(let i = periodLen - 1; i >= 0; i--) {{
                        const d = targetDates[i];
                        if (s.growths[d]) growth = s.growths[d];
                        if (s.patterns[d] && !["", "不明", "－"].includes(s.patterns[d])) {{
                            pattern = s.patterns[d];
                            break;
                        }}
                    }}
                    return {{ ticker: s.ticker, persistence, change, vol, growth, pattern }};
                }}).filter(x => x !== null);

                // 多段階ソート用ヘルパー
                const getSorter = (keys, orders) => (a, b) => {{
                    for(let i=0; i<keys.length; i++) {{
                        const k = keys[i], ord = orders[i];
                        if(a[k] !== b[k]) return ord * (a[k] - b[k]);
                    }}
                    return 0;
                }};

                const sections = [
                    {{ title: "🏆 総合・サバイバルリーダー", hint: "定着 ➔ 騰落率 ➔ 成長 ➔ 低ボラ", 
                       data: [...analyzed].sort(getSorter(['persistence','change','growth','vol'], [-1,-1,-1,1])).slice(0,5), isTotal: true }},
                    {{ title: "📐 High-Base (Strict) リーダー", hint: "定着 ➔ 低ボラ ➔ 騰落率 ➔ 成長", 
                       data: analyzed.filter(x => x.pattern.includes('Strict')).sort(getSorter(['persistence','vol','change','growth'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "📉 High-Base (Normal) リーダー", hint: "定着 ➔ 低ボラ ➔ 騰落率 ➔ 成長", 
                       data: analyzed.filter(x => x.pattern.includes('High-Base') && !x.pattern.includes('Strict')).sort(getSorter(['persistence','vol','change','growth'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "🌀 VCP・収束リーダー", hint: "定着 ➔ 低ボラ ➔ 成長 ➔ 騰落率", 
                       data: analyzed.filter(x => x.pattern.includes('VCP')).sort(getSorter(['persistence','vol','growth','change'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "⚡ PowerPlay・勢いリーダー", hint: "定着 ➔ 騰落率 ➔ 成長 ➔ 低ボラ", 
                       data: analyzed.filter(x => x.pattern.includes('PowerPlay')).sort(getSorter(['persistence','change','growth','vol'], [-1,-1,-1,1])).slice(0,5) }}
                ];

                // 3. レンダリング
                let html = "";
                sections.forEach(sec => {{
                    html += `<div class="card"><h2 class="section-title">${{sec.title}}</h2><p class="priority-hint">優先順位: ${{sec.hint}}</p><div class="rank-grid">`;
                    if (sec.data.length === 0) html += "<p style='color:#999; padding:10px;'>対象なし</p>";
                    sec.data.forEach((s, idx) => {{
                        html += `
                        <div class="rank-card">
                            ${{sec.isTotal ? `<div class="rank-badge">${{idx+1}}</div>` : ''}}
                            <span class="persistence-tag">${{s.persistence}}日出現</span>
                            <h3 style="margin:5px 0;">${{s.ticker}}</h3>
                            <div class="metric-box">
                                <div class="metric-row"><span>期間騰落</span> <b style="color:${{s.change >=0 ? '#e74c3c':'#2980b9'}}">${{s.change.toFixed(1)}}%</b></div>
                                <div class="metric-row"><span>値幅(Vol)</span> <b>${{s.vol.toFixed(1)}}%</b></div>
                                <div class="metric-row"><span>売上成長</span> <b>${{s.growth}}%</b></div>
                            </div>
                            <div class="pattern-tag">${{s.pattern}}</div>
                        </div>`;
                    }});
                    html += "</div></div>";
                }});
                document.getElementById('dynamic-rankings-area').innerHTML = html;

                // 4. チャート更新
                Plotly.newPlot('chart-market', [
                    {{ x: targetDates, y: targetDates.map(d => data.market.find(m => m.date===d).ad), name: 'A/D比', type: 'scatter', line: {{width:4, color:'#3498db'}} }},
                    {{ x: targetDates, y: targetDates.map(d => data.market.find(m => m.date===d).dist), name: '売り抜け', type: 'bar', opacity: 0.3, marker: {{color:'#e74c3c'}}, yaxis: 'y2' }}
                ], {{ 
                    yaxis: {{title: 'A/D比'}}, yaxis2: {{overlaying:'y', side:'right', title: '売り抜け日'}},
                    margin: {{t:20, b:40, l:50, r:50}}, template: 'plotly_white' 
                }});

                Plotly.newPlot('chart-scatter', [{{
                    x: analyzed.map(x => x.persistence), y: analyzed.map(x => x.change), text: analyzed.map(x => x.ticker),
                    mode: 'markers+text', textposition: 'top center',
                    marker: {{ size: 14, color: analyzed.map(x => x.vol), colorscale: 'Viridis', showscale: true, colorbar: {{title: 'Vol(%)'}} }}
                }}], {{ 
                    xaxis: {{title: '出現日数'}}, yaxis: {{title: '期間騰落率(%)'}},
                    margin: {{t:20, b:40, l:50, r:50}}, template: 'plotly_white' 
                }});
            }}

            // 初回実行
            handleDateChange();
        </script>
    </body>
    </html>
    """
    return html_content

def upload_to_drive(content, filename):
    """Google Driveへのアップロード（上書き対応）"""
    service = get_drive_service()
    fh = io.BytesIO(content.encode('utf-8'))
    media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
    
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{filename}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])
    
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
        print(f"Updated: {filename}")
    else:
        file_metadata = {'name': filename, 'parents': [SUMMARY_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"Created: {filename}")

if __name__ == "__main__":
    service = get_drive_service()
    
    # 最新の週次トレンドCSVを検索
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    if not res.get('files'):
        print("CSV file not found.")
        sys.exit(1)
    
    file_id = res['files'][0]['id']
    csv_name = res['files'][0]['name']
    
    # ダウンロード
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    
    fh.seek(0)
    trend_df = pd.read_csv(fh, dtype=str)
    
    # 動的レポート生成
    html_report = create_intelligence_report(trend_df)
    
    # アップロード
    report_filename = csv_name.replace('weekly_detailed_trend', 'interactive_ranking').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
