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
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, token_uri="https://oauth2.googleapis.com/token")
    return build('drive', 'v3', credentials=creds)

def create_intelligence_report(df):
    # 1. 日付列の特定とソート
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols] # 形式: MM/DD
    
    # JavaScript用に 2026/MM/DD 形式に変換
    formatted_dates = [f"2026/{{d}}" for d in dates]

    # 2. 市場データの抽出
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_data = []
    for d in dates:
        meta = str(market_row.get(f'価格_{{d}}', ""))
        ad = re.search(r'A/D比:\s*([\d\.]+)', meta)
        dist = re.search(r'売り抜け:\s*(\d+)', meta)
        market_data.append({{
            "date": f"2026/{{d}}",
            "display_date": d,
            "status": meta.split('|')[0].strip(),
            "ad": float(ad.group(1)) if ad else 1.0,
            "dist": int(dist.group(1)) if dist else 0
        }})

    # 3. 銘柄データの抽出
    stock_rows = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    stocks_json = []
    for _, row in stock_rows.iterrows():
        prices = {{f"2026/{{d}}": (float(p) if pd.notnull(p := pd.to_numeric(row.get(f'価格_{{d}}'), errors='coerce')) else None) for d in dates}}
        patterns = {{f"2026/{{d}}": str(row.get(f'パターン_{{d}}', "")) for d in dates}}
        growths = {{f"2026/{{d}}": float(pd.to_numeric(row.get(f'売上成長(%)_{{d}}'), errors='coerce') or 0) for d in dates}}

        stocks_json.append({{
            "ticker": row['銘柄'],
            "prices": prices,
            "patterns": patterns,
            "growths": growths
        }})

    full_data_payload = {{
        "dates": formatted_dates,
        "market": market_data,
        "stocks": stocks_json
    }}

    # 4. HTML/JavaScript エンジン
    html_template = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <title>Dynamic Strategy Analytics</title>
        <script src="https://cdn.plot.ly/plotly-2.24.1.min.js"></script>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f4f7f9; margin: 0; padding: 20px; }}
            .container {{ max-width: 1200px; margin: auto; }}
            .control-panel {{ background: #1a2a3a; color: white; padding: 20px; border-radius: 12px; display: flex; align-items: center; gap: 25px; margin-bottom: 25px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); position: sticky; top: 10px; z-index: 1000; }}
            .date-input {{ background: #2c3e50; border: 1px solid #34495e; color: white; padding: 8px; border-radius: 5px; outline: none; }}
            .card {{ background: white; border-radius: 15px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
            .market-summary {{ display: flex; justify-content: space-around; font-size: 1.1em; font-weight: bold; background: #f8f9fa; padding: 15px; border-radius: 10px; }}
            .rank-section {{ margin-top: 40px; }}
            .section-title {{ border-left: 6px solid #3498db; padding-left: 15px; color: #2c3e50; }}
            .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; margin-top: 15px; }}
            .rank-card {{ border: 1px solid #eee; border-radius: 10px; padding: 15px; position: relative; background: #fff; }}
            .rank-badge {{ position: absolute; top: -8px; left: -8px; background: #333; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8em; border: 2px solid #fff; }}
            .persistence-tag {{ float: right; background: #e74c3c; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; font-weight: bold; }}
            .metric-box {{ background: #f1f3f5; padding: 10px; border-radius: 6px; margin: 10px 0; font-size: 0.85em; }}
            .metric-row {{ display: flex; justify-content: space-between; border-bottom: 1px solid #e0e0e0; padding: 3px 0; }}
            .metric-row:last-child {{ border-bottom: none; }}
            .priority-idx {{ color: #7f8c8d; font-size: 0.8em; }}
            .pattern-tag {{ color: #95a5a6; font-size: 0.75em; font-style: italic; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔭 戦略・戦術マルチ解析ダッシュボード</h1>
            
            <div class="control-panel">
                <div>📅 <b>分析開始日の指定:</b> <input type="date" id="start-date-picker" class="date-input" onchange="handleDateChange()"></div>
                <div id="period-info" style="font-size: 0.9em; color: #bdc3c7;"></div>
            </div>

            <div class="card">
                <h2 class="section-title">🌍 市場環境の変遷</h2>
                <div class="market-summary" id="market-stats"></div>
                <div id="chart-market" style="height:350px;"></div>
            </div>

            <div id="dynamic-rankings"></div>

            <div class="card">
                <h2 class="section-title">📊 銘柄収束解析（選択期間）</h2>
                <div id="chart-scatter" style="height:550px;"></div>
            </div>
        </div>

        <script>
            const data = {json.dumps(full_data_payload)};
            
            // 日付文字列(2026/MM/DD)をDateオブジェクトに
            const parseDate = (str) => new Date(str);
            const formatDateInput = (str) => str.replace(/\//g, '-');

            // カレンダーの初期設定
            const datePicker = document.getElementById('start-date-picker');
            const sortedDates = data.dates;
            datePicker.min = formatDateInput(sortedDates[0]);
            datePicker.max = formatDateInput(sortedDates[sortedDates.length - 2]); // 最低2日間必要
            datePicker.value = formatDateInput(sortedDates[Math.max(0, sortedDates.length - 5)]);

            function handleDateChange() {{
                const selected = datePicker.value.replace(/-/g, '/');
                // 選択日以降の日付リストを作成
                const targetDates = sortedDates.filter(d => parseDate(d) >= parseDate(selected));
                updateDashboard(targetDates);
            }}

            function updateDashboard(targetDates) {{
                const periodLen = targetDates.length;
                document.getElementById('period-info').innerText = `期間: ${{periodLen}} 日間 (最終日: ${{targetDates[periodLen-1]}})`;

                // 1. 市場環境の計算
                const mStart = data.market.find(m => m.date === targetDates[0]);
                const mEnd = data.market.find(m => m.date === targetDates[periodLen-1]);
                const adDiff = mEnd.ad - mStart.ad;
                const distDiff = mEnd.dist - mStart.dist;
                
                document.getElementById('market-stats').innerHTML = `
                    <div>現状: ${{mEnd.status}}</div>
                    <div>A/D変化: <span style="color:${{adDiff >= 0 ? 'green':'red'}}">${{adDiff >= 0 ? '+':''}}${{adDiff.toFixed(2)}}</span></div>
                    <div>売り抜け変化: <span style="color:${{distDiff <= 0 ? 'green':'red'}}">${{distDiff >= 0 ? '+':''}}${{distDiff}}日</span></div>
                `;

                // 2. 銘柄解析とソートロジック
                const analyzed = data.stocks.map(s => {{
                    const pList = targetDates.map(d => s.prices[d]).filter(p => p !== null);
                    if (pList.length < 2) return null;
                    
                    const persistence = pList.length;
                    const change = ((pList[pList.length-1] / pList[0]) - 1) * 100;
                    const vol = ((Math.max(...pList) - Math.min(...pList)) / Math.min(...pList)) * 100;
                    
                    // 期間内最新有効値の取得
                    let growth = 0, pattern = "データ不足";
                    for(let i=periodLen-1; i>=0; i--) {{
                        const d = targetDates[i];
                        if (s.growths[d]) growth = s.growths[d];
                        if (s.patterns[d] && !["", "不明", "－"].includes(s.patterns[d])) {{
                            pattern = s.patterns[d]; break;
                        }}
                    }}
                    return {{ ticker: s.ticker, persistence, change, vol, growth, pattern }};
                }}).filter(x => x !== null);

                const getSorter = (keys, orders) => (a, b) => {{
                    for(let i=0; i<keys.length; i++) {{
                        const ord = orders[i];
                        if(a[keys[i]] !== b[keys[i]]) return ord * (a[keys[i]] - b[keys[i]]);
                    }}
                    return 0;
                }};

                const sections = [
                    {{ title: "🏆 総合・サバイバルリーダー", priority: "定着 ➔ 騰落率 ➔ 成長 ➔ 低ボラ", 
                       data: [...analyzed].sort(getSorter(['persistence','change','growth','vol'], [-1,-1,-1,1])).slice(0,5), isTotal: true }},
                    {{ title: "📐 High-Base (Strict) リーダー", priority: "定着 ➔ 低ボラ ➔ 騰落率 ➔ 成長", 
                       data: analyzed.filter(x => x.pattern.includes('Strict')).sort(getSorter(['persistence','vol','change','growth'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "📉 High-Base (Normal) リーダー", priority: "定着 ➔ 低ボラ ➔ 騰落率 ➔ 成長", 
                       data: analyzed.filter(x => x.pattern.includes('High-Base') && !x.pattern.includes('Strict')).sort(getSorter(['persistence','vol','change','growth'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "🌀 VCP・収束リーダー", priority: "定着 ➔ 低ボラ ➔ 成長 ➔ 騰落率", 
                       data: analyzed.filter(x => x.pattern.includes('VCP')).sort(getSorter(['persistence','vol','growth','change'], [-1,1,-1,-1])).slice(0,5) }},
                    {{ title: "⚡ PowerPlay・勢いリーダー", priority: "定着 ➔ 騰落率 ➔ 成長 ➔ 低ボラ", 
                       data: analyzed.filter(x => x.pattern.includes('PowerPlay')).sort(getSorter(['persistence','change','growth','vol'], [-1,-1,-1,1])).slice(0,5) }}
                ];

                // 3. ランキング描画
                let html = "";
                sections.forEach(sec => {{
                    html += `<div class="card"><h2 class="section-title">${{sec.title}}</h2><p class="priority-idx">優先: ${{sec.priority}}</p><div class="rank-grid">`;
                    sec.data.forEach((s, i) => {{
                        html += `
                        <div class="rank-card">
                            ${{sec.isTotal ? `<div class="rank-badge">${{i+1}}</div>` : ""}}
                            <span class="persistence-tag">${{s.persistence}}日出現</span>
                            <h3 style="margin:5px 0;">${{s.ticker}}</h3>
                            <div class="metric-box">
                                <div class="metric-row"><span>騰落率</span> <b>${{s.change.toFixed(1)}}%</b></div>
                                <div class="metric-row"><span>値幅(Vol)</span> <b>${{s.vol.toFixed(1)}}%</b></div>
                                <div class="metric-row"><span>売上成長</span> <b>${{s.growth}}%</b></div>
                            </div>
                            <div class="pattern-tag">${{s.pattern}}</div>
                        </div>`;
                    }});
                    html += "</div></div>";
                }});
                document.getElementById('dynamic-rankings').innerHTML = html;

                // 4. チャート更新
                Plotly.newPlot('chart-market', [
                    {{ x: targetDates, y: targetDates.map(d => data.market.find(m => m.date===d).ad), name: 'A/D比', type: 'scatter', line: {{width:4}} }},
                    {{ x: targetDates, y: targetDates.map(d => data.market.find(m => m.date===d).dist), name: '売り抜け', type: 'bar', opacity: 0.3, yaxis: 'y2' }}
                ], {{ yaxis2: {{overlaying:'y', side:'right'}}, margin: {{t:20, b:40, l:40, r:40}}, template: 'plotly_white' }});

                Plotly.newPlot('chart-scatter', [{{
                    x: analyzed.map(x => x.persistence), y: analyzed.map(x => x.change), text: analyzed.map(x => x.ticker),
                    mode: 'markers+text', textposition: 'top center', marker: {{size:12, color:analyzed.map(x => x.vol), colorscale:'Portland', showscale:true}}
                }}], {{ xaxis:{{title:'出現日数'}}, yaxis:{{title:'騰落率(%)'}}, template: 'plotly_white' }});
            }}

            handleDateChange(); // 初回実行
        </script>
    </body>
    </html>
    """
    return html_template

def upload_to_drive(content, filename):
    service = get_drive_service()
    fh = io.BytesIO(content.encode('utf-8'))
    media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name = '{{filename}}' and trashed = false"
    res = service.files().list(q=query).execute()
    files = res.get('files', [])
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        service.files().create(body={{'name': filename, 'parents': [SUMMARY_FOLDER_ID]}}, media_body=media).execute()

if __name__ == "__main__":
    service = get_drive_service()
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    if not res.get('files'): sys.exit(1)
    
    file_id = res['files'][0]['id']
    csv_name = res['files'][0]['name']
    req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    trend_df = pd.read_csv(fh, dtype=str)
    
    html_report = create_intelligence_report(trend_df)
    report_filename = csv_name.replace('weekly_detailed_trend', 'interactive_intelligence').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
