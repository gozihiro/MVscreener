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
ACCUMULATION_FOLDER_ID = os.environ.get('ACCUMULATION_FOLDER_ID')

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

def get_accumulation_ranking(service):
    """Accumulationフォルダ内のCSVを解析しランキングデータを生成"""
    states = []
    page_token = None
    query = f"'{ACCUMULATION_FOLDER_ID}' in parents and trashed = false"
    
    while True:
        results = service.files().list(q=query, fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
        for f in results.get('files', []):
            if f['name'].startswith('['):
                try:
                    parts = f['name'].split('_')
                    persistence = int(parts[0][1:3])
                    ticker = parts[1]
                    
                    request = service.files().get_media(fileId=f['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done: _, done = downloader.next_chunk()
                    fh.seek(0)
                    df = pd.read_csv(fh)
                    
                    if len(df) < 10: continue
                    
                    df['is_up'] = (df['Close'] > df['Open']) & (df['Close'] > df['Close'].shift(1))
                    consistency = (df['is_up'].tail(10).sum() / 10) * 100
                    
                    high_50 = df['High'].max()
                    last_close = df['Close'].iloc[-1]
                    proximity = (last_close / high_50) * 100
                    
                    df['range'] = df['High'] - df['Low']
                    avg_range = df['range'].tail(10).mean()
                    tightness = (1 - (df['range'].iloc[-1] / avg_range)) * 100 if avg_range > 0 else 0
                    
                    ema13 = df['Close'].ewm(span=13, adjust=False).mean()
                    macd = df['Close'].ewm(span=12, adjust=False).mean() - df['Close'].ewm(span=26, adjust=False).mean()
                    impulse = 1 if (ema13.iloc[-1] > ema13.iloc[-2] and macd.iloc[-1] > macd.iloc[-2]) else 0
                    
                    score = (consistency * 0.3) + (proximity * 0.4) + (max(0, tightness) * 0.2) + (impulse * 10)
                    
                    states.append({
                        "ticker": ticker,
                        "persistence": persistence,
                        "score": round(score, 1),
                        "consistency": round(consistency, 0),
                        "proximity": round(proximity, 1),
                        "tightness": "タイト" if tightness > 20 else "通常",
                        "impulse": "Blue" if impulse == 1 else "Neutral"
                    })
                except: continue
        page_token = results.get('nextPageToken')
        if not page_token: break
        
    return states

def create_intelligence_report(df, acc_data=[]):
    """HTMLレポート生成（領域分割・個別優先順位設定版）"""
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    
    market_row = df[df['銘柄'] == '### MARKET_ENVIRONMENT ###'].iloc[0]
    market_data = []
    for d in dates:
        meta = str(market_row.get(f'価格_{d}', ""))
        if "REPORT_METADATA" in meta:
            ad_match = re.search(r'A/D比:\s*([\d\.]+)', meta)
            dist_match = re.search(r'売り抜け:\s*(\d+)', meta)
            market_data.append({
                "date": f"2026/{d}",
                "status": meta.split('|')[0].replace("REPORT_METADATA,", "").strip() if '|' in meta else "不明",
                "ad": float(ad_match.group(1)) if ad_match else 1.0,
                "dist": int(dist_match.group(1)) if dist_match else 0,
                "valid": True
            })
        else:
            market_data.append({"date": f"2026/{d}", "status": "データ収集中", "ad": 1.0, "dist": 0, "valid": False})

    stock_rows = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    stocks_json = []
    for _, row in stock_rows.iterrows():
        prices, patterns, growths, launchpads = {}, {}, {}, {}
        ema10s, sma20s, sma50s = {}, {}, {}
        for d in dates:
            p_val = pd.to_numeric(row.get(f'価格_{d}'), errors='coerce')
            prices[f"2026/{d}"] = float(p_val) if pd.notnull(p_val) else None
            patterns[f"2026/{d}"] = str(row.get(f'パターン_{d}', ""))
            growths[f"2026/{d}"] = float(pd.to_numeric(row.get(f'売上成長(%)_{d}'), errors='coerce') or 0)
            lp_val = pd.to_numeric(row.get(f'発射台スコア_{d}'), errors='coerce')
            launchpads[f"2026/{d}"] = float(lp_val) if pd.notnull(lp_val) else 0
            e10_val = pd.to_numeric(row.get(f'10EMA_{d}'), errors='coerce')
            ema10s[f"2026/{d}"] = float(e10_val) if pd.notnull(e10_val) else None
            s20_val = pd.to_numeric(row.get(f'20SMA_{d}'), errors='coerce')
            sma20s[f"2026/{d}"] = float(s20_val) if pd.notnull(s20_val) else None
            s50_val = pd.to_numeric(row.get(f'50SMA_{d}'), errors='coerce')
            sma50s[f"2026/{d}"] = float(s50_val) if pd.notnull(s50_val) else None

        stocks_json.append({
            "ticker": str(row['銘柄']),
            "prices": prices,
            "patterns": patterns,
            "growths": growths,
            "launchpads": launchpads,
            "ema10s": ema10s,
            "sma20s": sma20s,
            "sma50s": sma50s
        })

    full_data_payload = {
        "dates": [f"2026/{d}" for d in dates],
        "market": market_data,
        "stocks": stocks_json,
        "accumulation": acc_data
    }

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
            .rank-card {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 15px; padding: 20px; position: relative; transition: 0.3s; overflow: hidden; }}
            .rank-card:hover {{ transform: translateY(-5px); box-shadow: 0 8px 16px rgba(0,0,0,0.1); }}
            .rank-badge {{ position: absolute; top: -12px; left: -12px; background: #1a2a3a; color: white; width: 35px; height: 35px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; border: 3px solid #fff; }}
            .persistence-tag {{ float: right; background: #e74c3c; color: white; padding: 4px 10px; border-radius: 6px; font-size: 0.8em; font-weight: bold; }}
            .quality-badges {{ margin-top: 8px; display: flex; gap: 5px; flex-wrap: wrap; }}
            .q-badge {{ padding: 2px 6px; border-radius: 4px; font-size: 0.75em; font-weight: bold; text-transform: uppercase; }}
            .q-trend {{ background: #e8f5e9; color: #2e7d32; border: 1px solid #c8e6c9; }}
            .q-strict {{ background: #fff3e0; color: #ef6c00; border: 1px solid #ffe0b2; }}
            .metric-box {{ background: #f1f3f5; padding: 12px; border-radius: 10px; margin: 15px 0; font-size: 0.9em; }}
            .metric-row {{ display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #dee2e6; }}
            .metric-row:last-child {{ border-bottom: none; }}
            .priority-hint {{ font-size: 0.85em; color: #7f8c8d; font-style: italic; margin-bottom: 15px; }}
            .pattern-tag {{ color: #95a5a6; font-size: 0.8em; font-style: italic; border-top: 1px solid #eee; padding-top: 10px; }}
            .explanation-box {{ background: #eef7fd; border-left: 5px solid #3498db; padding: 15px; margin-top: 15px; font-size: 0.9em; line-height: 1.6; }}
            .score-highlight {{ color: #f39c12; font-weight: bold; }}
            .tier-header {{ background: #2c3e50; color: white; padding: 10px 20px; border-radius: 10px; margin-bottom: 15px; display: inline-block; }}
            .acc-table {{ width: 100%; border-collapse: collapse; font-size: 0.85em; margin-bottom: 10px; background: white; }}
            .acc-table th, .acc-table td {{ border-bottom: 1px solid #eee; padding: 8px 10px; text-align: left; }}
            .acc-table th {{ color: #7f8c8d; font-weight: normal; background: #fafafa; border-top: 1px solid #eee; }}
            .acc-table .ticker-name {{ font-weight: bold; color: #1a2a3a; font-size: 1.1em; }}
            .acc-table .score-val {{ font-weight: bold; color: #f39c12; }}
            .acc-table tr:hover {{ background: #fcfcfc; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 戦略的銘柄解析インテリジェンス V3</h1>
            <div class="control-panel">
                <div>📅 <b>分析開始日を選択:</b> <input type="date" id="start-date-picker" class="date-input" onchange="handleDateChange()"></div>
                <div id="period-info"></div>
            </div>
            <div class="card">
                <h2 style="margin-top:0;">🌍 市場環境の変遷 (Fact-Check)</h2>
                <div class="market-grid" id="market-stats"></div>
                <div id="chart-market" style="height:380px;"></div>
                <div class="explanation-box">
                    <b>📈 需給診断のポイント:</b><br>
                    ・<b>A/D比（青線）：</b> 市場全体の「健康度」。上昇は個別株への広範な買いを、下落は一部銘柄への資金集中または全体的な投げ売りを意味します。<br>
                    ・<b>売り抜け日（赤棒）：</b> 指数の下落と出来高増が重なった「機関投資家の出口戦略」の痕跡。6〜7日を超えると「下落警戒」となります。
                </div>
            </div>
            <div id="accumulation-ranking-area"></div>
            <div id="dynamic-rankings-area"></div>
            <div class="card">
                <h2 style="margin-top:0;">📈 銘柄収束解析（出現日数 vs 騰落率）</h2>
                <div id="chart-scatter" style="height:600px;"></div>
                <div class="explanation-box">
                    <b>📈 プロットの解釈（ミネルヴィニ・エルダー視点）:</b><br>
                    ・<b>右側（高定着）：</b> 機関投資家の強力なサポートがあり、スクリーニングに残り続けている「本命」です。<br>
                    ・<b>右上（リーダー）：</b> 地合いが悪い中でも新高値を追うRS（相対的強さ）の塊。次の強気相場を牽引する主役候補です。<br>
                    ・<b>右下（タイト）：</b> 定着しているが値動きが静かな銘柄。ミネルヴィニ流のVCPやHigh-Baseの完成間近である可能性が高く、ボラティリティが「死んだ」後に爆発的なブレイクアウトが期待できます。<br>
                    ・<b>ドットの色（Score）：</b> 赤・オレンジが濃いほど「発射台スコア」が高く、直近のセットアップが完了に近いことを示します。
                </div>
            </div>
            <div style="text-align:center; color:#95a5a6; font-size:0.85em; padding: 50px;">
                ※本レポートはDrive上の最新データをブラウザ側でリアルタイムに演算しています。
            </div>
        </div>
        <script>
            const data = {json.dumps(full_data_payload)};
            const datePicker = document.getElementById('start-date-picker');
            const sortedDates = data.dates;

            datePicker.min = sortedDates[0].replace(/\//g, '-');
            datePicker.max = sortedDates[sortedDates.length - 1].replace(/\//g, '-');
            datePicker.value = sortedDates[Math.max(0, sortedDates.length - 5)].replace(/\//g, '-');

            function handleDateChange() {{
                const selected = datePicker.value.replace(/-/g, '/');
                const targetDates = sortedDates.filter(d => new Date(d) >= new Date(selected));
                updateDashboard(targetDates);
            }}

            function updateDashboard(targetDates) {{
                const periodLen = targetDates.length;
                const latestDate = targetDates[periodLen - 1];
                document.getElementById('period-info').innerHTML = `期間: <b>${{periodLen}}</b> 日間 <br><small>対象: ${{targetDates[0]}} ～ ${{latestDate}}</small>`;

                const mEnd = data.market.find(m => m.date === latestDate);
                document.getElementById('market-stats').innerHTML = `
                    <div>現状<br><span>${{mEnd.status}}</span></div>
                    <div>A/D比<br><span>${{mEnd.ad.toFixed(2)}}</span></div>
                    <div>売り抜け日<br><span>${{mEnd.dist}}日</span></div>
                `;

                // Accumulation Ranking (Unchanged)
                let accHtml = '<div class="card"><h2 style="margin-top:0;">💎 Accumulation Survival Ranking</h2>';
                const accTiers = [
                    {{ label: "🔥 熟成 (10日以上)", filter: d => d.persistence >= 10 }},
                    {{ label: "✅ 確立 (5〜9日)", filter: d => d.persistence >= 5 && d.persistence <= 9 }},
                    {{ label: "🌱 出現 (1〜4日)", filter: d => d.persistence <= 4 }}
                ];
                accTiers.forEach(tier => {{
                    const tierData = data.accumulation.filter(tier.filter).sort((a,b) => b.score - a.score);
                    if(tierData.length > 0) {{
                        accHtml += `<div class="tier-header">${{tier.label}}</div><table class="acc-table"><thead><tr><th>#</th><th>銘柄</th><th>出現</th><th>総合Score</th><th>続伸率</th><th>新高値比</th><th>VCP</th><th>Impulse</th></tr></thead><tbody>`;
                        tierData.forEach((s, idx) => {{
                            accHtml += `<tr><td>${{idx+1}}</td><td class="ticker-name">${{s.ticker}}</td><td>${{s.persistence}}日</td><td class="score-val">${{s.score}}</td><td>${{s.consistency}}%</td><td>${{s.proximity}}%</td><td>${{s.tightness}}</td><td style="color:${{s.impulse === 'Blue' ? '#3498db' : '#95a5a6'}}">${{s.impulse}}</td></tr>`;
                        }});
                        accHtml += '</tbody></table><div style="margin-bottom:20px;"></div>';
                    }}
                }});
                document.getElementById('accumulation-ranking-area').innerHTML = accHtml + '</div>';

                const analyzed = data.stocks.map(s => {{
                    const pricesInPeriod = targetDates.map(d => s.prices[d]).filter(p => p !== null);
                    if (pricesInPeriod.length < 1) return null;
                    const persistence = pricesInPeriod.length;
                    const change = pricesInPeriod.length >= 2 ? ((pricesInPeriod[persistence - 1] / pricesInPeriod[0]) - 1) * 100 : 0;
                    const vol = pricesInPeriod.length >= 2 ? ((Math.max(...pricesInPeriod) - Math.min(...pricesInPeriod)) / Math.min(...pricesInPeriod)) * 100 : 0;
                    const latestLaunchpad = s.launchpads[latestDate] || 0;

                    // Stealth Accumulation Logic (Fully Restored)
                    let stealthScore = 0;
                    if (pricesInPeriod.length >= 6) {{
                        const last6 = pricesInPeriod.slice(-6);
                        let upCount = 0, isTight = true;
                        for (let i = 1; i < 6; i++) {{
                            const dailyRet = (last6[i] / last6[i-1]) - 1;
                            if (dailyRet > 0) upCount++;
                            if (Math.abs(dailyRet) > 0.035) isTight = false;
                        }}
                        if (upCount >= 4 && isTight) stealthScore = upCount;
                    }}

                    // Momentum Stealth Logic (Fully Restored)
                    let momentumStealthScore = 0;
                    if (pricesInPeriod.length >= 11) {{
                        const last10 = pricesInPeriod.slice(-11);
                        let upCount = 0, isTight = true;
                        for (let i = 1; i < 11; i++) {{
                            const dailyRet = (last10[i] / last10[i-1]) - 1;
                            if (dailyRet > 0) upCount++;
                            if (Math.abs(dailyRet) > 0.077) isTight = false;
                        }}
                        const m10 = s.ema10s[latestDate], m20 = s.sma20s[latestDate], m50 = s.sma50s[latestDate];
                        const isPerfectOrder = (m10 && m20 && m50) ? (m10 > m20 && m20 > m50) : true;
                        if (upCount >= 7 && isTight && isPerfectOrder) momentumStealthScore = upCount;
                    }}

                    const latestPat = s.patterns[latestDate] || "";
                    const isTrendOk = latestPat.includes('[Trend_OK]');
                    const isStrictVcp = latestPat.includes('Strict') || latestPat.includes('Validated');

                    let growth = 0, pattern = "－", launchpad = 0;
                    for(let i = periodLen - 1; i >= 0; i--) {{
                        const d = targetDates[i];
                        if (growth === 0 && s.growths[d]) growth = s.growths[d];
                        if (s.launchpads[d] > launchpad) launchpad = s.launchpads[d];
                        if (pattern === "－" && s.patterns[d] && s.patterns[d] !== "－") pattern = s.patterns[d];
                    }}
                    return {{ ticker: s.ticker, persistence, change, vol, growth, pattern, launchpad, latestLaunchpad, stealthScore, momentumStealthScore, isTrendOk, isStrictVcp }};
                }}).filter(x => x !== null);

                const getSorter = (keys, orders) => (a, b) => {{
                    for(let i=0; i<keys.length; i++) {{
                        const k = keys[i], ord = orders[i];
                        if(a[k] !== b[k]) return ord * (a[k] - b[k]);
                    }}
                    return 0;
                }};

                const sections = [
                    {{ title: "🏆 Super Performance (全条件合格)", hint: "優先順位: 最新スコア ➔ 定着日数", 
                        data: analyzed.filter(x => x.isTrendOk && x.isStrictVcp).sort(getSorter(['latestLaunchpad','persistence'], [-1,-1])).slice(0,10) }},
                    
                    {{ title: "🚀 Ready to Launch (即応銘柄) 総合 TOP 5", hint: "優先順位: 最新発射台 ➔ 定着 ➔ 成長率", 
                        data: analyzed.filter(x => x.latestLaunchpad > 0).sort(getSorter(['latestLaunchpad','persistence','growth'], [-1,-1,-1])).slice(0,5) }},

                    // VCPグループ
                    {{ title: "🚀 VCP [Quality Validated]", hint: "品質タグ合格銘柄 (すべて表示) | 順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('VCP') && (x.isTrendOk || x.isStrictVcp)).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])) }},
                    {{ title: "🚀 VCP [Category TOP 5]", hint: "品質不問 | 優先順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('VCP')).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])).slice(0,5) }},

                    // PowerPlayグループ
                    {{ title: "⚡ PowerPlay [Trend Validated]", hint: "品質タグ合格銘柄 (すべて表示) | 順位: 定着日数 ➔ 期間騰落率", 
                        data: analyzed.filter(x => x.pattern.includes('PowerPlay') && (x.isTrendOk || x.isStrictVcp)).sort(getSorter(['persistence','change'], [-1,-1])) }},
                    {{ title: "⚡ PowerPlay [Category TOP 5]", hint: "品質不問 | 優先順位: 期間騰落率 ➔ 定着日数", 
                        data: analyzed.filter(x => x.pattern.includes('PowerPlay')).sort(getSorter(['change','persistence'], [-1,-1])).slice(0,5) }},

                    // High-Baseグループ
                    {{ title: "📐 High-Base(Strict) [Quality Validated]", hint: "品質タグ合格銘柄 (すべて表示) | 順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('High-Base(Strict)') && (x.isTrendOk || x.isStrictVcp)).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])) }},
                    {{ title: "📐 High-Base(Strict) [Category TOP 5]", hint: "品質不問 | 優先順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('High-Base(Strict)')).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])).slice(0,5) }},

                    {{ title: "📐 High-Base [Quality Validated]", hint: "品質タグ合格銘柄 (すべて表示) | 順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('High-Base') && !x.pattern.includes('Strict') && (x.isTrendOk || x.isStrictVcp)).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])) }},
                    {{ title: "📐 High-Base [Category TOP 5]", hint: "品質不問 | 優先順位: 定着日数 ➔ 最新発射台", 
                        data: analyzed.filter(x => x.pattern.includes('High-Base') && !x.pattern.includes('Strict')).sort(getSorter(['persistence','latestLaunchpad'], [-1,-1])).slice(0,5) }},

                    {{ title: "🕵️ Stealth Accumulation (隠密買い集め)", hint: "優先順位: 隠密スコア ➔ 定着 ➔ 低ボラ", 
                        data: analyzed.filter(x => x.stealthScore > 0).sort(getSorter(['stealthScore','persistence','vol'], [-1,-1,1])).slice(0,5) }},
                    {{ title: "🕵️ Momentum Stealth (短期加速)", hint: "優先順位: 隠密スコア ➔ 定着 ➔ 低ボラ", 
                        data: analyzed.filter(x => x.momentumStealthScore > 0).sort(getSorter(['momentumStealthScore','persistence','vol'], [-1,-1,1])).slice(0,5) }},
                    {{ title: "🏆 総合・サバイバルリーダー", hint: "優先順位: 定着 ➔ 騰落率 ➔ 成長率", 
                        data: [...analyzed].sort(getSorter(['persistence','change','growth'], [-1,-1,-1])).slice(0,5) }}
                ];

                let html = "";
                sections.forEach(sec => {{
                    html += `<div class="card"><h2 class="section-title">${{sec.title}}</h2><p class="priority-hint">${{sec.hint}}</p><div class="rank-grid">`;
                    if (sec.data.length === 0) html += "<p>対象なし</p>";
                    sec.data.forEach((s, idx) => {{
                        const badges = (s.isTrendOk ? '<span class="q-badge q-trend">Trend_OK</span>' : '') + (s.isStrictVcp ? '<span class="q-badge q-strict">VCP_Strict</span>' : '');
                        html += `
                        <div class="rank-card">
                            <div class="rank-badge">${{idx+1}}</div>
                            <span class="persistence-tag">${{s.persistence}}日出現</span>
                            <h3 style="margin:5px 0;">${{s.ticker}}</h3>
                            <div class="quality-badges">${{badges}}</div>
                            <div class="metric-box">
                                <div class="metric-row"><span>${{sec.title.includes('Stealth') ? '隠密Score' : '発射台Score'}}</span> <b class="score-highlight">${{s.momentumStealthScore || s.stealthScore || s.latestLaunchpad}}</b></div>
                                <div class="metric-row"><span>期間騰落</span> <b style="color:${{s.change >=0 ? '#e74c3c':'#2980b9'}}">${{s.change.toFixed(1)}}%</b></div>
                                <div class="metric-row"><span>売上成長</span> <b>${{s.growth}}%</b></div>
                            </div>
                            <div class="pattern-tag" title="${{s.pattern}}">${{s.pattern}}</div>
                        </div>`;
                    }});
                    html += "</div></div>";
                }});
                document.getElementById('dynamic-rankings-area').innerHTML = html;

                const chartData = targetDates.map(d => data.market.find(m => m.date===d)).filter(m => m.valid);
                Plotly.newPlot('chart-market', [
                    {{ x: chartData.map(m => m.date), y: chartData.map(m => m.ad), name: 'A/D比', type: 'scatter', line: {{width:4, color:'#3498db'}} }},
                    {{ x: chartData.map(m => m.date), y: chartData.map(m => m.dist), name: '売り抜け', type: 'bar', opacity: 0.3, marker: {{color:'#e74c3c'}}, yaxis: 'y2' }}
                ], {{ yaxis: {{title: 'A/D比'}}, yaxis2: {{overlaying:'y', side:'right', title: '売り抜け日'}}, margin: {{t:20, b:40, l:50, r:50}}, template: 'plotly_white' }});

                const scatterData = [...analyzed].sort((a, b) => a.launchpad - b.launchpad);
                Plotly.newPlot('chart-scatter', [{{
                    x: scatterData.map(x => x.persistence), y: scatterData.map(x => x.change), text: scatterData.map(x => x.ticker),
                    mode: 'markers+text', textposition: 'top center',
                    marker: {{ size: 14, color: scatterData.map(x => x.launchpad), colorscale: [[0, 'rgb(255, 255, 204)'], [1, 'rgb(189, 0, 38)']], reversescale: false, cmin: 0, cmax: 10, showscale: true, colorbar: {{title: 'Score', titleside: 'right'}} }}
                }}], {{ xaxis: {{title: '出現日数'}}, yaxis: {{title: '期間騰落率(%)'}}, margin: {{t:20, b:40, l:50, r:50}}, template: 'plotly_white' }});
            }}
            handleDateChange();
        </script>
    </body>
    </html>
    """
    return html_content

def upload_to_drive(content, filename):
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
    query = f"'{SUMMARY_FOLDER_ID}' in parents and name contains 'weekly_detailed_trend' and trashed = false"
    res = service.files().list(q=query, fields="files(id, name)", orderBy="createdTime desc").execute()
    if not res.get('files'):
        print("CSV file not found."); sys.exit(1)
    file_id = res['files'][0]['id']
    csv_name = res['files'][0]['name']
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    trend_df = pd.read_csv(fh, dtype=str)
    accumulation_data = get_accumulation_ranking(service)
    html_report = create_intelligence_report(trend_df, accumulation_data)
    report_filename = csv_name.replace('weekly_detailed_trend', 'interactive_ranking').replace('.csv', '.html')
    upload_to_drive(html_report, report_filename)
