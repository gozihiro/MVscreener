import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import io
import re
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- [Drive操作等の共通関数は既存のものを維持] ---

def create_intelligence_report(df):
    date_cols = sorted([c for c in df.columns if '価格_' in c])
    dates = [c.split('_')[-1] for c in date_cols]
    latest_date = dates[-1]

    # --- 1. 銘柄データの数値化と特徴抽出 ---
    stocks = df[df['銘柄'] != '### MARKET_ENVIRONMENT ###'].copy()
    analysis_list = []
    
    for _, row in stocks.iterrows():
        prices = [pd.to_numeric(row.get(f'価格_{d}'), errors='coerce') for d in dates]
        prices = [p for p in prices if pd.notnull(p)]
        if not prices: continue

        persistence = int(pd.to_numeric(row.get('出現回数', 0), errors='coerce') or 0)
        weekly_change = ((prices[-1] / prices[0]) - 1) * 100 if prices[0] != 0 else 0
        
        # ボラティリティ（タイトネス）: (最大-最小)/最小。低いほど「タイト」
        vol = ((max(prices) - min(prices)) / min(prices) * 100) if min(prices) > 0 else 999
        
        growth = float(pd.to_numeric(get_latest_non_empty(row, "売上成長(%)", dates), errors='coerce') or 0)
        pattern = get_latest_non_empty(row, "パターン", dates)

        analysis_list.append({
            'ticker': row['銘柄'], 
            'persistence': persistence,
            'change': weekly_change, 
            'growth': growth, 
            'vol': vol,
            'pattern': pattern,
            'judgment': get_latest_non_empty(row, "成長性判定", dates)
        })
    
    all_df = pd.DataFrame(analysis_list)

    # --- 2. カテゴリ別・多段階ソート実行 ---
    
    # ① 総合ランキング (定着 > 騰落 > 成長 > ボラ低)
    top_overall = all_df.sort_values(
        by=['persistence', 'change', 'growth', 'vol'], 
        ascending=[False, False, False, True]
    ).head(5)

    # ② High-Base (Strict) (定着 > ボラ低 > 騰落 > 成長)
    hb_strict_leaders = all_df[all_df['pattern'].str.contains('High-Base\(Strict\)', na=False)].sort_values(
        by=['persistence', 'vol', 'change', 'growth'], 
        ascending=[False, True, False, False]
    ).head(3)

    # ③ High-Base (Normal) (定着 > ボラ低 > 騰落 > 成長) ※Strictは除外
    hb_normal_leaders = all_df[
        all_df['pattern'].str.contains('High-Base', na=False) & 
        ~all_df['pattern'].str.contains('Strict', na=False)
    ].sort_values(
        by=['persistence', 'vol', 'change', 'growth'], 
        ascending=[False, True, False, False]
    ).head(3)

    # ④ VCP (定着 > ボラ低 > 成長 > 騰落)
    vcp_leaders = all_df[all_df['pattern'].str.contains('VCP', na=False)].sort_values(
        by=['persistence', 'vol', 'growth', 'change'], 
        ascending=[False, True, False, False]
    ).head(3)

    # ⑤ PowerPlay (定着 > 騰落高 > 成長 > ボラ低)
    power_leaders = all_df[all_df['pattern'].str.contains('PowerPlay', na=False)].sort_values(
        by=['persistence', 'change', 'growth', 'vol'], 
        ascending=[False, False, False, True]
    ).head(3)

    # --- 3. HTML構築 ---
    report_html = f"""
    <html>
    <head><meta charset='utf-8'><style>
        body {{ font-family: sans-serif; max-width: 1200px; margin: auto; padding: 20px; background: #f4f7f9; }}
        .card {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 25px; }}
        h1 {{ color: #2c3e50; text-align: center; border-bottom: 4px solid #3498db; padding-bottom: 10px; }}
        .category-title {{ color: #2980b9; border-left: 6px solid #2980b9; padding-left: 15px; margin-top: 30px; }}
        .rank-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; margin-top: 10px; }}
        .rank-card {{ border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; background: #fff; }}
        .badge {{ background: #e74c3c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .logic-note {{ font-size: 0.85em; color: #666; font-style: italic; margin-bottom: 10px; }}
    </style></head>
    <body>
        <h1>📊 週次：事実に基づく戦略的ランキングレポート</h1>

        <div class="card">
            <h2 class="category-title">🏆 総合・サバイバルリーダー (Survival Leaders)</h2>
            <p class="logic-note">優先順位：定着日数 ➔ 週次騰落率(RS) ➔ 売上成長率 ➔ 低ボラティリティ</p>
            {render_rank_grid(top_overall)}
        </div>

        <div class="card">
            <h2 class="category-title">📐 High-Base (Strict) リーダー</h2>
            <p class="logic-note">優先順位：定着日数 ➔ 低ボラティリティ(Tightness) ➔ 週次騰落率 ➔ 売上成長率</p>
            {render_rank_grid(hb_strict_leaders)}
        </div>

        <div class="card">
            <h2 class="category-title">📉 High-Base (Normal) リーダー</h2>
            <p class="logic-note">優先順位：定着日数 ➔ 低ボラティリティ ➔ 週次騰落率 ➔ 売上成長率</p>
            {render_rank_grid(hb_normal_leaders)}
        </div>

        <div class="card">
            <h2 class="category-title">🌀 VCP・ボラティリティ収束リーダー</h2>
            <p class="logic-note">優先順位：定着日数 ➔ 低ボラティリティ ➔ 売上成長率 ➔ 週次騰落率</p>
            {render_rank_grid(vcp_leaders)}
        </div>

        <div class="card">
            <h2 class="category-title">⚡ PowerPlay・勢い重視リーダー</h2>
            <p class="logic-note">優先順位：定着日数 ➔ 週次騰落率(高) ➔ 売上成長率 ➔ 低ボラティリティ</p>
            {render_rank_grid(power_leaders)}
        </div>
    </body>
    </html>
    """
    return report_html

def render_rank_grid(target_df):
    if target_df.empty: return "<p style='color:#999;'>今週の該当銘柄なし</p>"
    cards = []
    for _, s in target_df.iterrows():
        cards.append(f'''
        <div class="rank-card">
            <span class="badge">{s['persistence']}/5日 定着</span>
            <h3 style="margin:10px 0;">{s['ticker']}</h3>
            <div style="font-size:0.95em;">
                週次騰落: <b style="color:{'#e74c3c' if s['change'] >= 0 else '#2980b9'}">{s['change']:+.1f}%</b><br>
                値幅(Vol): <b>{s['vol']:.1f}%</b><br>
                売上成長: <b>{s['growth']}%</b>
            </div>
            <div style="margin-top:8px; font-size:0.8em; color:#888; border-top:1px solid #eee; padding-top:5px;">
                {s['pattern']}
            </div>
        </div>
        ''')
    return f'<div class="rank-grid">{"".join(cards)}</div>'
