import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import os

USER_AGENT = 'Minervini-Screener/GA (contact: your-email@example.com)'

def get_full_universe_tickers():
    headers = {'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate'}
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        return [item['ticker'].replace('-', '.') for item in data.values()]
    except: return []

def get_market_health(idx_df):
    c = idx_df['Close'].squeeze()
    v = idx_df['Volume'].squeeze()
    sma50 = c.rolling(50).mean().iloc[-1]
    curr = c.iloc[-1]
    recent_close = c.iloc[-25:]
    recent_vol = v.iloc[-25:]
    dist_days = 0
    acc_days = 0
    for i in range(1, len(recent_close)):
        if recent_close.iloc[i] < recent_close.iloc[i-1] and recent_vol.iloc[i] > recent_vol.iloc[i-1]: dist_days += 1
        if recent_close.iloc[i] > recent_close.iloc[i-1] and recent_vol.iloc[i] > recent_vol.iloc[i-1]: acc_days += 1
    score = 1
    if curr > sma50 and dist_days < 4 and acc_days > dist_days: score = 2
    if curr < sma50 or dist_days >= 6 or (dist_days == 5 and dist_days > acc_days): score = 0
    return score, dist_days, acc_days

def analyze_ticker(ticker, idx_close):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        mkt_cap = info.get('marketCap', 0)
        if mkt_cap == 0 or mkt_cap > 100 * 1e9: return None
        df = stock.history(period="1y", auto_adjust=True)
        if len(df) < 200: return None
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        sma20, sma50, sma200 = c.rolling(20).mean(), c.rolling(50).mean(), c.rolling(200).mean()
        ema10 = c.ewm(span=10, adjust=False).mean()
        vol_sma50 = v.rolling(50).mean()
        tags = []
        if (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1]) and (sma200.iloc[-20:].diff().dropna() > 0).all() and (v.iloc[-3:] < vol_sma50.iloc[-3:]).all() and ((c.rolling(20).std().iloc[-1]*4/sma20.iloc[-1]) == (c.rolling(20).std()*4/sma20).iloc[-20:].min()):
            tags.append("VCP_Original")
        if (c.iloc[-1] > sma20.iloc[-1] > sma50.iloc[-1]):
            if (c.iloc[-1] / c.iloc[-40] >= 1.70) and (c.iloc[-3:] > ema10.iloc[-3:]).all(): tags.append("PowerPlay")
            if (1.10 <= c.iloc[-1]/c.iloc[-10] <= 1.70) and (c.iloc[-1]/h.iloc[-10:].max() >= 0.90): tags.append("High-Base")
        if tags:
            rev_g, eps_g = info.get('revenueGrowth', 0), info.get('earningsGrowth', 0)
            f_score = 2 if (rev_g >= 0.25 and eps_g >= 0.25) else (1 if (rev_g >= 0.25 or eps_g >= 0.25 or rev_g >= 0.50) else 0)
            return {"Ticker": ticker, "Price": round(c.iloc[-1], 2), "Tags": ", ".join(tags), "F_Score": f_score}
    except: pass
    return None

if __name__ == "__main__":
    universe = get_full_universe_tickers()
    idx_df = yf.download("^GSPC", period="1y", progress=False, auto_adjust=True)
    m_score, d_days, a_days = get_market_health(idx_df)
    print(f"Market Score: {m_score}, Dist: {d_days}, Acc: {a_days}")
    
    found = []
    for ticker in universe[:500]: # テスト用に最初は数を絞るのを推奨
        res = analyze_ticker(ticker, idx_df['Close'].squeeze())
        if res:
            found.append(res)
            print(f"Found: {res['Ticker']}")
        time.sleep(0.1)
    
    pd.DataFrame(found).to_csv("results.csv", index=False)
