import os
import sqlite3
import time
import json
import requests
import re
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# ==========================================
# 1. 配置区域
# ==========================================
DB_NAME = 'financial_data_v17.db'

# 【代理配置】
PROXY_URL = "http://127.0.0.1:10808"
os.environ["HTTP_PROXY"] = PROXY_URL
os.environ["HTTPS_PROXY"] = PROXY_URL
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

# 伪装头 (模拟真实浏览器)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1"
}

# 字段映射
SA_MAP = {
    'Revenue': 'Total Revenue', 'revenue': 'Total Revenue',
    'Net Income': 'Net Income', 'netIncome': 'Net Income',
    'Gross Profit': 'Gross Profit', 'grossProfit': 'Gross Profit',
    'Operating Income': 'Operating Income', 'opIncome': 'Operating Income',
    'EBITDA': 'EBITDA', 'ebitda': 'EBITDA',
    'Shares Outstanding (Basic)': 'Ordinary Shares Number', 'shares': 'Ordinary Shares Number',
    'EPS (Basic)': 'Basic EPS', 'eps': 'Basic EPS'
}

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return super(NpEncoder, self).default(obj)

# ==========================================
# 2. 数据库初始化
# ==========================================
def init_database():
    if os.path.exists(DB_NAME):
        try:
            os.remove(DB_NAME)
            print(f"🧹 [Clean] 旧库已清理。")
        except: pass

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS historical_financials
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ticker TEXT,
                  announce_date TEXT,
                  report_period TEXT,
                  period_label TEXT,
                  report_type TEXT,
                  adj_close_price REAL,
                  shares_outstanding REAL,
                  market_cap_billions REAL,
                  financials_json TEXT,
                  updated_at TEXT)''')
    conn.commit()
    conn.close()
    print(f"✅ [Init] 数据库 {DB_NAME} 就绪。")

# ==========================================
# 3. 获取精准财报日历
# ==========================================
def get_earnings_calendar_yf(ticker):
    print(f"   📅 获取财报日历 (YF)...")
    try:
        tick = yf.Ticker(ticker)
        dates = tick.earnings_dates
        if dates is None or dates.empty: return []
        valid_dates = []
        for dt in dates.index:
            valid_dates.append(dt.strftime('%Y-%m-%d'))
        return valid_dates
    except Exception as e:
        print(f"      ⚠️ 日历获取失败: {e}")
        return []

# ==========================================
# 4. 主数据源：StockAnalysis 爬虫
# ==========================================
def fetch_data_stockanalysis(ticker):
    base_url = f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/"
    urls = [(base_url, "Annual"), (base_url + "?p=quarterly", "Quarterly")]
    processed_data = []
    
    for url, r_type in urls:
        print(f"   🕷️ 尝试爬取 {r_type}: {url} ...")
        try:
            r = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=10)
            if r.status_code != 200: continue
            
            match = re.search(r'<script id=["\']__NEXT_DATA__["\'] type=["\']application/json["\']>(.*?)</script>', r.text)
            if not match:
                print("      ⚠️ 未找到数据标记 (可能被反爬拦截)")
                continue
                
            data_json = json.loads(match.group(1))
            try:
                core_data = data_json['props']['pageProps']['data']['data']
            except: continue

            for item in core_data:
                date_str = item.get('date')
                if not date_str: continue
                r_date = datetime.strptime(date_str, "%Y-%m-%d")
                if r_date > datetime.now(): continue
                
                final_data = {}
                for sa_k, std_k in SA_MAP.items():
                    val = item.get(sa_k)
                    if val is not None:
                        try: final_data[std_k] = float(val)
                        except: pass
                
                shares = final_data.get('Ordinary Shares Number', 0)
                q_num = (r_date.month - 1) // 3 + 1
                period_label = "FY" if r_type == 'Annual' else f"Q{q_num}"

                processed_data.append({
                    'report_period': date_str,
                    'report_type': r_type,
                    'period_label': period_label,
                    'shares': shares,
                    'data': final_data
                })
        except Exception:
            continue
            
    return processed_data

# ==========================================
# 5. 备用数据源：YFinance (修复版)
# ==========================================
def fetch_data_yfinance_backup(ticker):
    print(f"   🛡️ 启动备用数据源 (YFinance)...")
    tick = yf.Ticker(ticker)
    results = []
    
    try:
        # 【核心修复】兼容不同版本的 YFinance 属性名
        # 1. 尝试获取 Income Statement
        if hasattr(tick, 'income_stmt'): 
            inc_a = tick.income_stmt.T
            inc_q = tick.quarterly_income_stmt.T
        elif hasattr(tick, 'financials'):
            inc_a = tick.financials.T
            inc_q = tick.quarterly_financials.T
        else: 
            print("      ❌ 无法找到 Income Statement 属性")
            return []

        # 2. 尝试获取 Balance Sheet
        if hasattr(tick, 'balance_sheet'):
            bal_a = tick.balance_sheet.T
            bal_q = tick.quarterly_balance_sheet.T
        else:
            bal_a, bal_q = pd.DataFrame(), pd.DataFrame()

        # 3. 尝试获取 Cash Flow
        if hasattr(tick, 'cashflow'):
            cf_a = tick.cashflow.T
            cf_q = tick.quarterly_cashflow.T
        elif hasattr(tick, 'cash_flow'):
            cf_a = tick.cash_flow.T
            cf_q = tick.quarterly_cash_flow.T
        else:
            cf_a, cf_q = pd.DataFrame(), pd.DataFrame()
        
        # 定义任务
        tasks = [
            (inc_a, bal_a, cf_a, 'Annual', 'FY'),
            (inc_q, bal_q, cf_q, 'Quarterly', 'Qx')
        ]
        
        for inc, bal, cf, r_type, p_lbl in tasks:
            if inc.empty: continue
            
            # 合并
            full = inc.join(bal, lsuffix='_i', rsuffix='_b').join(cf, rsuffix='_c')
            full = full.loc[:, ~full.columns.duplicated()]
            
            for date_idx, row in full.iterrows():
                # 处理时区
                r_date = date_idx.tz_localize(None) if hasattr(date_idx, 'tz') and date_idx.tz else date_idx
                # 防止 date_idx 变成 object 类型
                if not isinstance(r_date, datetime): r_date = pd.to_datetime(r_date)
                
                if r_date > datetime.now(): continue
                
                final_data = row.to_dict()
                
                # 简单映射 YF 字段
                shares = final_data.get('Ordinary Shares Number', 0)
                if not shares: shares = final_data.get('Share Issued', 0)
                if not shares: shares = tick.info.get('sharesOutstanding', 0)
                
                if r_type == 'Quarterly':
                    q_num = (r_date.month - 1) // 3 + 1
                    period_label = f"Q{q_num}"
                else:
                    period_label = "FY"
                
                results.append({
                    'report_period': r_date.strftime('%Y-%m-%d'),
                    'report_type': r_type,
                    'period_label': period_label,
                    'shares': shares,
                    'data': final_data
                })
    except Exception as e:
        print(f"      ❌ 备用源异常: {e}")
        
    return results

# ==========================================
# 6. 股价获取
# ==========================================
def get_adj_close_price(ticker, target_date_str):
    try:
        target_dt = datetime.strptime(target_date_str, '%Y-%m-%d')
        if target_dt > datetime.now(): 
            target_dt = datetime.now() - timedelta(days=1)
            
        start = target_dt - timedelta(days=7)
        end = target_dt + timedelta(days=7)
        
        tick = yf.Ticker(ticker)
        hist = tick.history(start=start, end=end)
        
        if hist.empty: return None, "No Data"
        
        hist.index = hist.index.tz_localize(None)
        target_ts = pd.Timestamp(target_date_str)
        
        past_df = hist[hist.index <= target_ts]
        if not past_df.empty:
            return past_df.iloc[-1]['Close'], past_df.index[-1].strftime('%Y-%m-%d')
        return None, "Gap"
    except: return None, "Err"

# ==========================================
# 7. 主流程
# ==========================================
def run_v17():
    init_database()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    targets = ['AAPL', 'NVDA', 'MSFT']
    print("🚀 启动 V17 (属性修复版)")
    
    for t in targets:
        print(f"\nAnalyzing {t}...")
        
        known_announce_dates = get_earnings_calendar_yf(t)
        
        # 1. 尝试爬虫
        fin_data = fetch_data_stockanalysis(t)
        
        # 2. 如果爬虫失败，切换 YFinance (代码已修复)
        if not fin_data:
            print("   ⚠️ 爬虫无数据，切换到 YFinance 备用通道...")
            fin_data = fetch_data_yfinance_backup(t)
            
        print(f"   📄 最终获取到 {len(fin_data)} 条记录")
        
        fin_data.sort(key=lambda x: x['report_period'], reverse=True)
        
        valid_count = 0
        for item in fin_data:
            r_period = item['report_period']
            
            exists = c.execute("SELECT id FROM historical_financials WHERE ticker=? AND report_period=? AND report_type=?", 
                             (t, r_period, item['report_type'])).fetchone()
            if exists: continue

            # 公告日匹配
            r_dt = datetime.strptime(r_period, '%Y-%m-%d')
            best_ann_date = None
            min_diff = 999
            for ad_str in known_announce_dates:
                ad_dt = datetime.strptime(ad_str, '%Y-%m-%d')
                diff = (ad_dt - r_dt).days
                if 10 <= diff <= 100:
                    if diff < min_diff:
                        min_diff = diff
                        best_ann_date = ad_str
            
            if best_ann_date:
                ann_date = best_ann_date
                note = "精准"
            else:
                days = 60 if item['report_type'] == 'Annual' else 35
                ann_date = (r_dt + timedelta(days=days)).strftime('%Y-%m-%d')
                note = "估算"
            
            if datetime.strptime(ann_date, '%Y-%m-%d') > datetime.now():
                ann_date = datetime.now().strftime('%Y-%m-%d')

            # 股价
            price, p_date = get_adj_close_price(t, ann_date)
            shares = item['shares']
            
            if (not shares or shares == 0) and price:
                 try: shares = yf.Ticker(t).info.get('sharesOutstanding', 0)
                 except: pass

            if price and shares:
                mkt_cap = price * shares
                json_str = json.dumps(item['data'], cls=NpEncoder)
                
                c.execute('''INSERT INTO historical_financials 
                             (ticker, announce_date, report_period, period_label, report_type,
                              adj_close_price, shares_outstanding, market_cap_billions, 
                              financials_json, updated_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (t, ann_date, r_period, item['period_label'], item['report_type'],
                           price, shares, mkt_cap / 1e9, json_str, datetime.now().strftime('%Y-%m-%d')))
                
                print(f"   ✅ {r_period} | {item['report_type']} | 市值 ${mkt_cap/1e9:.2f}B")
                valid_count += 1
                
        print(f"   📊 本轮新增: {valid_count} 条记录")
        time.sleep(2)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_v17()