import os
import sqlite3
import time
import json
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# ==========================================
# 1. 配置区域
# ==========================================
DATA_DIR = 'data'
DB_FILENAME = 'financial_data_v20.db' # 升级到 V20
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)

# 【代理配置】
PROXY_URL = "http://127.0.0.1:10808"
os.environ["HTTP_PROXY"] = PROXY_URL
os.environ["HTTPS_PROXY"] = PROXY_URL

# JSON 序列化辅助 (修复 NaN 问题)
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating):
            if np.isnan(obj): return None # 将 NaN 转为 null
            return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return super(NpEncoder, self).default(obj)

# ==========================================
# 2. 数据库初始化
# ==========================================
def init_database():
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    
    # 强制清理旧库，因为表结构变了
    if os.path.exists(DB_PATH):
        try: os.remove(DB_PATH)
        except: pass

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 新增 fiscal_year, fiscal_quarter 字段
    c.execute('''CREATE TABLE IF NOT EXISTS historical_financials
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ticker TEXT,
                  announce_date TEXT,       -- 实际发布日
                  report_period TEXT,       -- 财报截止日 (自然日)
                  fiscal_year INTEGER,      -- 财年 (如 2026)
                  fiscal_quarter TEXT,      -- 财季 (如 Q3)
                  report_type TEXT,         -- Annual/Quarterly
                  adj_close_price REAL,
                  shares_outstanding REAL,
                  market_cap_billions REAL,
                  financials_json TEXT,
                  data_source TEXT,
                  updated_at TEXT)''')
    conn.commit()
    conn.close()
    print(f"✅ [Init] 数据库 V20 就绪: {DB_PATH}")

# ==========================================
# 3. 核心算法：财年/财季计算器
# ==========================================
def calculate_fiscal_context(ticker_obj, report_date):
    """
    根据公司的财年结束月，将 自然日期 转换为 财年/财季
    NVDA 案例: 财年结束 1月。Report 2025-10-31 -> FY2026 Q3
    """
    try:
        # 获取财年结束信息
        info = ticker_obj.info
        # lastFiscalYearEnd 是时间戳，转为月份
        fy_end_ts = info.get('lastFiscalYearEnd')
        
        if fy_end_ts:
            fy_end_month = datetime.fromtimestamp(fy_end_ts).month
        else:
            # 默认兜底：NVDA=1, AAPL=9, MSFT=6
            ticker_symbol = ticker_obj.ticker
            if ticker_symbol == 'NVDA': fy_end_month = 1
            elif ticker_symbol == 'AAPL': fy_end_month = 9
            elif ticker_symbol == 'MSFT': fy_end_month = 6
            else: fy_end_month = 12

        r_year = report_date.year
        r_month = report_date.month

        # 逻辑：
        # 如果 财年结束月是 12月 (正常): 财年 = 自然年
        # 如果 财年结束月 < 报表月: 财年 = 自然年 + 1
        
        fiscal_year = r_year
        if r_month > fy_end_month:
            fiscal_year = r_year + 1
        
        # 计算季度
        # 核心逻辑：计算当前月相对于财年开始月的偏移量
        # 财年开始月 = fy_end_month + 1
        months_offset = (r_month - fy_end_month + 12) % 12
        if months_offset == 0: months_offset = 12
        
        q_num = (months_offset - 1) // 3 + 1
        fiscal_quarter = f"Q{q_num}"
        
        return fiscal_year, fiscal_quarter

    except Exception:
        # 出错兜底：按自然年算
        return report_date.year, f"Q{(report_date.month-1)//3+1}"

# ==========================================
# 4. 核心算法：历史股本回溯
# ==========================================
def get_historical_shares(tick, target_date):
    """
    通过 get_shares_full 获取具体时间点的历史股本
    解决旧财报中 shares 为 NULL 的问题
    """
    try:
        # 获取历史股本序列
        shares_series = tick.get_shares_full(start=target_date - timedelta(days=90), 
                                             end=target_date + timedelta(days=30))
        
        if shares_series.empty:
            return None
        
        # 找到离 target_date 最近的那个索引
        # shares_series.index 包含时间信息
        # 将 target_date 转为 timestamp 用于比较
        target_ts = pd.Timestamp(target_date).tz_localize(shares_series.index.tz)
        
        # 找最近的
        closest_idx = shares_series.index.get_indexer([target_ts], method='nearest')[0]
        shares = shares_series.iloc[closest_idx]
        
        return float(shares)
    except:
        return None

# ==========================================
# 5. 数据获取 (YFinance)
# ==========================================
def fetch_and_process_data(ticker):
    print(f"\nAnalyzing {ticker}...")
    tick = yf.Ticker(ticker)
    
    # 1. 获取财报日历 (用于公告日)
    try:
        calendar_df = tick.earnings_dates
        if calendar_df is not None:
            # 索引是发布日，将其转为字符串列表
            calendar_dates = [d.strftime('%Y-%m-%d') for d in calendar_df.index]
        else:
            calendar_dates = []
    except: calendar_dates = []

    # 2. 获取原始报表
    results = []
    
    # 获取所有可能的报表
    tables = [
        (tick.income_stmt.T, 'Annual'), 
        (tick.quarterly_income_stmt.T, 'Quarterly'),
        (tick.balance_sheet.T, 'Annual'),
        (tick.quarterly_balance_sheet.T, 'Quarterly'),
        (tick.cash_flow.T, 'Annual'),
        (tick.quarterly_cash_flow.T, 'Quarterly')
    ]

    # 合并同类项 (按日期和类型)
    merged_data = {} # Key: (date, type) -> Value: dict
    
    for df, r_type in tables:
        if df.empty: continue
        for dt, row in df.iterrows():
            # 处理日期和时区
            r_date = dt.tz_localize(None) if hasattr(dt, 'tz') and dt.tz else dt
            if not isinstance(r_date, datetime): r_date = pd.to_datetime(r_date)
            if r_date > datetime.now(): continue # 过滤未来

            key = (r_date, r_type)
            if key not in merged_data:
                merged_data[key] = {}
            
            # 累加数据
            row_dict = row.to_dict()
            merged_data[key].update(row_dict)

    # 3. 处理每一条汇总数据
    final_list = []
    for (r_date, r_type), data_dict in merged_data.items():
        # A. 计算财年/财季 (NVDA 修正逻辑)
        if r_type == 'Annual':
            fy, fq = calculate_fiscal_context(tick, r_date)
            fq = "FY" # 年报统一显示 FY
        else:
            fy, fq = calculate_fiscal_context(tick, r_date)
        
        # B. 确定公告日 (Announce Date)
        r_date_str = r_date.strftime('%Y-%m-%d')
        ann_date_str = None
        
        # 在日历中找匹配
        # 逻辑：公告日通常在截止日后 15-90 天
        best_diff = 999
        r_dt_obj = r_date.to_pydatetime()
        
        for cal_d_str in calendar_dates:
            cal_dt = datetime.strptime(cal_d_str, '%Y-%m-%d')
            diff = (cal_dt - r_dt_obj).days
            if 10 <= diff <= 100:
                if diff < best_diff:
                    best_diff = diff
                    ann_date_str = cal_d_str
        
        if not ann_date_str:
            # 找不到就估算
            offset = 60 if r_type == 'Annual' else 35
            ann_date_str = (r_date + timedelta(days=offset)).strftime('%Y-%m-%d')
        
        # 修正：公告日不能是未来
        if datetime.strptime(ann_date_str, '%Y-%m-%d') > datetime.now():
            ann_date_str = datetime.now().strftime('%Y-%m-%d')

        # C. 获取股本 (三级策略：报表 -> 历史序列 -> 当前)
        shares = data_dict.get('Ordinary Shares Number')
        if not shares: shares = data_dict.get('Share Issued')
        
        # 如果报表里没有，去历史序列里查 (修复 NULL 关键)
        if not shares:
            shares = get_historical_shares(tick, datetime.strptime(ann_date_str, '%Y-%m-%d'))
            
        # 如果还没有，用当前兜底 (但标记一下)
        if not shares:
            shares = tick.info.get('sharesOutstanding')
        
        final_list.append({
            'report_period': r_date_str,
            'announce_date': ann_date_str,
            'fiscal_year': fy,
            'fiscal_quarter': fq,
            'report_type': r_type,
            'shares': shares,
            'data': data_dict
        })
    
    return final_list

# ==========================================
# 6. 股价获取 (复权)
# ==========================================
def get_price(ticker, date_str):
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        if dt > datetime.now(): dt = datetime.now() - timedelta(days=1)
        
        start = dt - timedelta(days=10)
        end = dt + timedelta(days=10)
        
        # auto_adjust=True 拿到复权价
        hist = yf.Ticker(ticker).history(start=start, end=end)
        if hist.empty: return None
        
        target_ts = pd.Timestamp(date_str).tz_localize(hist.index.tz)
        
        # 找最近的收盘价
        idx = hist.index.get_indexer([target_ts], method='nearest')[0]
        return float(hist.iloc[idx]['Close'])
    except: return None

# ==========================================
# 7. 主流程
# ==========================================
def run_v20():
    init_database()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    targets = ['AAPL', 'NVDA', 'MSFT']
    
    for t in targets:
        data_list = fetch_and_process_data(t)
        # 按时间倒序
        data_list.sort(key=lambda x: x['report_period'], reverse=True)
        
        count = 0
        for item in data_list:
            # 查重 (增加 fiscal_quarter 维度)
            exists = c.execute("SELECT id FROM historical_financials WHERE ticker=? AND report_period=? AND report_type=?", 
                             (t, item['report_period'], item['report_type'])).fetchone()
            if exists: continue

            price = get_price(t, item['announce_date'])
            shares = item['shares']
            
            if price and shares:
                mkt_cap = price * shares
                # 使用 NpEncoder 清洗 json
                json_str = json.dumps(item['data'], cls=NpEncoder)
                
                # 打印日志，着重显示财年信息
                fy_info = f"FY{item['fiscal_year']} {item['fiscal_quarter']}"
                
                c.execute('''INSERT INTO historical_financials 
                             (ticker, announce_date, report_period, fiscal_year, fiscal_quarter, report_type,
                              adj_close_price, shares_outstanding, market_cap_billions, 
                              financials_json, data_source, updated_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (t, item['announce_date'], item['report_period'], 
                           item['fiscal_year'], item['fiscal_quarter'], item['report_type'],
                           price, shares, mkt_cap / 1e9, 
                           json_str, "YFinance_V20", datetime.now().strftime('%Y-%m-%d')))
                
                print(f"   ✅ {t} {item['report_period']} -> {fy_info} | 市值 ${mkt_cap/1e9:.2f}B | 股本 {shares/1e9:.2f}B")
                count += 1
            else:
                print(f"   ⚠️ {t} {item['report_period']} 缺失数据: Price={price}, Shares={shares}")

        print(f"   -> 入库 {count} 条")
        time.sleep(1)
        
    conn.commit()
    conn.close()
    print(f"\n🏁 完成。请检查数据库中的 'fiscal_year' 和 'fiscal_quarter' 列。")

if __name__ == "__main__":
    run_v20()