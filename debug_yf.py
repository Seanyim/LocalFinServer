import yfinance as yf
import pandas as pd

# 设置显示所有列
pd.set_option('display.max_columns', None)

def diagnose_ticker(symbol):
    print(f"\n🔬 正在诊断: {symbol} ...")
    try:
        tick = yf.Ticker(symbol)
        
        # 1. 测试基础连接 (Info)
        print("   [1/3] 获取基础信息 (Info)... ", end="")
        try:
            info = tick.info
            # 检查是否包含关键字段
            if 'currentPrice' in info or 'symbol' in info:
                print("✅ 成功")
            else:
                print("⚠️ 成功但数据不完整")
        except Exception as e:
            print(f"❌ 失败: {e}")

        # 2. 测试财务数据 (Quarterly Income)
        print("   [2/3] 获取季度财报 (Quarterly Income)... ", end="")
        q_inc = tick.quarterly_income_stmt
        if q_inc is not None and not q_inc.empty:
            print(f"✅ 成功 (获取到 {len(q_inc.columns)} 个季度)")
            print(f"       最近周期: {q_inc.columns[0].date()}")
        else:
            print("❌ 失败 (返回为空)")
            
        # 3. 测试股价 (History)
        print("   [3/3] 获取股价 (History)... ", end="")
        hist = tick.history(period="5d")
        if not hist.empty:
            print(f"✅ 成功 (最近收盘价: {hist['Close'].iloc[-1]:.2f})")
        else:
            print("❌ 失败 (返回为空)")

    except Exception as e:
        print(f"\n❌ 严重错误: {e}")

if __name__ == "__main__":
    # 测试 NVDA
    diagnose_ticker("NVDA")