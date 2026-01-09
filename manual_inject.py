import sqlite3

DB_NAME = 'test_financial.db'

def inject_test_data():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    print("💉 正在手动注入测试数据...")
    
    # 插入几条真实的历史财报发布日
    # 1. NVDA: 2023-11-21 (对应 10月底的财报)
    # 2. AAPL: 2023-11-02 (对应 9月底的财报)
    
    test_data = [
        ('NVDA', '2023-11-21', 0), # 0 代表未处理，会触发计算
        ('AAPL', '2023-11-02', 0)
    ]
    
    c.executemany('INSERT OR IGNORE INTO calendar_queue VALUES (?, ?, ?)', test_data)
    
    conn.commit()
    conn.close()
    print("✅ 注入完成！请重新运行主程序，它应该会跳过日历下载，直接开始计算市值。")

if __name__ == "__main__":
    inject_test_data()