import sqlite3

conn = sqlite3.connect('test_financial.db')
c = conn.cursor()
# 手动插入一条 Apple 2023年Q3财报的发布记录 (发布日: 2023-11-02)
# 这样程序就会以为这是一条“过期未处理”的任务，从而触发计算
c.execute("INSERT OR IGNORE INTO calendar_queue VALUES ('AAPL', '2023-11-02', 0)")
conn.commit()
conn.close()
print("已插入测试用的历史数据。")