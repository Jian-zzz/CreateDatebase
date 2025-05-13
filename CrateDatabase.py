import os
import glob
import sqlite3
import pandas as pd

root_dirs = [
    r"C:\Users\29563\Desktop\5.13 任务\5.13 任务\2023年",
    r"C:\Users\29563\Desktop\5.13 任务\5.13 任务\2024年"
]

for root in root_dirs:
    excel_files = glob.glob(os.path.join(root, "*.xlsx")) + glob.glob(os.path.join(root, "*.xls"))

    for file_path in excel_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_").replace("-", "_")
        db_path = os.path.splitext(file_path)[0] + ".db"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")

        create_sql = f"""
        CREATE TABLE [{table_name}] (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            SensorID VARCHAR,
            DataValue FLOAT,
            RevTime VARCHAR
        )
        """
        cursor.execute(create_sql)

        try:
            # 读取 Excel 文件
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, engine='openpyxl')
            elif file_path.endswith('.xls'):
                df = pd.read_excel(file_path, engine='xlrd')
        except Exception as e:
            print(f"文件 {file_path} 无法读取（可能损坏）: {e}")
            conn.close()
            continue

        # 动态匹配列名：日期/时间
        date_cols = [col for col in df.columns if any(key in str(col) for key in ["日期", "时间"])]
        rain_cols = [col for col in df.columns if "雨量" in str(col)]

        # 检查列是否存在
        if not date_cols or not rain_cols:
            print(f"文件 {file_path} 缺少日期或雨量列")
            conn.close()
            continue

        date_col = date_cols[0]
        rain_col = rain_cols[0]

        # 重命名列
        df = df.rename(columns={date_col: "RevTime", rain_col: "DataValue"})

        # 处理日期格式
        df["RevTime"] = (
            pd.to_datetime(df["RevTime"], errors="coerce")  # 强制解析日期
            .dt.strftime("%Y-%m-%d %H:%M")  # 格式化为“年-月-日 时:分”
            # .fillna("0000-00-00 00:00")  # 无效日期默认值
        )

        # 处理数值列
        df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce").fillna(0.0)
        df["SensorID"] = os.path.splitext(os.path.basename(file_path))[0]
        df = df[["SensorID", "DataValue", "RevTime"]].dropna()

        # 插入数据库
        try:
            cursor.executemany(
                f"INSERT INTO [{table_name}] (SensorID, DataValue, RevTime) VALUES (?, ?, ?)",
                df.values.tolist()
            )
            conn.commit()
            print(f"[成功] 已写入数据库：{db_path}")
        except Exception as e:
            print(f"[失败] 插入数据失败：{e}")
            conn.rollback()

        conn.close()

print("处理完成。")