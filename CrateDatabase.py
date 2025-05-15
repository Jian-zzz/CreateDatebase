import os
import glob
import sqlite3
import pandas as pd

# 原始 Excel 文件所在目录
root_dirs = [
    r"C:\Users\29563\Desktop\5.13 任务\5.13 任务\2023年",
    r"C:\Users\29563\Desktop\5.13 任务\5.13 任务\2024年"
]

# 统一保存数据库的根目录
output_base = r"C:\Users\29563\Desktop\5.13 任务\database"

for root in root_dirs:
    excel_files = glob.glob(os.path.join(root, "*.xlsx")) + glob.glob(os.path.join(root, "*.xls"))

    for file_path in excel_files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        table_base_name = file_name.replace(" ", "_").replace("-", "_")

        try:
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, engine='openpyxl')
            elif file_path.endswith('.xls'):
                df = pd.read_excel(file_path, engine='xlrd')
        except Exception as e:
            print(f"文件 {file_path} 无法读取（可能损坏）: {e}")
            continue

        date_cols = [col for col in df.columns if any(key in str(col) for key in ["日期", "时间"])]
        rain_cols = [col for col in df.columns if "雨量" in str(col)]

        if not date_cols or not rain_cols:
            print(f"文件 {file_path} 缺少日期或雨量列")
            continue

        date_col = date_cols[0]
        rain_col = rain_cols[0]

        df = df.rename(columns={date_col: "RevTime", rain_col: "DataValue"})
        df["RevTime"] = pd.to_datetime(df["RevTime"], errors="coerce")
        df = df.dropna(subset=["RevTime"])  # 移除无效日期

        df["Year"] = df["RevTime"].dt.strftime("%Y")
        df["Month"] = df["RevTime"].dt.strftime("%m")
        df["RevTime"] = df["RevTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce").fillna(0.0)
        df["SensorID"] = file_name
        df = df[["SensorID", "DataValue", "RevTime", "Year", "Month"]]

        # 按年/月分组
        for (year, month), group_df in df.groupby(["Year", "Month"]):
            # 构建目标数据库路径
            save_folder = os.path.join(output_base, year, month)
            os.makedirs(save_folder, exist_ok=True)

            db_path = os.path.join(save_folder, f"{file_name}.db")
            table_name = table_base_name

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
            cursor.execute(f"""
                CREATE TABLE [{table_name}] (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    SensorID VARCHAR,
                    DataValue FLOAT,
                    RevTime VARCHAR
                )
            """)

            try:
                insert_df = group_df[["SensorID", "DataValue", "RevTime"]]
                cursor.executemany(
                    f"INSERT INTO [{table_name}] (SensorID, DataValue, RevTime) VALUES (?, ?, ?)",
                    insert_df.values.tolist()
                )
                conn.commit()
                print(f"[成功] 写入 {db_path}（表：{table_name}）")
            except Exception as e:
                print(f"[失败] 插入失败：{e}")
                conn.rollback()
            finally:
                conn.close()

print("处理完成。")