from .PostgreUtils import PostgreUtils
from .MysqlUtils import MysqlUtils
from .SQLiteUtils import SQLiteUtils
import pandas as pd
import concurrent.futures
from sqlalchemy import create_engine
import polars as pl
import base64
import pickle
from typing import List

class DBUtils:
    def __init__(self, db_name, user, password, host, port, db_instance="postgresql"):
        if db_instance == "postgresql":
            self.db = PostgreUtils(db_name, user, password, host, port)
            self.engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")  
        elif db_instance == "mysql":
            self.db = MysqlUtils(db_name, user, password, host, port)
            self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}")
        elif db_instance == "sqlite":
            self.db = SQLiteUtils(db_name, user, password, host, port)
            self.engine = create_engine(f"sqlite:///{db_name}")
        else:
            raise ValueError(f"Unsupported database instance: {db_instance}")

    def store_df(self, data: pl.DataFrame | pd.DataFrame, table_name: str, chunk_size: int = 2048, max_workers: int = 8, table_replace: bool = False):
        if len(data) == 0:
            return
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)
        if table_replace:
            data.head(0).write_database(table_name, self.engine.connect(), if_table_exists="replace")
        num_chunks = (len(data) + chunk_size - 1) // chunk_size  # 计算总块数
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.db.insert_df, data.slice(i * chunk_size, chunk_size), table_name) for i in range(num_chunks)]
            for future in futures:
                future.result()

    def store_dict(self, datas: dict[str, pd.DataFrame], chunk_size: int = 2048, max_workers: int = 8, table_replace: bool = False):
        for key, data in datas.items():
            if len(data) == 0:
                continue
            col_types = {}
            for col in data.columns:
                if data[col].iloc[0] is None:
                    filtered_df = data[data[col].notna()]
                    if (len(filtered_df) > 0 and isinstance(filtered_df[col].iloc[0], (str, int, float))) or len(
                            filtered_df) == 0:
                        col_types[col] = True
                    else:
                        col_types[col] = False
                else:
                    if isinstance(data[col].iloc[0], (str, int, float)):
                        col_types[col] = True
                    else:
                        col_types[col] = False

            # processed dataframe && store
            for col in data.columns:
                if not col_types[col]:
                    data[col] = data[col].apply(self._to_base64)
            data = pl.from_pandas(data)
            self.store_df(data, key, chunk_size, max_workers, table_replace)

    def query_df(self, table_name: str, columns: List[str] = ["*"], condition: str = None, limit: int = None, chunk_size: int = 2048, max_workers: int = 8) -> pd.DataFrame:
        single_data = self.db.select_df(table_name, columns=columns, condition=condition, limit=1)
        col_types = {}
        if len(single_data) == 0:
            return None
        else:
            for col in single_data.columns:
                if isinstance(single_data[col].iloc[0], str) and single_data[col].iloc[0].startswith("base64_encode::"):
                    col_types[col] = True
                else:
                    col_types[col] = False

        # Get total count first
        total_count = self.db.count_data(table_name, condition)
        if limit:
            total_count = min(total_count, limit)
        if total_count == 0:
            return None
        num_chunks = (total_count + chunk_size - 1) // chunk_size
        # Process chunks in parallel
        partial_dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    self.db.select_df,
                    table_name,
                    columns=columns,
                    condition=condition,
                    limit=chunk_size,
                    offset=i * chunk_size
                ) for i in range(num_chunks)
            ]
            for future in futures:
                df = future.result()
                partial_dfs.append(df)

        def process_df(df):
            for col in df.columns:
                if col_types[col]:
                    df[col] = df[col].apply(self._from_base64)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_df, df) for df in partial_dfs]
            for future in futures:
                future.result()
        # Combine all chunks
        data = pd.concat(partial_dfs, ignore_index=True)
        return data

    def query_df_sql(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询并返回DataFrame
        :param sql: SQL查询语句
        :return: 查询结果DataFrame
        """
        sql = sql.lower()
        if not sql.strip().startswith("select"):
            raise ValueError("只支持SELECT查询语句")
            
        # 提取FROM后的表名
        if len(sql.split(" from ")) < 2:
            raise ValueError("FROM语句缺失")
        table_name = sql.split("from")[1].strip().split("where")[0].strip()

        # 提取列名
        columns = []
        columns_part = sql.split("select")[1].strip().split("from")[0].strip()
        if columns_part == "*":
            columns = ["*"]
        else:
            if ' as ' in columns_part:
                columns = [columns_part]
            else:
                columns = [col.strip() for col in columns_part.split(",")]

        # 提取WHERE条件（如果有）
        condition = None
        if " where " in sql:
            condition = sql.split("where")[1].strip()
        
        # 使用query_df方法执行查询
        return self.query_df(table_name, columns=columns, condition=condition)
    
    def execute_sql(self, sql: str) -> any:
        return self.db.execute(sql)

    def drop_table(self, table_name: str):
        return self.db.drop_table(table_name)
    
    def create_table(self, table_name: str, columns: List[str]):
        return self.db.create_table(table_name, columns)

    def create_table_df(self, table_name: str, df: pd.DataFrame):
        df.head(0).write_database(table_name, self.engine.connect(), if_table_exists="replace")

    def _to_base64(self, entry: any):
        if entry is None:
            return None
        binary_data = pickle.dumps(entry)
        base64_str = base64.b64encode(binary_data).decode('utf-8')
        return "base64_encode::" + base64_str
    
    def _from_base64(self, entry: str):
        if entry is None:
            return None
        base64_str = entry.split("base64_encode::")[1]
        binary_data = base64.b64decode(base64_str)
        return pickle.loads(binary_data)

