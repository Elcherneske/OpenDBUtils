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
import numpy as np

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

    def store_df(self, data: pl.DataFrame | pd.DataFrame, table_name: str, chunk_size: int = 2048, max_workers: int = 8, table_replace: bool = False, encode: bool = True):
        if data is None:
            return
        if isinstance(data, pl.DataFrame):
            data = data.to_pandas()
        data = DataFrameUtils(data).encode(encode)
        data = pl.from_pandas(data,include_index=True)
        if table_replace:
            data.head(0).write_database(table_name, self.engine.connect(), if_table_exists="replace")
        num_chunks = (len(data) + chunk_size - 1) // chunk_size  # 计算总块数
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.db.insert_df, data.slice(i * chunk_size, chunk_size), table_name) for i in range(num_chunks)]
            for future in futures:
                future.result()

    def store_dict(self, datas: dict[str, pd.DataFrame], chunk_size: int = 2048, max_workers: int = 8, table_replace: bool = False, encode: bool = True):
        for key, data in datas.items():
            self.store_df(data, key, chunk_size, max_workers, table_replace, encode)

    def query_df(self, table_name: str, columns: List[str] = ["*"], condition: str = None, limit: int = None, chunk_size: int = 2048, max_workers: int = 8) -> pd.DataFrame:
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

        def process_df(df: pd.DataFrame):
            df = DataFrameUtils(df).decode()
            return df

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_df, df) for df in partial_dfs]
            for future in futures:
                future.result()
        # Combine all chunks
        data = pd.concat(partial_dfs, ignore_index=True)
        data = data.set_index(data.columns[0])
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


class DataFrameUtils:
    def __init__(self, data: pd.DataFrame):
        self.data = data
    
    def encode(self, encode: bool = True):
        if self.data is None or len(self.data) == 0:
            return None
        col_types = self._check_column_types(self.data)
        for col in self.data.columns:
            if not col in col_types:
                raise ValueError(f"Column {col} not found in DataFrame")
            if not encode and col_types[col] != 0:
                raise ValueError(f"Column {col} is not a common column")
            if col_types[col] == 1 and encode:
                self.data[col] = self.data[col].apply(self._to_base64)
            elif col_types[col] == 2 and encode:
                self.data[col] = self.data[col].apply(self._to_int)
            elif col_types[col] == 3 and encode:
                self.data[col] = self.data[col].apply(self._to_float)
            elif col_types[col] == -1 and encode:
                self.data[col] = self.data[col].apply(self._to_pickle_base64)
        return self.data

    def decode(self):
        if self.data is None or len(self.data) == 0:
            return None
        for col in self.data.columns:
            filtered_df = self.data[self.data[col].notna()]
            if len(filtered_df) == 0:
                continue
            if isinstance(filtered_df[col].iloc[0], str) and filtered_df[col].iloc[0].startswith("base64_encode::"):
                self.data[col] = self.data[col].apply(self._from_base64)
            elif isinstance(filtered_df[col].iloc[0], str) and filtered_df[col].iloc[0].startswith("base64_pickle_encode::"):
                self.data[col] = self.data[col].apply(self._from_pickle_base64)
        return self.data

    def _to_pickle_base64(self, entry: any):
        if entry is None:
            return None
        binary_data = pickle.dumps(entry)
        base64_str = base64.b64encode(binary_data).decode('utf-8')
        return "base64_pickle_encode::" + base64_str
    
    def _from_pickle_base64(self, entry: str):
        if entry is None:
            return None
        base64_str = entry.split("base64_pickle_encode::")[1]
        binary_data = base64.b64decode(base64_str)
        return pickle.loads(binary_data)
    
    def _to_base64(self, entry: any):
        if entry is None:
            return None
        base64_str = base64.b64encode(entry).decode('utf-8')
        return "base64_encode::" + base64_str
    
    def _from_base64(self, entry: str):
        if entry is None:
            return None
        base64_str = entry.split("base64_encode::")[1]
        return base64.b64decode(base64_str)
    
    def _to_int(self, entry: any):
        if entry is None:
            return None
        return int(entry)
    
    def _to_float(self, entry: any):
        if entry is None:
            return None
        return float(entry)
    
    def _check_column_types(self, data: pd.DataFrame) -> dict[str, int]:
        """
        检查DataFrame中各列的数据类型并返回类型标识
        
        参数:
            data (pandas.DataFrame): 要检查的数据框
            
        返回:
            dict: 键为列名，值为类型标识的字典
                0: str, int, float, 全为空
                1: bytes
                2: numpy.int
                3: numpy.float
               -1: 其他类型
        """
        col_types = {}
        if len(data) == 0:
            return col_types
        for col in data.columns:
            filtered_df = data[data[col].notna()]
            if len(filtered_df) == 0:
                col_types[col] = 0 
                continue
            
            sample_value = filtered_df[col].iloc[0]
            if isinstance(sample_value, (str, int, float)):
                col_types[col] = 0
            elif isinstance(sample_value, bytes):
                col_types[col] = 1
            elif isinstance(sample_value, (np.integer)):
                col_types[col] = 2
            elif isinstance(sample_value, (np.floating)):
                col_types[col] = 3
            else:
                col_types[col] = -1
            
        return col_types
