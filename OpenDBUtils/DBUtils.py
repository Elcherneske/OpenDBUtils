from PostgreUtils import PostgreUtils
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import time
from sqlalchemy import create_engine
import polars as pl
import base64
import pickle


class DBUtils:
    def __init__(self, db_name, user, password, host, port, db_instance="postgres"):
        if db_instance == "postgres":
            self.db = PostgreUtils(db_name, user, password, host, port)
            self.engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
        else:
            raise ValueError(f"Unsupported database instance: {db_instance}")

    def store_df(self, data: pl.DataFrame, table_name: str, chunk_size: int = 2048, max_workers: int = 8,
                 table_replace: bool = False):
        if len(data) == 0:
            return
        if table_replace:
            data.head(0).write_database(table_name, self.engine.connect(), if_table_exists="replace")
        num_chunks = (len(data) + chunk_size - 1) // chunk_size  # 计算总块数
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.db.copy_insert, data.slice(i * chunk_size, chunk_size), table_name) for i in
                       range(num_chunks)]
            for future in futures:
                future.result()

    def store_dict(self, datas: dict[str, pd.DataFrame], chunk_size: int = 2048, max_workers: int = 8,
                   table_replace: bool = False):
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

    def query_data(self, table_name: str, condition: str = None, limit: int = None, chunk_size: int = 2048,
                   max_workers: int = 8):
        single_data = self.db.select_data_to_df(table_name, condition=condition, limit=1)
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
                    self.db.select_data_to_df,
                    table_name,
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

    def execute_query(self, sql: str) -> pd.DataFrame:
        return self.db.execute_query(sql)

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

