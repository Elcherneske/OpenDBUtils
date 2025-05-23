import sqlite3
import io
from typing import List, Tuple, Any
import pandas as pd
import polars as pl
from .DBInterface import DBInterface
from sqlalchemy import create_engine


class SQLiteUtils(DBInterface):
    def __init__(self, dbname: str, user: str = None, password: str = None, host: str = None, port: str = None):
        """
        初始化SQLite数据库连接参数
        :param dbname: 数据库文件路径
        :param user: 不适用于SQLite (保留参数以符合接口)
        :param password: 不适用于SQLite (保留参数以符合接口)
        :param host: 不适用于SQLite (保留参数以符合接口)
        :param port: 不适用于SQLite (保留参数以符合接口)
        """
        super().__init__(dbname, user, password, host, port)
        self.dbname = dbname
        # SQLite不需要用户名、密码、主机和端口，但为了保持接口一致性，保留这些参数
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def _connect(self):
        """建立数据库连接"""
        try:
            conn = sqlite3.connect(self.dbname)
            # 启用外键约束
            conn.execute("PRAGMA foreign_keys = ON")
            # 设置行工厂以返回字典
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            raise Exception(f"数据库连接失败: {str(e)}")

    def execute(self, sql: str) -> any:
        """
        执行任意SQL语句
        :param sql: SQL语句
        :return: 执行结果
        """
        try:
            conn, cursor = self._connect()
            cursor.execute(sql)
            conn.commit()
            return cursor.fetchall()
        except Exception as e:
            conn.rollback()
            raise Exception(f"执行SQL语句失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def create_table(self, table_name: str, columns: List[str]) -> None:
        """
        创建数据表
        :param table_name: 表名
        :param columns: 列定义列表，例如 ["id INTEGER PRIMARY KEY", "name TEXT"]
        """
        try:
            conn, cursor = self._connect()
            columns_str = ", ".join(columns)
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
            cursor.execute(create_table_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"创建表失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def insert_data(self, table_name: str, columns: List[str], values: List[Any]) -> None:
        """
        插入数据
        :param table_name: 表名
        :param columns: 列名列表
        :param values: 值列表
        """
        try:
            conn, cursor = self._connect()
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(values))
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            cursor.execute(insert_query, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"插入数据失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def insert_df(self, data: pd.DataFrame | pl.DataFrame, table_name: str):
        """
        插入DataFrame数据
        :param data: 数据框
        :param table_name: 表名
        """
        try:
            if isinstance(data, pl.DataFrame):
                data = data.to_pandas()
            engine = create_engine(f"sqlite:///{self.dbname}")
            data.to_sql(table_name, engine, if_exists='append', index=False)
        except Exception as e:
            raise Exception(f"插入DataFrame数据失败: {str(e)}")

    def select_data(self, table_name: str, columns: List[str] = ["*"], condition: str = None, 
                   limit: int = None, offset: int = None) -> List[Tuple]:
        """
        查询数据
        :param table_name: 表名
        :param columns: 要查询的列名列表
        :param condition: WHERE条件语句
        :param limit: 限制查询结果数量
        :param offset: 偏移量
        :return: 查询结果列表
        """
        try:
            conn, cursor = self._connect()
            columns_str = ", ".join(columns)
            select_query = f"SELECT {columns_str} FROM {table_name}"
            if condition:
                select_query += f" WHERE {condition}"
            if limit:
                select_query += f" LIMIT {limit}"
            if offset:
                select_query += f" OFFSET {offset}"
            
            cursor.execute(select_query)
            return cursor.fetchall()
        except Exception as e:
            raise Exception(f"查询数据失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def select_df(self, table_name: str, columns: List[str] = ["*"], condition: str = None, 
                 limit: int = None, offset: int = None) -> pd.DataFrame:
        """
        查询数据并转换为DataFrame
        :param table_name: 表名
        :param columns: 要查询的列名列表
        :param condition: WHERE条件语句
        :param limit: 限制查询结果数量
        :param offset: 偏移量
        :return: 查询结果DataFrame
        """
        try:
            conn, cursor = self._connect()
            columns_str = ", ".join(columns)
            select_query = f"SELECT {columns_str} FROM {table_name}"
            if condition:
                select_query += f" WHERE {condition}"
            if limit:
                select_query += f" LIMIT {limit}"
            if offset:
                select_query += f" OFFSET {offset}"
            
            # 使用pandas的read_sql_query直接读取为DataFrame
            return pd.read_sql_query(select_query, conn)
        except Exception as e:
            raise Exception(f"查询数据失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def count_data(self, table_name: str, condition: str = None) -> int:
        """
        查询数据数量
        :param table_name: 表名
        :param condition: WHERE条件语句
        :return: 数据数量
        """
        try:
            conn, cursor = self._connect()
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            if condition:
                count_query += f" WHERE {condition}"
            cursor.execute(count_query)
            return cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"查询数据数量失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def delete_data(self, table_name: str, condition: str) -> None:
        """
        删除数据
        :param table_name: 表名
        :param condition: WHERE条件语句
        """
        try:
            conn, cursor = self._connect()
            delete_query = f"DELETE FROM {table_name} WHERE {condition}"
            cursor.execute(delete_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"删除数据失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def drop_table(self, table_name: str) -> None:
        """
        删除表
        :param table_name: 表名
        """
        try:
            conn, cursor = self._connect()
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"删除表失败: {str(e)}")
        finally:
            cursor.close()
            conn.close() 