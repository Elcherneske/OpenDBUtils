from abc import ABC, abstractmethod
from typing import List, Tuple, Any
import pandas as pd
import polars as pl

class DBInterface(ABC):
    """数据库操作的抽象接口类"""
    
    @abstractmethod
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: str = None):
        """初始化数据库连接参数"""
        pass

    @abstractmethod
    def _connect(self):
        """建立数据库连接"""
        pass
    
    @abstractmethod
    def execute(self, sql: str) -> any:
        """执行任意SQL语句"""
        pass

    @abstractmethod
    def create_table(self, table_name: str, columns: List[str]) -> None:
        """创建数据表"""
        pass

    @abstractmethod
    def insert_data(self, table_name: str, columns: List[str], values: List[Any]) -> None:
        """插入单条数据"""
        pass

    @abstractmethod
    def insert_df(self, data: pd.DataFrame|pl.DataFrame, table_name: str):
        """插入DataFrame"""
        pass

    @abstractmethod
    def select_data(self, table_name: str, columns: List[str] = ["*"], condition: str = None, limit: int = None, offset: int = None) -> List[Tuple]:
        """查询数据"""
        pass

    @abstractmethod
    def select_df(self, table_name: str, columns: List[str] = ["*"], condition: str = None, limit: int = None, offset: int = None) -> pd.DataFrame:
        """查询数据并转换为DataFrame"""
        pass

    @abstractmethod
    def count_data(self, table_name: str, condition: str = None) -> int:
        """查询数据数量"""
        pass

    @abstractmethod
    def delete_data(self, table_name: str, condition: str) -> None:
        """删除数据"""
        pass

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """删除表"""
        pass