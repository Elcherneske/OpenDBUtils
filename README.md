# OpenDBUtils
an open source utils for interacting with databases.

## Supported Databases
- [no] MySQL
- [yes] PostgreSQL
- [no] SQLite
- [no] MongoDB
- [no] Redis

additional databases will be supported in the future.

## Installation
1. clone the repository
```bash
git clone https://github.com/Elcherneske/OpenDBUtils.git
```

## Usage
```python
from OpenDBUtils import DBUtils

# initialize the DBUtils object
db_utils = DBUtils(
    db_name="postgres",
    user="xxx",
    password="xxx",
    host="localhost",
    port="5432",
    db_instance="postgresql"
)

# store a polars dataframe to the database
import polars as pl
import pandas as pd
pd_df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})
pl_df = pl.from_pandas(pd_df)
db_utils.store_df(pl_df, table_name="test_table", chunk_size=2048, max_workers=8, table_replace=True)

# store a dictionary of pandas dataframes to the database
dict = {
    "table_name1": pd_df1,
    "table_name2": pd_df2
}
db_utils.store_dict(dict, chunk_size=2048, max_workers=8, table_replace=True)

# query data from the database
pd_df = db_utils.query_data(table_name="test_table", condition="age > 25", limit=10, chunk_size=2048, max_workers=8)
print(pd_df)

# execute a sql query
pd_df = db_utils.execute_query(sql="SELECT * FROM test_table WHERE age > 25")
print(pd_df)

```


