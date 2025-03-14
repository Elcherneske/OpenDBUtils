# OpenDBUtils
an open source utils for interacting with databases.

## Supported Databases
- [yes] MySQL
- [yes] PostgreSQL
- [yes] SQLite
- [no] MongoDB
- [no] Redis

additional databases will be supported in the future.

## Installation
1. pip install OpenDBUtils
```bash
pip install git+https://github.com/Elcherneske/OpenDBUtils.git
```

2. clone the repository
```bash
git clone https://github.com/Elcherneske/OpenDBUtils.git
```

## Usage
```python
from OpenDBUtils import DBUtils

# initialize the DBUtils object
postgres_db_utils = DBUtils(
    db_name="postgres",
    user="xxx",
    password="xxx",
    host="localhost",
    port="5432",
    db_instance="postgresql"
)

mysql_db_utils = DBUtils(
    db_name="mysql",
    user="xxx",
    password="xxx",
    host="localhost",
    port="3306",
    db_instance="mysql"
)

sqlite_db_utils = DBUtils(
    db_name="sqlite_file_path",
    db_instance="sqlite"
)

# Create a table
db_utils.create_table(
    "users",
    [
        "id INTEGER",
        "name VARCHAR(100)",
        "email VARCHAR(100)",
        "age INTEGER"
    ]
)

# Create and store a simple DataFrame
import polars as pl
import pandas as pd
import time

# Create a pandas DataFrame
users_df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['张三', '李四', '王五', '赵六', '钱七'],
    'email': ['zhang@example.com', 'li@example.com', 'wang@example.com', 
             'zhao@example.com', 'qian@example.com'],
    'age': [25, 30, 35, 28, 40]
})

# Convert to polars DataFrame and store
users_pl_df = pl.from_pandas(users_df)
db_utils.store_df(users_pl_df, "users", table_replace=False)

# Create and store complex data with nested structures
complex_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Project A', 'Project B', 'Project C'],
    'data_list': [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    'data_dict': [{'a': 1, 'b': 2}, {'c': 3, 'd': 4}, {'e': 5, 'f': 6}]
})

# Store complex data using store_dict
db_utils.store_dict({'complex_data': complex_df}, table_replace=True) # set table_replace=True to initialize the table if it not exists

# Query data with condition
users_result = db_utils.query_df("users", condition="age > 30")
print("Users with age > 30:")
print(users_result)

# Query complex data and check serialization
complex_result = db_utils.query_df("complex_data")
print("Complex data query result:")
print(complex_result)
print(f"Data list example: {complex_result['data_list'].iloc[0]}")
print(f"Data dict example: {complex_result['data_dict'].iloc[0]}")

# Use SQL query
sql_result = db_utils.query_df_sql("SELECT name, age FROM users WHERE age > 30")
print("SQL query result:")
print(sql_result)

# Count data
count = db_utils.db.count_data("users", condition="age > 30")
print(f"Number of users with age > 30: {count}")

# Delete data
db_utils.db.delete_data("users", "age = 30")
print("After deleting users with age = 30:")
remaining_users = db_utils.query_df("users")
print(remaining_users)

# Clean up
db_utils.drop_table("users")
db_utils.drop_table("complex_data")

```


