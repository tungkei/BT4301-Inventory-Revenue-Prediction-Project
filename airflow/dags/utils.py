import pandas as pd
from sqlalchemy.dialects.mysql import insert


def add_rows(df: pd.DataFrame, name: str, con, if_exists):
    return df.to_sql(name=name, con=con, if_exists=if_exists, method=insert_method)


def insert_method(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)
