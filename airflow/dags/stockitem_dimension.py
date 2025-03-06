import pandas as pd
from airflow.decorators import task
from constants import DATA_CONN_URL
from sqlalchemy import create_engine

import utils

data_conn = create_engine(DATA_CONN_URL, echo=False)


@task
def stockitem_dimension():
    str_sql = """
    SELECT DISTINCT
        StockItemID,
        StockItemName,
        StockGroupID,
        StockGroupName,
        Brand,
        Size,
        CostPrice
    FROM
        staging
    ;
    """
    df = pd.read_sql(sql=str_sql, con=data_conn)
    if len(df.index) == 0:
        print("empty set")
        return
    row_count = utils.add_rows(df, "stockitem_dimension", data_conn, "append")
    print("rows affected", row_count)
