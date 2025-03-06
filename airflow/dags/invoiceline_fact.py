import pandas as pd
from airflow.decorators import task
from constants import DATA_CONN_URL
from sqlalchemy import create_engine

import utils

data_conn = create_engine(DATA_CONN_URL, echo=False)


@task
def invoiceline_fact():
    str_sql = """
    SELECT DISTINCT
        InvoiceLineID,
        InvoiceID,
        StockItemID,
        CustomerID,
        Quantity,
        UnitPrice
    FROM
        staging
    ;
    """
    df = pd.read_sql(sql=str_sql, con=data_conn)
    if len(df.index) == 0:
        print("empty set")
        return
    row_count = utils.add_rows(df, "invoiceline_fact", data_conn, "append")
    print("rows affected", row_count)
