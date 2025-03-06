import pandas as pd
from airflow.decorators import task
from constants import DATA_CONN_URL
from sqlalchemy import create_engine

import utils

data_conn = create_engine(DATA_CONN_URL, echo=False)


@task
def time_dimension():
    str_sql = """
    SELECT DISTINCT
        InvoiceID,
        InvoiceDate,
        YEAR(InvoiceDate) AS InvoiceDateYear,
        MONTH(InvoiceDate) AS InvoiceDateMonth,
        WEEK(InvoiceDate) AS InvoiceDateWeek,
        DAY(InvoiceDate) AS InvoiceDateDay,
        IF(
            (DayOfWeek(InvoiceDate) - 1) = 0,
            7,
            DayOfWeek(InvoiceDate) - 1
        ) As InvoiceDateDayOfWeek
    FROM
        staging
    ;
    """
    df = pd.read_sql(sql=str_sql, con=data_conn)
    if len(df.index) == 0:
        print("empty set")
        return
    row_count = utils.add_rows(df, "time_dimension", data_conn, "append")
    print("rows affected", row_count)
