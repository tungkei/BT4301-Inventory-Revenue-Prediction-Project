import mysql.connector
import pandas as pd
from airflow.decorators import task
from airflow.models import Variable
from constants import (
    DATA_CONN_URL,
    DATAWAREHOUSE_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_USER,
    PERIOD_OFFSET,
    WWI_CONN_URL,
)
from pendulum import Date
from sqlalchemy import create_engine

wwi_conn = create_engine(WWI_CONN_URL, echo=False)
data_conn = create_engine(DATA_CONN_URL, echo=False)


@task
def bootstrap_staging(COUNTER_VARIABLE_KEY: str, PERIOD_START: Date, PERIOD_END: Date):
    period = int(Variable.get(COUNTER_VARIABLE_KEY))
    offset = period * PERIOD_OFFSET
    period_start = PERIOD_START + offset
    period_end = PERIOD_END + offset
    str_sql = f"""
    SELECT
        InvoiceLines.InvoiceLineID,
        InvoiceLines.InvoiceID,
        InvoiceLines.StockItemID,
        InvoiceLines.Quantity,
        InvoiceLines.UnitPrice,

        Invoices.CustomerID,
        Invoices.InvoiceDate,

        StockItems.StockItemName,
        StockItems.Brand,
        StockItems.Size,

        StockGroups.StockGroupID,
        StockGroups.StockGroupName,

        StockItemHoldings.LastCostPrice AS CostPrice,

        Customers.CustomerName,
        Customers.CustomerCategoryID,

        CustomerCategories.CustomerCategoryName
    FROM
        InvoiceLines
        INNER JOIN Invoices ON InvoiceLines.InvoiceID = Invoices.InvoiceID
        INNER JOIN StockItems ON InvoiceLines.StockItemID = StockItems.StockItemID
        INNER JOIN StockItemStockGroups ON StockItems.StockItemID = StockItemStockGroups.StockItemID
        INNER JOIN StockGroups ON StockItemStockGroups.StockGroupID = StockGroups.StockGroupID
        INNER JOIN StockItemHoldings ON StockItems.StockItemID = StockItemHoldings.StockItemID
        INNER JOIN Customers ON Invoices.CustomerID = Customers.CustomerID
        INNER JOIN CustomerCategories ON Customers.CustomerCategoryID = CustomerCategories.CustomerCategoryID
    WHERE
        Invoices.InvoiceDate >= "{period_start.to_date_string()}" AND
        Invoices.InvoiceDate < "{period_end.to_date_string()}"
    ;
    """
    df = pd.read_sql(sql=str_sql, con=wwi_conn)
    row_count = df.to_sql(name="staging", con=data_conn, if_exists="replace")
    print("rows affected", row_count)
    print(str_sql)


@task
def drop_staging():
    db_datawarehouse = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        database=DATAWAREHOUSE_DATABASE,
    )
    cursor = db_datawarehouse.cursor()
    cursor.execute("DROP TABLE staging;")
    db_datawarehouse.commit()
    db_datawarehouse.close()


@task
def bootstrap_counter(COUNTER_VARIABLE_KEY: str):
    Variable.setdefault(COUNTER_VARIABLE_KEY, 0)


@task.short_circuit
def check_counter(COUNTER_VARIABLE_KEY: str, NUM_PERIODS: int):
    current_value = int(Variable.get(COUNTER_VARIABLE_KEY))
    print("counter current value:", current_value)
    return current_value < NUM_PERIODS


@task
def update_counter(COUNTER_VARIABLE_KEY: str):
    current_value = int(Variable.get(COUNTER_VARIABLE_KEY))
    Variable.update(COUNTER_VARIABLE_KEY, current_value + 1)
    print("counter updated")
