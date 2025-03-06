import mysql.connector
from airflow.decorators import task
from airflow.models import Variable
from constants import (
    DATAWAREHOUSE_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_USER,
)


@task
def bootstrap_relations(COUNTER_VARIABLE_KEY: str):
    counter = int(Variable.get(COUNTER_VARIABLE_KEY))
    if counter > 0:
        print("counter > 0")
        return
    db_datawarehouse = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        database=DATAWAREHOUSE_DATABASE,
    )
    cursor = db_datawarehouse.cursor()
    # fmt: off
    cursor.execute("ALTER TABLE customer_dimension ADD PRIMARY KEY (CustomerID);")
    cursor.execute("ALTER TABLE stockitem_dimension ADD PRIMARY KEY (StockItemID, StockGroupID);")
    cursor.execute("ALTER TABLE time_dimension ADD PRIMARY KEY (InvoiceID);")
    cursor.execute("ALTER TABLE invoiceline_fact ADD PRIMARY KEY (InvoiceLineID);")

    cursor.execute("ALTER TABLE invoiceline_fact ADD FOREIGN KEY (CustomerID) REFERENCES customer_dimension(CustomerID);")
    cursor.execute("ALTER TABLE invoiceline_fact ADD FOREIGN KEY (StockItemID) REFERENCES stockitem_dimension(StockItemID);")
    cursor.execute("ALTER TABLE invoiceline_fact ADD FOREIGN KEY (InvoiceID) REFERENCES time_dimension(InvoiceID);")
    # fmt: on
    db_datawarehouse.commit()
    db_datawarehouse.close()
