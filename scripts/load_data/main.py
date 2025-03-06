import os
from glob import glob

import pandas as pd
from sqlalchemy import create_engine

CONN_URL = "mysql+mysqlconnector://root:password@mysql:3306"


def main():
    # bootstrap WWI database
    conn = create_engine(CONN_URL).connect()
    conn.exec_driver_sql("CREATE DATABASE IF NOT EXISTS `wwi`;")
    conn.exec_driver_sql("CREATE DATABASE IF NOT EXISTS `wwi_datawarehouse`;")
    conn.commit()

    # connection to WWI database
    wwi_conn = create_engine(f"{CONN_URL}/wwi")

    # csv => dataframe => MySQL
    for file in glob("./**/*.csv", recursive=True):
        fname = os.path.splitext(os.path.basename(file))[0]
        fname = fname.split(".")[-1]
        print(f"file: {file} , table: {fname}")
        if file == "./Sales/Sales.Invoices.csv":
            df = pd.read_csv(
                file,
                sep=";",
                on_bad_lines="warn",
                parse_dates=["InvoiceDate"],
                date_format="%d/%m/%Y",
            )
            df.to_sql(name=fname, con=wwi_conn, if_exists="fail")
        else:
            df = pd.read_csv(
                file,
                sep=";",
                on_bad_lines="warn",
            )
            df.to_sql(name=fname, con=wwi_conn, if_exists="fail")

    return


if __name__ == "__main__":
    main()
