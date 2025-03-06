from pendulum import duration

MYSQL_HOST = "mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"

CONN_URL = "mysql://root:password@mysql:3306"

WWI_DATABASE = "wwi"
WWI_CONN_URL = f"{CONN_URL}/{WWI_DATABASE}"

DATAWAREHOUSE_DATABASE = "wwi_datawarehouse"
DATA_CONN_URL = f"{CONN_URL}/{DATAWAREHOUSE_DATABASE}"

PERIOD_OFFSET = duration(months=1)
