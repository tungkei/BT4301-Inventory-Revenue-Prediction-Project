from datetime import datetime

from airflow.decorators import dag
from stockitem_dimension import stockitem_dimension
from time_dimension import time_dimension
from pendulum import date

from customer_dimension import customer_dimension
from invoiceline_fact import invoiceline_fact
from relations import bootstrap_relations
from staging import (
    bootstrap_counter,
    bootstrap_staging,
    check_counter,
    drop_staging,
    update_counter,
)


@dag(
    start_date=datetime(2024, 4, 5),
    schedule_interval="*/1 * * * *",
    catchup=False,
)
def simulation_etl_dag():
    COUNTER_VARIABLE_KEY = "simulation_etl_dag_period_counter"
    PERIOD_START = date(year=2015, month=6, day=1)
    PERIOD_END = date(year=2015, month=7, day=1)
    NUM_PERIODS = 12

    (
        bootstrap_counter(COUNTER_VARIABLE_KEY)
        >> check_counter(COUNTER_VARIABLE_KEY, NUM_PERIODS)
        >> bootstrap_staging(COUNTER_VARIABLE_KEY, PERIOD_START, PERIOD_END)
        >> [
            customer_dimension(),
            stockitem_dimension(),
            time_dimension(),
        ]
        >> invoiceline_fact()
        >> bootstrap_relations(COUNTER_VARIABLE_KEY)
        >> drop_staging()
        >> update_counter(COUNTER_VARIABLE_KEY)
    )


simulation_etl_dag()
