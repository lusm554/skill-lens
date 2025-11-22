import pendulum
from airflow.sdk import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example']
)
def own_tutorial_taskflow_api():
    @task()
    def hello():
        print('Hello World!')
    hello()

# Call function for register dag in airflow
own_tutorial_taskflow_api()
