import os

import pendulum
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def test_hhru_env():
    @task()
    def echo_env():
        """
        ### Checks if hhru envs available
        """
        CLIENT_ID = os.getenv("CLIENT_ID", "")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
        print(len(CLIENT_ID), len(CLIENT_SECRET))
        print(CLIENT_ID[:3], CLIENT_SECRET[:3])

    echo_env()


test_hhru_env()
