import pendulum
from airflow.sdk import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example']
)
def own_tutorial_taskflow_api():
    """
    ### Own TaskFlow API Tutorial Documentation
    This is documentation of DAG in MarkDown format.
    """
    @task()
    def hello():
        """
        ### Prints "Hello world!"
        """
        print('Hello World!')

    @task()
    def extract():
        """
        # Extract task
        Extracts data sample
        """
        extract_example = {
            "payload": [1, 0, 1, 0]
        }
        return extract_example

    @task(multiple_outputs=False)
    def transform(extract_example: dict):
        """
        # Transform task
        Transforms sample data
        """
        decoded_msg = ""
        for bit in extract_example['payload']:
            if bit==1: decoded_msg+='a'
            else: decoded_msg+='b'
        return {"decoded_msg": decoded_msg}

    @task()
    def load(decoded_msg):
        """
        # Load task
        Loads data to storage, stdout
        """
        print(f"Decoded message: {decoded_msg}")

    hello()
    extract_example = extract()
    decoded_msg = transform(extract_example)
    load(decoded_msg)

# Call function for register dag in airflow
own_tutorial_taskflow_api()
