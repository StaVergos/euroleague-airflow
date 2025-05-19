import pendulum
import requests
from airflow.decorators import dag, task


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def euroleague_api_call():
    """DAG to get a request from Euroleague API."""

    @task()
    def get_clubs():
        result_raw = requests.get("https://api-live.euroleague.net/v2/clubs")
        result_data = result_raw.json().get("data")
        print(result_data)
        return result_data

    @task()
    def get_players():
        result_raw = requests.get("https://api-live.euroleague.net/v2/people")
        result_data = result_raw.json().get("data")
        print(result_data)
        return result_data

    all_clubs = get_clubs()
    all_players = get_players()


euroleague_dag = euroleague_api_call()
