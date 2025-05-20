import pendulum
import requests
from airflow.decorators import dag, task


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def euroleague_games_2024_2025():
    @task()
    def get_games_2024():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2023/games"
        )
        result_data = result_raw.json().get("data")
        print(result_data)
        return result_data

    @task()
    def get_games_2025():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2024/games"
        )
        result_data = result_raw.json().get("data")
        print(result_data)
        return result_data

    games_2024 = get_games_2024()
    games_2025 = get_games_2025()


euroleague_dag = euroleague_games_2024_2025()
