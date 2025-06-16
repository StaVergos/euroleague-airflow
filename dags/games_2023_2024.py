import pendulum
import requests
from airflow.decorators import dag, task
from core.mongodb.mongo_service import (
    sanitize_id,
    games_2023_collection,
    games_2024_collection,
)
from pymongo.errors import BulkWriteError


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def euroleague_games_2023_2024():
    @task()
    def get_games_2023():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2023/games"
        )
        result_data = result_raw.json().get("data")
        if result_data:
            try:
                games_2023_documents = games_2023_collection.insert_many(
                    result_data, ordered=False
                )
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
            return games_2023_documents.acknowledged
        else:
            all_games_2023_documents = list(games_2023_collection.find())
            first_document = all_games_2023_documents[0]
            return sanitize_id(first_document)

    @task()
    def get_games_2024():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2024/games"
        )
        result_data = result_raw.json().get("data")
        if result_data:
            try:
                games_2024_documents = games_2024_collection.insert_many(
                    result_data, ordered=False
                )
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
            return games_2024_documents.acknowledged
        else:
            all_games_2024_documents = list(games_2024_collection.find())
            first_document = all_games_2024_documents[0]
            return sanitize_id(first_document)

    games_2023 = get_games_2023()  # noqa: F841
    games_2024 = get_games_2024()  # noqa: F841


euroleague_games_dag = euroleague_games_2023_2024()
