import pendulum
import requests
from airflow.decorators import dag, task
from core.mongodb.mongo_service import db, sanitize_id
from pymongo.errors import BulkWriteError

games_2023_collection = db.games_2023
games_2023_collection.create_index("gameCode", unique=True)
games_2024_collection = db.games_2024
games_2024_collection.create_index("gameCode", unique=True)


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
        games_to_be_added = []
        for game in result_data:
            game_code = game.get("gameCode")
            query = {"gameCode": game_code}
            existing_game = games_2023_collection.find_one(query)
            if not existing_game:
                games_to_be_added.append(game)
        if games_to_be_added:
            try:
                games_2023_documents = games_2023_collection.insert_many(
                    games_to_be_added, ordered=False
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
        games_to_be_added = []
        for game in result_data:
            game_code = game.get("gameCode")
            query = {"gameCode": game_code}
            existing_game = games_2024_collection.find_one(query)
            if not existing_game:
                games_to_be_added.append(game)
        if games_to_be_added:
            try:
                games_2024_documents = games_2024_collection.insert_many(
                    games_to_be_added, ordered=False
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

    games_2024 = get_games_2023()
    games_2025 = get_games_2024()


euroleague_dag = euroleague_games_2023_2024()
