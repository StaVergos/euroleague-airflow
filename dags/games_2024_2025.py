import pendulum
import requests
from airflow.decorators import dag, task
import datetime
from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")
db = client["euroleague"]
games_2024_collection = db.games_2024
games_2025_collection = db.games_2025


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
        games_to_be_added = []
        for game in result_data:
            game_code = game.get("gameCode")
            query = {"gameCode": game_code}
            existing_game = games_2024_collection.find(query)
            if not existing_game:
                games_to_be_added.append(game)
            if games_to_be_added:
                games_2024_documents = games_2024_collection.insert_many(games_to_be_added)
                return games_2024_documents
            else:
                all_games_2024_documents = games_2024_collection.find()
                first_document = all_games_2024_documents[0]
                first_document["_id"] = str(first_document["_id"])
                return first_document

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
