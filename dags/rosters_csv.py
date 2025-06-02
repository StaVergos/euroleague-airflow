from pymongo import MongoClient
import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import logging
import pendulum
from airflow.decorators import dag, task


MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "euroleague"


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def season_rosters_2023_2024():
    @task
    def upload_players():
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' already exists.")
        except Exception:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")

        client = MongoClient("mongodb://mongodb:27017/")
        db = client["euroleague"]

        players_2023_collection = db.players_2023
        players_2024_collection = db.players_2024
        all_players_2023 = list(players_2023_collection.find())
        all_players_2024 = list(players_2024_collection.find())

        players_2023 = []
        for player in all_players_2023:
            if (
                not player.get("person_isReferee")
                and player.get("typeName") == "Player"
            ):
                player = {
                    "person_code": player.get("person_code"),
                    "person_name": player.get("person_name"),
                    "person_height": player.get("person_height"),
                    "person_weight": player.get("person_weight"),
                    "person_birthDate": player.get("person_birthDate"),
                    "position": player.get("position"),
                    "positionName": player.get("positionName"),
                    "club_code": player.get("club_code"),
                    "club_name": player.get("club_name"),
                    "club_abbreviatedName": player.get("club_abbreviatedName"),
                    "club_editorialName": player.get("club_editorialName"),
                    "club_tvCode": player.get("club_tvCode"),
                    "season_name": player.get("season_name"),
                }
                players_2023.append(player)

        players_2024 = []
        for player in all_players_2024:
            if (
                not player.get("person_isReferee")
                and player.get("typeName") == "Player"
            ):
                player = {
                    "person_code": player.get("person_code"),
                    "person_name": player.get("person_name"),
                    "person_height": player.get("person_height"),
                    "person_weight": player.get("person_weight"),
                    "person_birthDate": player.get("person_birthDate"),
                    "position": player.get("position"),
                    "positionName": player.get("positionName"),
                    "club_code": player.get("club_code"),
                    "club_name": player.get("club_name"),
                    "club_abbreviatedName": player.get("club_abbreviatedName"),
                    "club_editorialName": player.get("club_editorialName"),
                    "club_tvCode": player.get("club_tvCode"),
                    "season_name": player.get("season_name"),
                }
                players_2024.append(player)

        players_23_df = pd.DataFrame.from_records(players_2023)
        players_23_df.to_csv("players_23.csv")

        players_24_df = pd.DataFrame.from_records(players_2024)
        players_24_df.to_csv("players_24.csv")

        try:
            s3.upload_file("players_23.csv", BUCKET_NAME, "players_23.csv")
            print("Uploaded players_23.csv to euroleague bucket")
        except ClientError as e:
            logging.error(e)

        try:
            s3.upload_file("players_24.csv", BUCKET_NAME, "players_24.csv")
            print("Uploaded players_24.csv to euroleague bucket")
        except ClientError as e:
            logging.error(e)

    task_apload = upload_players()  # noqa: F841


players_rosters = season_rosters_2023_2024()
