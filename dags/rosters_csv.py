import pandas as pd
from botocore.exceptions import ClientError
import logging
import pendulum
from airflow.decorators import dag, task
from core.minio.minio_service import s3_client, BUCKET_NAME
from core.mongodb.mongo_service import (
    sanitize_id,
    players_2023_collection,
    players_2024_collection,
    roster_2023_collection,
    roster_2024_collection,
)
from pymongo.errors import BulkWriteError


@dag(
    dag_id="season_rosters_2023_2024",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def season_rosters_2023_2024():
    @task
    def upload_players():
        s3 = s3_client()
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' already exists.")
        except Exception:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")

        all_players_2023 = list(players_2023_collection.find())
        all_players_2024 = list(players_2024_collection.find())

        players_2023 = []
        for player in all_players_2023:
            if (
                not player.get("person_is_referee")
                and player.get("type_name") == "Player"
            ):
                player = {
                    "person_code": player.get("person_code"),
                    "person_name": player.get("person_name"),
                    "person_height": player.get("person_height"),
                    "person_weight": player.get("person_weight"),
                    "person_birth_date": player.get("person_birthDate"),
                    "position": player.get("position"),
                    "position_name": player.get("positionName"),
                    "club_code": player.get("club_code"),
                    "club_name": player.get("club_name"),
                    "club_abbreviated_name": player.get("club_abbreviatedName"),
                    "club_editorial_name": player.get("club_editorialName"),
                    "club_tv_code": player.get("club_tvCode"),
                    "season_name": player.get("season_name"),
                }
                players_2023.append(player)

        players_2024 = []
        for player in all_players_2024:
            if (
                not player.get("person_is_referee")
                and player.get("type_name") == "Player"
            ):
                player = {
                    "person_code": player.get("person_code"),
                    "person_name": player.get("person_name"),
                    "person_height": player.get("person_height"),
                    "person_weight": player.get("person_weight"),
                    "person_birth_date": player.get("person_birthDate"),
                    "position": player.get("position"),
                    "position_name": player.get("positionName"),
                    "club_code": player.get("club_code"),
                    "club_name": player.get("club_name"),
                    "club_abbreviated_name": player.get("club_abbreviatedName"),
                    "club_editorial_name": player.get("club_editorialName"),
                    "club_tv_code": player.get("club_tvCode"),
                    "season_name": player.get("season_name"),
                }
                players_2024.append(player)

        if players_2023:
            try:
                roster_2023_collection.insert_many(players_2023, ordered=False)
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
        else:
            all_players_2023_documents = list(roster_2023_collection.find())
            first_document = all_players_2023_documents[0]
            return sanitize_id(first_document)

        if players_2024:
            try:
                roster_2024_collection.insert_many(players_2024, ordered=False)
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
        else:
            all_players_2024_documents = list(roster_2024_collection.find())
            first_document = all_players_2024_documents[0]
            return sanitize_id(first_document)

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
