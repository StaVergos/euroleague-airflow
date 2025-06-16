import pendulum
import requests
import logging
from airflow.decorators import dag, task
import pandas as pd
from botocore.exceptions import ClientError
from core.minio.minio_service import s3_client, BUCKET_NAME
from core.mongodb.mongo_service import (
    sanitize_id,
    games_2023_collection,
    games_2024_collection,
    players_2023_collection,
    players_2024_collection,
    roster_2023_collection,
    roster_2024_collection,
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

    @task()
    def get_players_2023():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2023/people"
        )
        result_data = result_raw.json().get("data")
        flat_data = []
        for player in result_data:
            player = {
                "person_code": player["person"]["code"],
                "person_name": player["person"]["name"],
                "person_alias": player["person"]["alias"],
                "person_aliasRaw": player["person"]["aliasRaw"],
                "person_passportName": player["person"]["passportName"],
                "person_passportSurname": player["person"]["passportSurname"],
                "person_jerseyName": player["person"]["jerseyName"],
                "person_abbreviatedName": player["person"]["abbreviatedName"],
                "person_country_code": (
                    (player.get("person") or {}).get("country") or {}
                ).get("code", None),
                "person_country_name": (
                    (player.get("person") or {}).get("country") or {}
                ).get("name", None),
                "person_height": player["person"]["height"],
                "person_weight": player["person"]["weight"],
                "person_birthDate": player["person"]["birthDate"],
                "person_birthCountry_code": (
                    (player.get("person") or {}).get("birthCountry") or {}
                ).get("code", None),
                "person_birthCountry_name": (
                    (player.get("person") or {}).get("birthCountry") or {}
                ).get("name", None),
                "person_twitterAccount": player["person"]["twitterAccount"],
                "person_instagramAccount": player["person"]["instagramAccount"],
                "person_facebookAccount": player["person"]["facebookAccount"],
                "person_is_referee": player["person"]["isReferee"],
                "person_images": player["person"]["images"],
                "type": player["type"],
                "type_name": player["typeName"],
                "active": player["active"],
                "startDate": player["startDate"],
                "endDate": player["endDate"],
                "order": player["order"],
                "dorsal": player["dorsal"],
                "dorsalRaw": player["dorsalRaw"],
                "position": player["position"],
                "positionName": player["positionName"],
                "lastTeam": player["lastTeam"],
                "externalId": player["externalId"],
                "images": player["images"],
                "club_code": player["club"]["code"],
                "club_name": player["club"]["name"],
                "club_abbreviatedName": player["club"]["abbreviatedName"],
                "club_editorialName": player["club"]["editorialName"],
                "club_tvCode": player["club"]["tvCode"],
                "club_isVirtual": player["club"]["isVirtual"],
                "club_images": player["club"]["images"]["crest"],
                "season_name": player["season"]["name"],
                "season_code": player["season"]["code"],
                "season_alias": player["season"]["alias"],
                "season_competitionCode": player["season"]["competitionCode"],
                "season_year": player["season"]["year"],
                "season_startDate": player["season"]["startDate"],
            }
            flat_data.append(player)
        if flat_data:
            try:
                players_2023_documents = players_2023_collection.insert_many(
                    flat_data, ordered=False
                )
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
            return players_2023_documents.acknowledged
        else:
            all_players_2023_documents = list(players_2023_collection.find())
            first_document = all_players_2023_documents[0]
            return sanitize_id(first_document)

    @task()
    def get_players_2024():
        result_raw = requests.get(
            "https://api-live.euroleague.net/v2/competitions/E/seasons/E2024/people"
        )
        result_data = result_raw.json().get("data")
        flat_data = []
        for player in result_data:
            player = {
                "person_code": player["person"]["code"],
                "person_name": player["person"]["name"],
                "person_alias": player["person"]["alias"],
                "person_aliasRaw": player["person"]["aliasRaw"],
                "person_passportName": player["person"]["passportName"],
                "person_passportSurname": player["person"]["passportSurname"],
                "person_jerseyName": player["person"]["jerseyName"],
                "person_abbreviatedName": player["person"]["abbreviatedName"],
                "person_country_code": (
                    (player.get("person") or {}).get("country") or {}
                ).get("code", None),
                "person_country_name": (
                    (player.get("person") or {}).get("country") or {}
                ).get("name", None),
                "person_height": player["person"]["height"],
                "person_weight": player["person"]["weight"],
                "person_birthDate": player["person"]["birthDate"],
                "person_birthCountry_code": (
                    (player.get("person") or {}).get("birthCountry") or {}
                ).get("code", None),
                "person_birthCountry_name": (
                    (player.get("person") or {}).get("birthCountry") or {}
                ).get("name", None),
                "person_twitterAccount": player["person"]["twitterAccount"],
                "person_instagramAccount": player["person"]["instagramAccount"],
                "person_facebookAccount": player["person"]["facebookAccount"],
                "person_is_referee": player["person"]["isReferee"],
                "person_images": player["person"]["images"],
                "type": player["type"],
                "type_name": player["typeName"],
                "active": player["active"],
                "startDate": player["startDate"],
                "endDate": player["endDate"],
                "order": player["order"],
                "dorsal": player["dorsal"],
                "dorsalRaw": player["dorsalRaw"],
                "position": player["position"],
                "positionName": player["positionName"],
                "lastTeam": player["lastTeam"],
                "externalId": player["externalId"],
                "images": player["images"],
                "club_code": player["club"]["code"],
                "club_name": player["club"]["name"],
                "club_abbreviatedName": player["club"]["abbreviatedName"],
                "club_editorialName": player["club"]["editorialName"],
                "club_tvCode": player["club"]["tvCode"],
                "club_isVirtual": player["club"]["isVirtual"],
                "club_images": player["club"]["images"]["crest"],
                "season_name": player["season"]["name"],
                "season_code": player["season"]["code"],
                "season_alias": player["season"]["alias"],
                "season_competitionCode": player["season"]["competitionCode"],
                "season_year": player["season"]["year"],
                "season_startDate": player["season"]["startDate"],
            }
            flat_data.append(player)
        if flat_data:
            try:
                players_2024_documents = players_2024_collection.insert_many(
                    flat_data, ordered=False
                )
            except BulkWriteError as e:
                return {
                    "message": f"No documents inserted. Exception: {str(e.details)}"
                }
            return players_2024_documents.acknowledged
        else:
            all_players_2024_documents = list(players_2024_collection.find())
            first_document = all_players_2024_documents[0]
            return sanitize_id(first_document)

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

    (
        get_games_2023()
        >> get_games_2024()
        >> get_players_2023()
        >> get_players_2024()
        >> upload_players()
    )


euroleague_dag = euroleague_games_2023_2024()
