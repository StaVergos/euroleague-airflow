import pendulum
import requests
from airflow.decorators import dag, task
from core.mongodb.mongo_service import db, sanitize_id
from pymongo.errors import BulkWriteError

games_2023_collection = db.games_2023
games_2023_collection.create_index("gameCode", unique=True)
games_2024_collection = db.games_2024
games_2024_collection.create_index("gameCode", unique=True)

players_2023_collection = db.players_2023
players_2023_collection.create_index("person_code", unique=True)
players_2024_collection = db.players_2024
players_2024_collection.create_index("person_code", unique=True)


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
                "person_isReferee": player["person"]["isReferee"],
                "person_images": player["person"]["images"],
                "type": player["type"],
                "typeName": player["typeName"],
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
                "person_isReferee": player["person"]["isReferee"],
                "person_images": player["person"]["images"],
                "type": player["type"],
                "typeName": player["typeName"],
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

    games_2023 = get_games_2023()  # noqa: F841
    games_2024 = get_games_2024()  # noqa: F841
    players_2023 = get_players_2023()  # noqa: F841
    players_2024 = get_players_2024()  # noqa: F841


euroleague_dag = euroleague_games_2023_2024()
