from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")
db = client["euroleague"]


games_2023_collection = db.games_2023
games_2023_collection.create_index("gameCode", unique=True)
games_2024_collection = db.games_2024
games_2024_collection.create_index("gameCode", unique=True)

players_2023_collection = db.players_2023
players_2023_collection.create_index("person_code", unique=True)
players_2024_collection = db.players_2024
players_2024_collection.create_index("person_code", unique=True)


roster_2023_collection = db.players_2023_roster
roster_2023_collection.create_index("person_code", unique=True)
roster_2024_collection = db.players_2024_roster
roster_2024_collection.create_index("person_code", unique=True)


def sanitize_id(document: dict[str, any]) -> dict[str, any]:
    document["_id"] = str(document["_id"])
    return document
