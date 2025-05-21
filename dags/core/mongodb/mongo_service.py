from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")
db = client["euroleague"]


def sanitize_id(document: dict[str, any]) -> dict[str, any]:
    document["_id"] = str(document["_id"])
    return document
