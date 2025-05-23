from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")
db = client["euroleague"]


def sanitize_id(document: dict[str, any]) -> dict[str, any]:
    document["_id"] = str(document["_id"])
    return document


def parse_dict(init, lkey=""):
    ret = {}
    for rkey, val in init.items():
        key = lkey + rkey
        if isinstance(val, dict):
            ret.update(parse_dict(val, key + "."))
        else:
            ret[key] = val
    return ret
