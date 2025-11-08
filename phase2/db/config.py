"""
Configuration and connection utility for MongoDB.
How to start your local MongoDB instance using Docker:
    docker run -d -p 27017:27017 --name mongodb -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=adminpass mongo:latest
How to start locally in terminal:
    MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=adminpass mongo:latest
"""

from pymongo import MongoClient, ConnectionFailure


def connect_to_mongo():
    """
    Connect to the local MongoDB instance running in Docker.
    Returns the client and the database object.
    Example usage: client, db = connect_to_mongo()
    """

    uri = "mongodb://admin:adminpass@localhost:27017/"
    db_name = "mongodb"

    try:
        client = MongoClient(uri)
        client.admin.command("ping")
        print("[MongoDB] Connected successfully.")

        db = client[db_name]
        print(f"Using database: {db_name}")
        return client, db

    except ConnectionFailure as e:
        print("[MongoDB] Connection failed:", e)
        raise
