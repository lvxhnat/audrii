import os
import json
import certifi
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

class MongoDBClient:

    def __init__(self,):
        # Reference the installed Certificate Authority bundle for verification
        self.__mongodb_cluster = MongoClient(
            os.environ['MONGODB_HOST_URL'], tlsCAFile=certifi.where())
        self.__mongodb_db = self.__mongodb_cluster[os.environ['MONGODB_DB_NAME']]

    def _get_collection(self, collection_name: str):
        collection = self.__mongodb_db[collection_name]
        return collection

    def get_all_items(self, collection_name: str):
        query_objects = self._get_collection(
            collection_name=collection_name).find()
        return list(query_objects)

    def insert_dataframe(self, df: pd.DataFrame, collection_name):
        try:
            collection = self._get_collection(collection_name=collection_name)
            records = json.loads(df.T.to_json()).values()
            insert_operations = [UpdateOne({"_id": index},
                                           {'$set': record}, upsert=True) for index, record in enumerate(records)]
            collection.bulk_write(insert_operations)
            return True
        except Exception as e:
            print(e)
            return False
        
mongodb_client = MongoDBClient()