import os
import numpy as np
import pandas as pd
from datetime import datetime

import gcsfs
import pandas_gbq
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

import dotenv
dotenv.load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS']


class CloudUtility:

    def __init__(self, ):
        self.project_id = os.environ['GOOGLE_PROJECT_ID']
        self.bucket_name = os.environ['GOOGLE_BUCKET_NAME']
        self.credentials_url = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

        self.credentials = service_account.Credentials.from_service_account_file(
            self.credentials_url)

        self.client = bigquery.Client(
            credentials=self.credentials, project=self.project_id)
        pandas_gbq.context.credentials = self.credentials
        pandas_gbq.context.project = self.project_id

        self.fs = gcsfs.GCSFileSystem(
            project=self.project_id, token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    def get_files_with_prefix_from_gcs(self, bucket_name=os.environ['GOOGLE_BUCKET_NAME'], prefix="", delimiter=None):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        blob_names = [blob.name for blob in blobs]
        return blob_names

    def read_file_from_gcs(
            self,
            folder,
            sep="\t",
            format: str = "parquet"):
        read_base = "gs://" + self.bucket_name + "/"
        blobs = self.get_files_with_prefix_from_gcs(
            self.bucket_name, prefix=folder)

        data = []
        for blob in blobs:
            with self.fs.open(read_base + blob) as f:
                if format == "csv":
                    df = pd.read_csv(f, sep=sep, encoding="utf8")
                    data.append(df)
                elif format == "parquet":
                    df = pd.read_parquet(f, encoding="utf8")
                    data.append(df)

        return pd.concat(df)

    def delete_file_from_gcs(
        self,
        prefix: str,
    ):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        # list all objects in the directory
        blobs = bucket.list_blobs(prefix=prefix)

        deleted_files = []
        for blob in blobs:
            blob.delete()
            deleted_files.append(blob.name)
        return deleted_files

    def write_to_cloud_storage(
            self,
            dataframe: pd.DataFrame,
            storage_url: str,
            format: str = "parquet"):
        ''' function to write data to google cloud storage
        Parameters
        =============
        chunks -> List[pd.DataFrame]        : A list of pandas dataframes containing the data we want to write 
        user -> [str]                       : The name of the user as stated by username in JWT Token (The official username)
        endpoint_storage -> [str]           : The name of the endpoint e.g. followings 

        Outputs
        =============
        storage_url -> [str]                : Path the data is written to, for example - "gs://bucket-name/data/twitter/followers/2022/James2_202201020505"
        '''
        url = "gs://" + self.bucket_name + "/" + storage_url

        if format == "parquet":
            with self.fs.open(url, "wb") as f:
                dataframe.to_parquet(f, engine="pyarrow", index=False)

        elif format == "csv":
            with self.fs.open(url, "w") as f:
                dataframe.to_csv(f, encoding="utf8", index=None, sep=url)

        return "gs://" + os.environ['GOOGLE_BUCKET_NAME'] + "/" + storage_url

    @staticmethod
    def query_from_bq(query):
        """ Pass in query string
        """
        return pandas_gbq.read_gbq(query)

    def query_generic_from_bq(self, database, tablename, columns="*"):
        if(type(columns) == "list"):
            columns = ",".join(columns)
        query = "SELECT %s FROM `%s.%s`" % (columns, database, tablename)
        return self.query_big_query(query)

    def write_files_to_bq(self, df, database, tablename, mode="append"):
        destination = "%s.%s" % (database, tablename)
        df.to_gbq(destination_table=destination,
                  if_exists=mode)
