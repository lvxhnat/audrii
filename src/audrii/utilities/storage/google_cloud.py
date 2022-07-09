import os
import gcsfs
import dotenv
import pandas_gbq
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

from concurrent.futures import ThreadPoolExecutor, as_completed
dotenv.load_dotenv()


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

    def get_files_with_prefix(
            self,
            prefix: str = "",
            bucket_name: str = os.environ['GOOGLE_BUCKET_NAME']):

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

    def read_file_from_gcs(
            self,
            prefix: str,
            sep: str = "\t",
            num_workers: int = 25,
            format: str = "parquet"):

        blobs = self.get_files_with_prefix(
            prefix=prefix,
            bucket_name=self.bucket_name)

        data = []

        with ThreadPoolExecutor(max_workers=num_workers) as executor:

            future_promise = {executor.submit(
                self._read_file_from_gcs, blob): blob for blob in blobs}

            for future in as_completed(future_promise):
                response = future.result()
                data.append(response)

        return pd.concat(data)

    def _read_file_from_gcs(self, blob: str, format: str = "parquet"):
        read_base = "gs://" + self.bucket_name + "/"
        with self.fs.open(read_base + blob) as f:
            if format == "csv":
                df = pd.read_csv(f, sep=sep, encoding="utf8")
            elif format == "parquet":
                df = pd.read_parquet(f)
        return df

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
