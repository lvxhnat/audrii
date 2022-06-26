import os
from dotenv import load_dotenv
load_dotenv()


def create_directory_if_not_exist(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def get_api_keys(name: str):
    api_keys = [value for key, value in os.environ.items() if name in key]
    return api_keys
