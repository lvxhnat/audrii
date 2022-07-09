import os
from dotenv import load_dotenv
load_dotenv()


def get_api_keys(name: str):
    api_keys = [value for key, value in os.environ.items() if name in key]
    return api_keys
