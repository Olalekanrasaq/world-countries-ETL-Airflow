import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json


def extract(url: str, e_path: str):
    """
    This function extract countries data from Rest Country API and load it into a json file.

    Parameters:
    url (str): the url to extract data from
    e_path(str): path to the file where the extracted data will be saved

    Returns: a json file
    """
    try:
        r = requests.get(url)
        data = r.json()
        # load the extracted data into a json_file
        with open(e_path, "w") as f:
            json.dump(data, f)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
