import requests
from . import constants as global_constants
from . import settings
from click import echo


def get_data(uri, email=None, api_key=None, debug=False, params={}):
    """
    Retrieves data from the API
    :return:
    """
    if debug:
        echo(f"\n\nEntering get_data\nuri={uri}\nparams={params}")
    if not settings.EMAIL and not email:
        raise Exception("Please specify an email")

    if not settings.API_KEY and not api_key:
        raise Exception("Please specify an API key")

    path_params = {
        "email": email if email else settings.EMAIL,
        "key": api_key if api_key else settings.API_KEY,
    }
    if debug:
        echo(f"path_params: {path_params}")

    uri = global_constants.HOST + uri
    if debug:
        echo(f"Constructed URL: {uri}")

    path_params.update(params)
    if debug:
        echo(f"Combined params: {path_params}")

    r = requests.get(uri, params=path_params)
    r.raise_for_status()
    return r.json()
