import settings
import requests


def get_data():
    """
    Retrieves data from the API
    :return:
    """
    if not settings.EMAIL:
        raise Exception("Please specify an email")

    if not settings.API_KEY:
        raise Exception("Please specify an API key")

    path_params = {
        "email": f"{settings.EMAIL}",
        "key": f"{settings.API_KEY}",
        "param": "44201",
        "bdate": "20190101",
        "edate": "20191231",
        "state": "36",
        "county": "061"
    }

    r = requests.get("https://aqs.epa.gov/data/api/dailyData/byCounty", params=path_params)

    return r.json()