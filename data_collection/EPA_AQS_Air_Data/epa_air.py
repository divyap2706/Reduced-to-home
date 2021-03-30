""" Author: Alex Biehl
    email: alex.biehl@nyu.edu"""

import requests
import csv
import json
import settings


def main():
    test = get_2020_data(44201)
    if test.get("success"):
        data = json.dumps(test.get("data"), indent=2)
        print("Success!")
        # print(f"Data: {data}")
        output(test.get("data"), "Ozone", 2020)

    else:
        print("Failure")
        print(f"Header: {test.get('header')}")
        #print(f"\n\noutput: {test}")


def output(data, param, year, append=False):
    file_name = f"epa_aqs_air_{param}_{year}.csv"
    write_type = "w" if not append else "a"
    with open(file_name, write_type, newline="") as csvfile:
        field_names = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=field_names)

        if not append:
            writer.writeheader()
        for row in data:
            writer.writerow(row)

    print("write complete")


def get_2019_data(param):
    response = make_request(param, bdate="20190101", edate="20191231")
    return {
        "success": response.get("Header")[0].get("status") == "Success",
        "data": response.get("Data"),
        "header": response.get("Header")
    }


def get_2020_data(param):
    response = make_request(param, bdate="20200101", edate="20201231")
    return {
        "success": response.get("Header")[0].get("status") == "Success",
        "data": response.get("Data"),
        "header": response.get("Header")
    }


def make_request(parameter=None, bdate=None, edate=None):
    uri = "https://aqs.epa.gov/data/api/dailyData/byCounty"
    path_params = {
        "email": settings.EMAIL,
        "key": settings.API_KEY,
        "param": parameter,
        "bdate": bdate,
        "edate": edate,
        # New York state
        "state": 36,
        # New York county
        "county": "061"
    }
    r = requests.get(uri, params=path_params)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    main()
