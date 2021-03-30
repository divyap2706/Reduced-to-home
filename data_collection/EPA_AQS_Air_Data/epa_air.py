""" Author: Alex Biehl
    email: alex.biehl@nyu.edu"""

import requests
import csv
import settings


def main():
    param_list = get_aqi_pollutants_list()
    print(f"Retrieved parameter list.\n{param_list}")
    for i in range(len(param_list)):
        param = param_list[i]
        data = get_2019_data(int(param.get("code")))
        do_append = i != 0
        if len(data.get("data")) > 0:
            output(data.get("data"), "AQI-POLLUTANTS", "2019-2020", append=do_append)
        else:
            print(
                f"No rows retrieved for {param.get('value_represented')} and year 2019"
            )
        data = get_2020_data(int(param.get("code")))
        if len(data.get("data")) > 0:
            output(data.get("data"), "AQI-POLLUTANTS", "2019-2020", append=do_append)
        else:
            print(
                f"No rows retrieved for {param.get('value_represented')} and year 2020"
            )

    print("Finished")


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
        "header": response.get("Header"),
    }


def get_2020_data(param):
    response = make_request(param, bdate="20200101", edate="20201231")
    return {
        "success": response.get("Header")[0].get("status") == "Success",
        "data": response.get("Data"),
        "header": response.get("Header"),
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
        "county": "061",
    }
    r = requests.get(uri, params=path_params)
    r.raise_for_status()
    data = r.json()
    if data.get("Header")[0].get("status") == "Failure":
        raise Exception(
            f"There was a failure retrieving data.\nParam: {parameter}\nbdate: {bdate}\n:edate: {edate}\nHeader: {data.get('Header')}"
        )

    return data


def get_aqi_pollutants_list():
    uri = "https://aqs.epa.gov/data/api/list/parametersByClass"
    pollutant_class = "AQI POLLUTANTS"
    path_params = {
        "email": settings.EMAIL,
        "key": settings.API_KEY,
        "pc": pollutant_class,
    }
    r = requests.get(uri, params=path_params)
    r.raise_for_status()
    j_data = r.json()
    if j_data.get("Header")[0].get("status") == "Failure":
        raise Exception(f"Unable to retrieve parameter list:\n{j_data.get('Header')}")

    return j_data.get("Data")


if __name__ == "__main__":
    main()
