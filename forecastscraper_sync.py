import requests
import re
import json
import time

import pandas as pd
from bs4 import BeautifulSoup


def get_response_text(id: int):
    URL = f'http://city.imd.gov.in/citywx/city_weather.php?id={id}'
    
    return requests.get(URL).text


def parse(id: int, text: str):
    soup = BeautifulSoup(text, 'lxml')
    
    result = []

    element_with_forecast_header = soup.find('font',string=re.compile("7 Day's Forecast"))
    try:
        forecast_rows = element_with_forecast_header\
                        .parent.parent.parent.parent.find_all('tr')[-7:]
    except:
        print(f"Could not find details for {id}")
    else:
        for row in forecast_rows:
            values = row.find_all('font')
            res = [id]
            for value in values:
                res.append(value.text.strip())
            result.append(tuple(res))
    finally:
        return result


def get_stations_forecast(station_details: dict):

    result = []
    for state_ut, cities in station_details.items():

        for station_name, station_id in cities.items():
            response = get_response_text(station_id)

            forecast = parse(station_id, response)

            if forecast:
                result += forecast
            else:
                print(f"Forecast doesn't exist for {station_name} with {station_id} from {state_ut}")
        print(f"{state_ut} done...")
    
    return result

if __name__ == '__main__':

    with open('./data/station_details/weather_stations.json', 'r') as f:
        stations = json.load(f)

    start = time.perf_counter()
    forecast_data = get_stations_forecast(stations)
    print(f"Total time taken: {time.perf_counter() - start}")

    columns = ['station_id', 'date', 'min_temp', 'max_temp', 'weather', 'image']

    forecast_df = pd.DataFrame(forecast_data, columns=columns)

    forecast_df.to_csv("forecast.csv")

    # total time taken to scrape all the stations forecast is ~35 minutes