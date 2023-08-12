import re
import json
import time
import asyncio
import aiohttp

import pandas as pd
from bs4 import BeautifulSoup


async def fetch_url(session: aiohttp.ClientSession, id):
    url = f'http://city.imd.gov.in/citywx/city_weather.php?id={id}'
    async with session.get(url) as response:
        data = await response.text()
        return id, data


async def scrape_urls(ids: list):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, id) for id in ids]
        results = await asyncio.gather(*tasks)
        return results


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


def get_stations_forecast(scraped_data: tuple):

    result = []

    for id, data in scraped_data:
        forecast = parse(id, data)

        if forecast:
            result += forecast
    
    return result


if __name__ == "__main__":

    with open('./data/station_details/station_ids.json', 'r') as f:
        station_ids = json.load(f)
    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    scraped_data = loop.run_until_complete(scrape_urls(station_ids))
    loop.close()
    print(f"Total time taken to scrape: {time.perf_counter() - start}")

    start = time.perf_counter()
    forecast_data = get_stations_forecast(scraped_data)
    print(f"Total time taken to parse: {time.perf_counter() - start}")

    columns = ['station_id', 'date', 'min_temp', 'max_temp', 'weather']

    forecast_df = pd.DataFrame(forecast_data, columns=columns)

    forecast_df.to_csv("forecast.csv", index=False)


    # Total time took to scrape all the station urls ~20 sec