import json
from bs4 import BeautifulSoup


with open("./html/imd_city_page.html") as fp:
    soup = BeautifulSoup(fp, 'lxml')


states = soup.select('ul#sidebarmenu1 > li > a')
state_wise_cities = soup.select('ul#sidebarmenu1 > li > ul')

result = {}
for state, cities in zip(states, state_wise_cities):
    print(state.text)

    s = {}
    for city in cities.select('li'):
        s[city.text] = city.a.attrs['href'].split("=")[-1]
    result[state.text] = s

with open("./data/station_details/weather_stations.json", "w") as file:
    json.dump(result, file, indent=4)
