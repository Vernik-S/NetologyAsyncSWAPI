import requests
from datetime import datetime


def get_sync():
    for i in range(1,100):
        response = requests.get(f"https://swapi.dev/api/people/{i}")
        print(response.json())

start = datetime.now()
get_sync()
print(datetime.now() - start)