import dataclasses
from time import sleep

import meilisearch
import json

client = meilisearch.Client('http://localhost:7700')


@dataclasses.dataclass
class Restaurant:
    id: int
    name: str
    aggregate_rating: int
    votes: int
    cuisines: list[str]
    city: str
    country_id: int
    city_id: int
    locality: str
    thumbnail: str


def entry_to_restaurant(raw: dict[str, any], id:int) -> Restaurant:
    raw = raw['restaurant']
    cuisines_r = raw["cuisines"].split(",")
    cuisines = []
    for c in cuisines_r:
        cuisines.append(c.strip())
    return Restaurant(
        id=id,
        name=raw["name"],
        locality=raw["location"]["locality"],
        city=raw["location"]["city"],
        country_id=raw["location"]["country_id"],
        city_id=raw["location"]["city_id"],
        aggregate_rating=raw["user_rating"]["aggregate_rating"],
        votes=raw["user_rating"]["votes"],
        cuisines=cuisines,
        thumbnail=raw["thumb"]
    )


restaurants = []
counter = 0
for i in range(1, 6):
    json_file = open(f'zomato/file{i}.json', encoding="utf8")
    zomato = json.load(json_file)
    pages = zomato

    for p in pages:
        if "results_found" in p:
            if p["results_found"] > 0:
                for r in p["restaurants"]:
                    restaurant = entry_to_restaurant(r, counter)
                    counter += 1
                    restaurants.append(dataclasses.asdict(restaurant))

print(restaurants[0])
client.index('zomato').update(primary_key='id')
task = client.index('zomato').add_documents(restaurants)
sleep(5)
print(client.get_task(task.task_uid))
# client.index('zomato').delete_all_documents()
