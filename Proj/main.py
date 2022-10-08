import asyncio
import itertools
from datetime import datetime

import aiohttp
from pprint import pprint
from more_itertools import chunked

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String
import re
from cache import AsyncLRU

PGN = "postgresql+asyncpg://app:1234@127.0.0.1:5431/netology_asyncio"

engine = create_async_engine(PGN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = "people"
    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK = 10


async def get_people(sw_id, session):
    async with session.get(f"https://swapi.dev/api/people/{sw_id}") as response:
        data = await response.json()
        return sw_id, response.status, data


# async def main_coros():
#     async with aiohttp.ClientSession() as session:
#         coros = (get_people(i, session) for i in range(1, 200))
#
#         for coros_chunk in chunked(coros, CHUNK):
#
#             results = await asyncio.gather(*coros_chunk)
#             for result in results:
#                 print(result)
#             # Если в чанке все ответы пришли с 404, то считается, что выкачены все персонажи
#             if all(result[1] == 404 for result in results):
#                 print("empty chunk")
#                 break


# async def main_tasks():
#     async with aiohttp.ClientSession() as session_http:
#
#         tasks = (asyncio.create_task(get_people(i, session_http)) for i in range(1, 200))
#
#         for tasks_chunk in chunked(tasks, CHUNK):
#             for task in tasks_chunk:
#                 result = await task
#                 print(result)


# def cached(old_function):
#     CACHE = {}
#
#     def new_function(*args, **kwargs):
#         #print(args[:-1])
#
#         key = f'{args[:-1]}_{kwargs}'  # сессию не нужно включать в список ключей
#         #print(key)
#         if key in CACHE:
#             return CACHE[key]
#         result = old_function(*args, **kwargs)
#
#         CACHE[key] = result
#         return result
#
#     return new_function


async def get_result(id_range, session):
    for id_chunk in chunked(id_range, CHUNK):
        tasks = (asyncio.create_task(get_people(i, session)) for i in id_chunk)
        count_404 = 0
        for task in tasks:
            result = await task
            if result[1] == 404: count_404 += 1
            yield result
        if count_404 == CHUNK: break  # выход если весь чанк пустой #raise StopIteration


# @cached


@AsyncLRU(maxsize=100)
async def get_additional_data(url, id_add, key, session):
    async with session.get(f"https://swapi.dev/api/{url}/{id_add}") as response:
        data = await response.json()
        return data[key]


async def get_data_by_ids(url, ids, key, session):
    tasks = (asyncio.create_task(get_additional_data(url, i, key, session)) for i in ids)
    for task in tasks:
        yield await task


def get_ids_from_urls(*urls):
    ids = []
    for url in urls:
        res = re.search(r"\d+", url).group()
        ids.append(res)
    return ids


async def create_string_for_additional_data(urls, url, key, session):
    ids = get_ids_from_urls(*urls)
    res_list = []
    async for film in get_data_by_ids(url, ids, key, session):
        res_list.append(film)
    res_string = ", ".join(res_list)
    return res_string


async def main_tasks_gen():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()


    async with aiohttp.ClientSession() as session_http:
        async with Session() as session_bd:

            async for person in get_result(range(1, 201), session_http):
                print(person)
                if person[1] == 200:
                    person_json = person[2]
                    films_string = await create_string_for_additional_data(person_json["films"], "films", "title",
                                                                           session_http)
                    homeworld_string = await create_string_for_additional_data([person_json["homeworld"]], "planets",
                                                                               "name", session_http)
                    species_string = await create_string_for_additional_data(person_json["species"], "species", "name",
                                                                             session_http)
                    starships_string = await create_string_for_additional_data(person_json["starships"], "starships",
                                                                               "name", session_http)
                    vehicles_string = await create_string_for_additional_data(person_json["vehicles"], "vehicles",
                                                                              "name", session_http)

                    session_bd.add(People(
                        id=person[0],
                        birth_year=person_json["birth_year"],
                        eye_color=person_json["eye_color"],
                        films=films_string,
                        gender=person_json["gender"],
                        hair_color=person_json["hair_color"],
                        height=person_json["height"],
                        homeworld=homeworld_string,
                        mass=person_json["mass"],
                        name=person_json["name"],
                        skin_color=person_json["skin_color"],
                        species=species_string,
                        starships=starships_string,
                        vehicles=vehicles_string,

                    ))

                    await session_bd.commit()


start = datetime.now()
asyncio.run(main_tasks_gen())
print(datetime.now() - start)
