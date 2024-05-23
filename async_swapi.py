import asyncio
import aiohttp
from more_itertools import chunked
from models import Session, DeclarativeBase, PG_DSN, SwapiPeople, init_db

MAX_CHUNK = 5


async def extract_names(list_data):
    list_names = []
    async with aiohttp.ClientSession() as client_session:
        for data in list_data:
            response = await client_session.get(data)
            response_json = await response.json()
            list_names.append(response_json[list(response_json.keys())[0]])
    return ','.join(list_names)


async def insert_people(people_list):
    async with Session() as session:
        async with session.begin():
            for person in people_list:
                if person.get('birth_year'):
                    person_data = {
                        'name': person['name'],
                        'height': person['height'],
                        'mass': person['mass'],
                        'hair_color': person['hair_color'],
                        'skin_color': person['skin_color'],
                        'eye_color': person['eye_color'],
                        'birth_year': person['birth_year'],
                        'gender': person['gender'],
                        'homeworld': person['homeworld'],
                        'films': await extract_names(person['films']),
                        'species': await extract_names(person['species']),
                        'vehicles': await extract_names(person['vehicles']),
                        'starships': await extract_names(person['starships'])
                    }
                    new_person = SwapiPeople(**person_data)
                    session.add(new_person)


async def get_person(session, person_id):
    async with aiohttp.ClientSession() as client_session:
        async with client_session.get(f"https://swapi.dev/api/people/{person_id}/") as response:
            return await response.json()


async def main():
    await init_db()
    tasks = []
    async with Session() as session:
        for person_id_chunk in chunked(range(1, 90), MAX_CHUNK):
            coros = [get_person(session, person_id) for person_id in person_id_chunk]
            results = await asyncio.gather(*coros)
            tasks.append(insert_people(results))

        await session.close()
        all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*all_tasks_set)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
