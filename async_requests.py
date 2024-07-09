import asyncio
import aiohttp
from more_itertools import chunked
from models import init_orm, SwapiPeople, Session
from datetime import datetime

MAX_REQWEST = 5
Number_CHARACTERS = 100


# Функция для получения json-файла с данными о персонаже фильма по id
async def get_people(person_id: int, http_session) -> dict:
    response = await http_session.get(f"http://swapi.py4e.com/api/people/{person_id}/")
    if response.status == 404:
        print(f'***** person_id {person_id} not found *****')
        return {'status': 404}
    json_data = await response.json()
    print(f'person_id {person_id} recorded')
    return json_data


# Функция для преобразования списка ссылок в строку с нужными данными из json-файлов, полученных
# с помощью get-запроса по каждой ссылке, например строку, перечисляющую названия фильмов
async def get_str(url_list: list, key: str) -> str:
    returned_list = []
    for url in url_list:
        async with aiohttp.ClientSession() as http_session:
            async with http_session.get(f'{url}') as response:
                data = await response.json()
                returned_list.append(data[key])
    return ', '.join(returned_list)


# Функция записывающая данные о персонаже фильма в таблицу бд
async def insert(jsons_list: list):
    async with Session() as db_session:
        for json_item in jsons_list:
            if json_item.get('status') == 404:
                break
            orm_objects = SwapiPeople(
                birth_year=json_item['birth_year'],
                eye_color=json_item['eye_color'],
                films=await get_str(json_item['films'], 'title'),
                gender=json_item['gender'],
                hair_color=json_item['hair_color'],
                height=json_item['height'],
                homeworld=await get_str([json_item['homeworld']], 'name'),
                mass=json_item['mass'],
                name=json_item['name'],
                skin_color=json_item['skin_color'],
                species=await get_str(json_item['species'], 'name'),
                starships=await get_str(json_item['starships'], 'name'),
                vehicles=await get_str(json_item['vehicles'], 'name'))
            db_session.add(orm_objects)
            await db_session.commit()


# Основная исполняющая функция
async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for chunks in chunked(range(1, Number_CHARACTERS), MAX_REQWEST):
            coros = [get_people(chunk, http_session) for chunk in chunks]
            jsons_list = await asyncio.gather(*coros)
            task = asyncio.create_task(insert(jsons_list))
    tasks_set = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks_set.remove(current_task)
    await asyncio.gather(*tasks_set)


if __name__ == '__main__':
    start = datetime.now()
    asyncio.run(main())
    print(f'Время обработки {datetime.now() - start}')
