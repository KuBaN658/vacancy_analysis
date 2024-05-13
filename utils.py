import requests
import re
from tqdm import tqdm
from time import sleep
import pandas as pd
from typing import List, Optional


def get_ids_on_page(url: str) -> List[int]:
    """
    Получает список идентификаторов вакансий с одной страницы результатов API.

    Parameters:
    url (str): URL страницы API для получения идентификаторов вакансий.

    Returns:
    List[int]: Список идентификаторов вакансий.
    """
    data = requests.get(url).json()
    if data.get('items'):
        return [el['id'] for el in data['items']]
    return []


def get_all_ids(text: str) -> List[int]:
    """
    Получает все идентификаторы вакансий по заданному тексту поиска.

    Parameters:
    text (str): Текст для поиска вакансий.

    Returns:
    List[int]: Список всех идентификаторов вакансий.
    """
    i = 0
    url = f'https://api.hh.ru/vacancies?' \
          f'text={text}&search_field=name&per_page=100&page={i}'
    ids = []

    while data := get_ids_on_page(url):
        ids.extend(data)
        i += 1
        url = url[:-1] + str(i)

    return ids


def get_dataset(ids: List[int]) -> pd.DataFrame:
    """
    Создает набор данных о вакансиях по списку идентификаторов.

    Parameters:
    ids (List[int]): Список идентификаторов вакансий.

    Returns:
    pd.DataFrame: DataFrame с данными о вакансиях.
    """
    dataset = []
    for id in tqdm(ids):
        url = f"https://api.hh.ru/vacancies/{id}"
        req = requests.get(url)
        data = req.json()
        req.close()
    
        try:
            # Удаление HTML-тегов из описания вакансии
            description_cleaned = re.sub(r"<[^>]*>", '', data['description'])
            
            # Сбор информации о вакансии в список
            vacancy = [
                data['id'],
                data['name'],
                data['published_at'],
                data['alternate_url'],
                data['type']['name'],
                data['employer']['name'],
                data['department']['name'] if data['department'] is not None else None,
                data['area']['name'],
                data['experience']['name'],
                [dic['name'] for dic in data['key_skills']],
                data['schedule']['name'],
                data['employment']['name'],
                description_cleaned,
                data['salary']['from'] if data['salary'] is not None else None,
                data['salary']['to'] if data['salary'] is not None else None,
                data['salary']['currency'] if data['salary'] is not None else None,
            ]
        except Exception as e:
            print(f"Error processing vacancy ID {id}: {e}")
        else:
            dataset.append(vacancy)
            sleep(0.5)  # Задержка для соблюдения лимита запросов к API

    # Преобразование списка вакансий в DataFrame
    columns = ['id', 'name', 'published_at', 'alternate_url', 'type', 'employer',
               'department', 'area', 'experience', 'key_skills', 'schedule',
               'employment', 'description', 'salary_from', 'salary_to', 'currency_salary']
    return pd.DataFrame(dataset, columns=columns)

    
    
    