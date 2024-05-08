import requests
import re
from tqdm import tqdm
from time import sleep
import pandas as pd
from datetime import timedelta


def get_ids_on_page(url):
    data = requests.get(url).json()
    if data.get('items'):
        return [el['id'] for el in data['items']]


def get_all_ids(text):
    i = 0
    url = f'https://api.hh.ru/vacancies?' \
          f'text={text}&period=3&search_field=name&per_page=100&page={i}'
    ids = []

    while data := get_ids_on_page(url):
        ids.extend(data)
        i += 1
        url = url[:-1] + str(i)

    return ids


def get_dataset(ids) :
    dataset = []
    for id in tqdm(ids):
        url = f"https://api.hh.ru/vacancies/{id}"
        req = requests.get(url)
        data = req.json()
        req.close()
    
        try:
            vacancy = [data['id'],
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
                re.sub(r"\<[^>]*\>", '', data['description']),
                data['salary']['from'] if data['salary'] is not None else None,
                data['salary']['to'] if data['salary'] is not None else None
            ]
        except:
            print(data)
        dataset.append(vacancy)
        sleep(0.2)
    return dataset

def merge_data(vacancy_name: str):
    new = pd.read_csv(f'data/new_vacancies_{vacancy_name}.csv', parse_dates=['published_at'])
    old = pd.read_csv(f'data/{vacancy_name}.csv', parse_dates=['published_at'])
    new['published_at'] = new['published_at'].dt.tz_convert(None) + timedelta(hours=3)

    merged = pd.concat((old, new)).sort_values(by=['id', 'published_at'])
    return merged.drop_duplicates(subset='id', keep='first').reset_index(drop=True)
    
    
    