import pandas as pd
import requests
from utils import (
    calc_experience,
    calc_salary_bin, 
    calc_salary_num,
    convert_salary,
    get_coords, 
    lemmatize_corpus, 
    calc_skills, 
    calc_skills_from_description)


da = pd.read_csv('data/da.csv', parse_dates=['published_at'])
ds = pd.read_csv('data/ds.csv', parse_dates=['published_at'])

# Создаем переменные
da['name_type'] = 'da'
ds['name_type'] = 'ds'

# объеденяем таблицы
vacancies = pd.concat((da, ds))

grid = (vacancies.name.str.lower().str.contains(r'data scien')
    & (vacancies.name.str.lower().str.contains(r'analyst')
    | vacancies.name.str.lower().str.contains(r'аналитик')) 
    & (~vacancies.name.str.lower().str.contains(r'видеоаналитика')))

vacancies = vacancies[~grid].sort_values(by='id')

vacancies['published_date'] = vacancies.published_at.dt.date

vacancies.drop_duplicates(
    subset=['name', 'employer', 'department', 'area', 'description'],
    keep=False, inplace=True
)

vacancies.experience = vacancies.experience.map(calc_experience)
vacancies.reset_index(drop=True, inplace=True)

vacancies['description_lemmatized'] = lemmatize_corpus(vacancies.description)

# Создаем новую колонку 'skills_from_key_skills' в DataFrame 'vacancies'
# В этой колонке для каждой вакансии будет храниться уникальный набор навыков,
# приведенный к нижнему регистру
vacancies['skills_from_key_skills'] = (
    vacancies['key_skills'].map(
        lambda x: ', '.join(set([skill[1:-1] for skill in x[1:-1].lower().split(', ')])))
)

# Преобразуем колонку 'skills_from_key_skills' в список для дальнейшей обработки
skills_data = vacancies['skills_from_key_skills'].to_list()

# Инициализируем словарь для подсчета частоты встречаемости каждого навыка
counter = {}

# Проходим по всем навыкам в списке 'skills_data'
for sequence in skills_data:
    # Разделяем строку с навыками на уникальные элементы и преобразуем в множество
    skills = set(sequence.split(', '))
    # Подсчитываем количество вхождений каждого навыка
    for skill in skills:
        counter[skill] = counter.get(skill, 0) + 1

# Фильтруем словарь 'counter', оставляя только те навыки, которые встречаются более 20 раз
counter = {k: v for k, v in counter.items() if v > 10}

# Сортируем словарь 'counter' по убыванию частоты встречаемости навыков
counter = dict(sorted(counter.items(), key=lambda item: item[1], reverse=True))

del counter['']
del counter['анализ данных']
del counter['data analysis']
del counter['machine learning']
del counter['аналитика']
del counter['data science']
del counter['ml']
del counter['аналитические исследования']
del counter['машинное обучение']
del counter['работа с большим объемом информации']
del counter['it']

del counter['ms excel']
del counter['ms powerpoint']
del counter['ms power bi']

# сохраняем скиллы во множестве и добавляем нужные элементы
SKILLS = set(counter.keys()).union({'excel', 'powerpoint', 'power bi'})

vacancies['skills_from_description'] = vacancies['description_lemmatized'].apply(calc_skills_from_description, skills=SKILLS)
vacancies['skills'] = vacancies.apply(calc_skills, axis=1)

vacancies['salary_num'] = vacancies.apply(calc_salary_num, axis=1)

# получаем курс валют ЦБ
CB = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
vacancies['salary_rub'] = vacancies.apply(convert_salary, axis=1, cb=CB)

vacancies['salary_bin'] = vacancies.apply(calc_salary_bin, axis=1)

coords = pd.read_csv('data/coords.csv')
coords.columns = 'area', 'point'
coords['lat'] = coords['point'].str.split(' ').map(lambda x: x[0])
coords['lon']= coords['point'].str.split(' ').map(lambda x: x[1])
coords.set_index('area', inplace=True)

vacancies['lat'] = vacancies.area.map(coords['lat'])
vacancies['lon'] = vacancies.area.map(coords['lon'])

vacancies.to_csv('data/vacancies_bi.csv', index=False)

vacancies['skills'] = vacancies['skills'].str.split(', ')
vacancies = vacancies[['id', 'skills']].explode('skills')
vacancies.to_csv('data/skills.csv', index=False)
