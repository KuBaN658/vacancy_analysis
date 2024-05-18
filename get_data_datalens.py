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
    calc_skills_from_description,
    process_frequency,
    calc_typical_place)


da = pd.read_csv('data/da.csv', parse_dates=['published_at'])
ds = pd.read_csv('data/ds.csv', parse_dates=['published_at'])

# Создаем переменные
da['name_type'] = 'da'
ds['name_type'] = 'ds'

# объеденяем таблицы
vacancies = pd.concat((da, ds))

# Фильтрация вакансий, которые содержат слова 'data scien' и 'analyst' или 'аналитик',
# но не содержат 'видеоаналитика' в названии, без учета регистра
grid = (vacancies.name.str.lower().str.contains(r'data scien')
    & (vacancies.name.str.lower().str.contains(r'analyst')
    | vacancies.name.str.lower().str.contains(r'аналитик')) 
    & (~vacancies.name.str.lower().str.contains(r'видеоаналитика')))

# Отбрасываем отфильтрованные вакансии и сортируем оставшиеся по идентификатору
vacancies = vacancies[~grid].sort_values(by='id')

# отфильтруем системных аналитиков
grid =(((vacancies.name.str.lower().str.contains(r'систем')) | 
      (vacancies.name.str.lower().str.contains(r'system'))) &
      ~(vacancies.name.str.lower().str.contains(r'data scientist') | 
      vacancies.name.str.lower().str.contains(r'аналитик данных') |
      vacancies.name.str.lower().str.contains(r'дата аналитик')))
vacancies = vacancies[~grid]

# Преобразование даты публикации вакансий в формат даты
vacancies['published_date'] = vacancies.published_at.dt.date

# Удаление дубликатов вакансий по набору ключевых полей, оставляя уникальные
vacancies.drop_duplicates(
    subset=['name', 'employer', 'department', 'area', 'description'],
    keep=False, inplace=True
)

# Сброс индекса DataFrame после предыдущих операций
vacancies.reset_index(drop=True, inplace=True)

# Преобразование значения опыта работы с помощью функции calc_experience
vacancies.experience = vacancies.experience.map(calc_experience)

# Лемматизация описания вакансий и сохранение результатов в новом столбце
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

# удалим скиллы которые не несут в себе информации
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

# удалим скилы с microsoft
del counter['ms excel']
del counter['ms powerpoint']
del counter['ms power bi']

# сохраняем скиллы во множестве и добавляем нужные элементы
SKILLS = set(counter.keys()).union({'excel', 'powerpoint', 'power bi'})

# Добавляем столбец с навыками, извлеченными из лемматизированного описания вакансий
vacancies['skills_from_description'] = vacancies['description_lemmatized'].apply(calc_skills_from_description, skills=SKILLS)

# Создаем столбец 'skills', который содержит навыки, полученные из различных столбцов DataFrame
vacancies['skills'] = vacancies.apply(calc_skills, axis=1)

# Конвертируем зарплату в числовой формат
vacancies['salary_num'] = vacancies.apply(calc_salary_num, axis=1)

# Получаем курс валют от Центрального Банка России
CB = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()

# Конвертируем зарплату в рубли с учетом актуального курса валют
vacancies['salary_rub'] = vacancies.apply(convert_salary, axis=1, cb=CB)

# Категоризируем зарплату
vacancies['salary_bin'] = vacancies.apply(calc_salary_bin, axis=1)

# Загружаем данные с координатами городов
coords = pd.read_csv('data/coords.csv')
coords.columns = 'area', 'point'

# Извлекаем широту и долготу из координат
coords['lat'] = coords['point'].str.split(' ').map(lambda x: x[0])
coords['lon'] = coords['point'].str.split(' ').map(lambda x: x[1])

# Устанавливаем название города в качестве индекса
coords.set_index('area', inplace=True)

# Добавляем координаты в основной DataFrame вакансий
vacancies['lat'] = vacancies.area.map(coords['lat'])
vacancies['lon'] = vacancies.area.map(coords['lon'])

# Сохраняем обработанные данные о вакансиях в CSV-файл
vacancies.to_csv('data/vacancies_bi.csv', index=False)

# Определяем категории опыта работы
grades = ("Junior (no experience)", "Junior+ (1-3 years)", "Middle (3-6 years)", "Senior (6+ years)")

# Вычисляем типичные места работы для аналитиков и датасаентистов
da_typical_place = calc_typical_place(vacancies, 'da', grades)
ds_typical_place = calc_typical_place(vacancies, 'ds', grades)

# Сохраняем данные о типичных местах работы в CSV-файлы
da_typical_place.to_csv('data/da_typical_place.csv')
ds_typical_place.to_csv('data/ds_typical_place.csv')

# Разделяем навыки в столбце 'skills' и преобразуем DataFrame таким образом,
# чтобы каждый навык был в отдельной строке
vacancies['skills'] = vacancies['skills'].str.split(', ')
vacancies = vacancies[['id', 'skills']].explode('skills')

# Удаляем строки с отсутствующими навыками и сбрасываем индекс
vacancies.dropna(subset=['skills'], inplace=True)
vacancies.reset_index(drop=True, inplace=True)

# Стандартизируем названия навыков
vacancies['skills'] = vacancies['skills'].map(process_frequency)

# Сохраняем навыки в CSV-файл
vacancies.to_csv('data/skills.csv', index=False)

