import requests
import re
from tqdm import tqdm
from time import sleep
import pandas as pd
import numpy as np
from typing import List
from pymystem3 import Mystem


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


def calc_experience(value: str) -> str:
    """
    Определяет уровень опыта на основе переданной строки.

    Функция анализирует строку, содержащую информацию об опыте работы, 
    и возвращает квалификационный уровень в соответствии с найденными данными.
    Если опыт работы не указан, возвращает уровень 'Junior (no experience)'.

    Parameters:
    value (str): Строка, содержащая информацию об опыте работы.

    Returns:
    str: Квалификационный уровень на основе опыта работы.
    """

    # Используем регулярное выражение для поиска цифр в строке
    experience = re.findall(r'\d', value)

    # Если цифры не найдены, считаем, что опыт работы отсутствует
    if not experience:
        return 'Junior (no experience)'
    
    # Возвращаем соответствующий уровень опыта на основе найденной первой цифры
    if experience[0] == '6':
        return 'Senior (6+ years)'
    if experience[0] == '1':
        return 'Junior+ (1-3 years)'
    if experience[0] == '3':
        return 'Middle (3-6 years)'
    

def lemmatize_corpus(description: pd.Series) -> pd.Series:
    """
    Производит лемматизацию серии текстовых данных.

    Эта функция принимает на вход серию текстовых данных (pd.Series), 
    объединяет их в один текст, разделяя маркером ' br ', 
    затем применяет к нему лемматизацию и возвращает новую серию, 
    где каждый элемент представляет собой лемматизированный текст.

    Parameters:
    description (pd.Series): Серия текстовых данных для лемматизации.

    Returns:
    pd.Series: Серия лемматизированных текстовых данных.
    """
    
    # Объединение всех текстов в одну строку с маркером ' br ' для разделения текстов
    texts = ' br '.join(description.to_list())
    
    # Инициализация лемматизатора
    stem = Mystem()
    
    print('Запуск лемматизации')
    # Применение лемматизации к объединенному тексту
    text_lemm = stem.lemmatize(texts)
    
    # Инициализация списка для хранения лемматизированных текстов
    data = []
    # Временный список для хранения слов текущего текста
    temp = []
    
    # Проходим по всем словам в лемматизированном тексте
    for word in tqdm(text_lemm):
        # Если встречаем маркер 'br', значит это конец текущего текста
        if word == 'br':
            # Добавляем лемматизированный текст в список data
            data.append(' '.join([word for word in temp if word.isalpha()]))
            # Очищаем временный список для следующего текста
            temp = []
        else:
            # Добавляем слово в временный список
            temp.append(word)
    # Добавляем последний текст в список data
    data.append(' '.join([word for word in temp if word.isalpha()]))
    
    # Преобразуем список лемматизированных текстов в pd.Series
    data = pd.Series(data, name='description_lemmatized')
    
    # Проверка на соответствие размеров входной и выходной серий
    assert description.shape[0] == data.shape[0]
    
    return data


def calc_skills_from_description(value: str, skills) -> str:
    """
    Извлекает навыки из описания, используя заданный список навыков SKILLS.

    Функция проходит по словам в строке 'value', объединяет их в группы от одного до четырех слов,
    и если сочетание слов является навыком из списка SKILLS, добавляет его в результат.
    Результат возвращает в виде строки, где навыки разделены запятой.

    Parameters:
    value (str): Строка с описанием, из которой необходимо извлечь навыки.

    Returns:
    str: Строка с уникальными навыками, извлеченными из описания.
    """
    res = []  # Инициализация списка для хранения найденных навыков
    value = value.lower().split()  # Приведение строки к нижнему регистру и разделение на слова
    start = 0  # Начальный индекс для среза слов
    stop = 4  # Конечный индекс для среза слов

    # Пока конечный индекс не превысит количество слов в строке
    while stop <= len(value):
        skill = ' '.join(value[start:stop])  # Объединение слов в потенциальный навык
        if skill in skills:  # Проверка, является ли сочетание слов навыком
            res.append(skill)  # Добавление навыка в результат
            start = stop  # Сдвиг начального индекса
            stop = start + 4  # Сдвиг конечного индекса
        else:
            # Уменьшение конечного индекса для проверки следующего сочетания слов
            if start < stop - 1:
                stop -= 1
            else:
                # Сдвиг начального индекса, если не найдено сочетаний
                start = stop
                stop = start + 4
    return ', '.join(set(res))  # Возврат строки с уникальными навыками

def calc_skills(row: dict) -> str:
    """
    Вычисляет итоговый набор навыков из двух полей строки DataFrame: 'skills_from_description' и 'skills_from_key_skills'.

    Если одно из полей пустое, возвращает значение другого поля. Если оба поля содержат значения,
    объединяет их в один уникальный набор навыков.

    Returns:
    str: Строка с уникальными навыками, полученными из обоих полей.
    """
    # Проверка и возврат навыков из описания, если поле с навыками из описания пусто
    if row['skills_from_description'] == '':
        return row['skills_from_key_skills']
    # Проверка и возврат навыков из ключевых слов, если поле с навыками из ключевых навыков пусто
    if row['skills_from_key_skills'] == '':
        return row['skills_from_description']
    # Объединение и возврат уникальных навыков из обоих полей
    return ', '.join(set(row['skills_from_description'].split(', ') + row['skills_from_key_skills'].split(', ')))


def calc_salary_num(row: pd.Series) -> float:
    """
    Вычисляет среднее значение зарплаты на основе верхнего и нижнего порогов.

    Функция принимает строку DataFrame, содержащую информацию о зарплатных предложениях
    (salary_to и salary_from), и возвращает среднее значение, если указаны оба порога,
    или одно из значений, если указан только один порог.

    Parameters:
    row (pd.Series): Строка DataFrame, содержащая информацию о зарплатных предложениях.

    Returns:
    float: Среднее значение зарплаты или один из указанных порогов.
    """
    
    # Проверяем, указаны ли оба порога зарплаты, и если да, возвращаем их среднее
    if not row.isna()['salary_to'] and not row.isna()['salary_from']:
        return (row['salary_to'] + row['salary_from']) / 2
    
    # Возвращаем верхний порог зарплаты, если он указан
    if not row.isna()['salary_to']:
        return row['salary_to']
    
    # Возвращаем нижний порог зарплаты, если он указан
    if not row.isna()['salary_from']:
        return row['salary_from']
    

def convert_salary(row: pd.Series, cb: dict) -> float:
    """
    Конвертирует зарплату из валюты вакансии в рубли на основе курса ЦБ.

    Функция проверяет валюту зарплаты и конвертирует её в рубли, используя
    текущий курс Центрального Банка, содержащийся в словаре cb.

    Parameters:
    row (pd.Series): Строка DataFrame, содержащая информацию о зарплате и валюте.
    cb (dict): Словарь с данными о курсах валют от Центрального Банка.

    Returns:
    float: Зарплата в рублях после конвертации.
    """

    # Если валюта указана в рублях вернем это значение
    if row['currency_salary'] == 'RUR' or row['currency_salary'] is np.nan:
        return row['salary_num']
    
    # Если валюта зарплаты указана как BYR, конвертируем её в BYN
    if row['currency_salary'] == 'BYR':
        row['currency_salary'] = 'BYN'
    
    # Получаем курс валюты из словаря CB
    dic = cb['Valute'][row['currency_salary']]
    rates = dic['Value'] / dic['Nominal']
    
    # Возвращаем зарплату в рублях после конвертации
    return row['salary_num'] * rates


def calc_salary_bin(row: pd.Series) -> str:
    """
    Категоризирует зарплатные предложения вакансий по их размеру.

    Функция принимает строку DataFrame, содержащую информацию о зарплате (salary_to и salary_from),
    и возвращает категорию зарплаты в зависимости от указанных значений.

    Parameters:
    row (pd.Series): Строка DataFrame, содержащая информацию о зарплатных предложениях.

    Returns:
    str: Категория зарплаты.
    """
    
    # Проверяем отсутствие данных о зарплате
    if row.isna()['salary_rub']:
        return 'ЗП не указана'
    
    # Категоризируем зарплату
    else:
        if row['salary_rub'] > 3e5:
            return 'Больше 300 тысяч'
        if row['salary_rub'] > 2e5:
            return 'От 200 тысяч до 300 тысяч'
        if row['salary_rub'] > 1e5:
            return 'От 100 тысяч до 200 тысяч'
        else:
            return 'Меньше 100 тысяч'
    
    
    