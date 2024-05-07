import requests
import re
import pandas as pd
from datetime import timedelta

from utils import get_all_ids, get_dataset, merge_data


if __name__ == '__main__':
    
    data_science_ids = get_all_ids('data+scien*')
    data_analyst_ids = get_all_ids('data+analyst+OR+аналитик+данных+OR+дата+аналитик')
    
    data_scientist_vacancies = get_dataset(data_science_ids)
    data_analyst_vacancies = get_dataset(data_analyst_ids)
    
    columns=['id', 'name', 'published_at', 'alternate_url', 
             'type', 'employer', 'department', 'area', 
             'experience', 'key_skills', 'schedule', 'employment', 
             'description', 'salary_from', 'salary_to', 'salary_currency'
    ]
    
    data_scientist_vacancies = pd.DataFrame(data_scientist_vacancies, columns=columns)
    data_analyst_vacancies = pd.DataFrame(data_analyst_vacancies, columns=columns)
    
    data_scientist_vacancies.to_csv('data/new_vacancies_ds.csv', index=False)
    data_analyst_vacancies.to_csv('data/new_vacancies_da.csv', index=False)

    merge_data('ds')
    merge_data('da')

    



