import pandas as pd

from utils import get_all_ids, get_dataset


if __name__ == '__main__':
    
    data_science_ids = get_all_ids('data+scien*')
    data_analyst_ids = get_all_ids('data+analyst+OR+аналитик+данных+OR+дата+аналитик')
    
    data_scientist_vacancies = get_dataset(data_science_ids)
    data_analyst_vacancies = get_dataset(data_analyst_ids)
    
    data_scientist_vacancies.to_csv('data/ds.csv', index=False)
    data_analyst_vacancies.to_csv('data/da.csv', index=False)


    



