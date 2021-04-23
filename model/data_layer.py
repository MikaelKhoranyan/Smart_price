import json
from typing import List

import sqlalchemy
from sqlalchemy import create_engine


def get_sql_connection(db_name:str='Prices') -> sqlalchemy.engine.Engine:
    """
    Возвращает engine-соедниение с базой данных

    param: db_name - имя базы данных
    returns: db engine object
    """
    with open("model/conf.json", 'rb') as f:
        conf = json.load(f)['sql_server']
    mssql_connection_url = f"mssql+pymssql://{conf['user']}:{conf['password']}@{conf['server_adress']}/{db_name}"
    return create_engine(mssql_connection_url)


def read_sql_queries(query_path_list:List[str]) -> str:
    """
    Чита
    param: query_path_list - список текстовых файлов с sql запросами в валидном порядке
    returns: query_source_text - сводный sql запрос с параметрами
    """
    query_source_text = ''
    for path in query_path_list:
        with open(path, 'r', encoding='utf-8') as f:
            query_source_text += f.read()
            f.close()
    return query_source_text



