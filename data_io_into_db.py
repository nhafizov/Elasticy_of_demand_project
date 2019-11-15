from sqlalchemy import create_engine
import pyodbc
import urllib.parse
from db_table.bitable import BeeEye
from db_table.chtable import ClickHouse
import pandas as pd
import numpy as np

elasticity_table = "NK_SKU_result_elasticity"
optimal_price_table = "NK_SKU_optimal_prices"
mae_table = "NK_SKU_mae"


def save_data_in_db(dataframe, mode, zzzTempTableName=None):
    "Сохраняет функцию в таблицу в BeeEye"
    server = '#'
    database = '#'
    username = '#'
    password = '#'
    conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    params = urllib.parse.quote_plus(conn_info)
    engine = create_engine("mssql+pymssql://spros:spros@bidb04z1.o3.ru/zzzTemp")
    print(dataframe)
    # сохраняет/добавляет в таблицу с эластичностью
    if mode == 'elasticity':
        zzzTempTableName = elasticity_table
    # сохраняет/добавляет в таблицу с оптимальными ценами
    elif mode == 'optimal':
        zzzTempTableName = optimal_price_table
    # сохраняет/добавляет в таблицу с mae по всем товарами
    elif mode == 'error':
        zzzTempTableName = mae_table
    try:
        dataframe.to_sql(name=zzzTempTableName, con=engine, if_exists='replace', index=False, )
    except:
        raise Exception("Can't' save %s table" % zzzTempTableName)


def get_data_from_db(hist_days, N=1, bi_features=None, ch_features=None, predict_SKU=False):
    "Скачивает данные из Clickouse/BeeEye/... и объединяет их с помощью left join по SKU из BeeEye"
    results = []
    cur_day = 0
    # выкачивает данные для указанных дней
    try:
        for day in hist_days:
            cur_day = day
            bi_result = BeeEye(day=day, N=N).get_result()
            ch_result = ClickHouse(day=day).get_result()
            # здесь делаем left join
            result = pd.merge(bi_result, ch_result, on='SKU', how='left')
            if day != 1:
                train_more0 = result[result['Sales'] > 0]
                train_0 = result[np.isnan(result['Sales'])].sample(n=train_more0.shape[0])
                result = train_more0.append(train_0)
            results.append(result)
            print(cur_day)
        # и делаем один датафрейм из всех
        return pd.concat(results, ignore_index=True)
    # в случае некоторых ошибок (например, MemoryError) это не помогает
    except Exception as ex:
        print('day ', cur_day)
        print('Exception: ', ex)
        if len(results) == 1:
            return results[-1]
        else:
            return pd.concat(results, ignore_index=True)
