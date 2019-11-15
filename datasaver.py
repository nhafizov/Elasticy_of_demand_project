from sqlalchemy import create_engine
import pyodbc
import urllib.parse

elasticity_table = "NK_SKU_result_elasticity"
optimal_price_table = "NK_SKU_optimal_prices"


def save_result_in_db(dataframe, mode, zzzTempTableName=None):
    server = '#'
    database = '#'
    username = '#'
    password = '#'
    conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    params = urllib.parse.quote_plus(conn_info)
    engine = create_engine("mssql+pymssql://spros:spros@#.#.#/#")
    print(dataframe)
    if mode == 'elasticity':
        zzzTempTableName = elasticity_table
    elif mode == 'optimal':
        zzzTempTableName = optimal_price_table
    try:
        dataframe.to_sql(name=zzzTempTableName, con=engine, if_exists='replace', index=False, )
    except:
        raise Exception("Can't' save %s table" % zzzTempTableName)
