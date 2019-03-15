import pyodbc
from bitable import BeeEye
from chtable import ClickHouse


# Добавить ClickHouse
class SKU_collector:
    def __init__(self, features=None, bi_table=None, ch_table=None):
        self.server = 'bidb04z1.o3.ru'
        self.database = 'BeeEye'
        self.username = 'spros'
        self.password = 'spros'
        self.conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password
        self.pyodbc_conn = pyodbc.connect(self.conn_info)
        self.cursor = self.pyodbc_conn.cursor()
        self.dataframe = None

    def load_sku(self):
        BI_table = BeeEye(elasticity_SKU=True)
        self.dataframe = BI_table.get_result()

    def get_data(self):
        self.load_sku()
        return self.dataframe

    def __del__(self):
        self.cursor.close()
