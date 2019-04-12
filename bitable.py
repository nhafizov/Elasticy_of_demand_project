import pyodbc
import pandas as pd
from bi_sql_queries import *


class BeeEye:
    def __init__(self, days=[1], predict_SKU=False, initialize_columns=[1, 1, 1, 1]):
        try:
            self.server = 'bidb04z1.o3.ru'
            self.database = 'BeeEye'
            self.username = 'spros'
            self.password = 'spros'
            self.conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password
            self.skuInit = False
            # дни, для которых нужно сделать прогноз
            self.days = days
            # признаки, которые будут юзаться
            self.initializeColumns = initialize_columns
            self.predict_SKU = predict_SKU
            self.exception_create = "Can't create temporary table %s"
            self.exception_load = "Can't load temporary table %s"
            self.pyodbc_conn = pyodbc.connect(self.conn_info)
            self.cursor = self.pyodbc_conn.cursor()
            self.connected = True
        except:
            raise Exception("Can't connect to the database")

    # Функция для инициализации SKU написана отдельно по следующим причинам:
    # 1) SKU - это не фича
    # 2) таблица SKU является основной таблицой для сбора признаков (без нее фичи не скачать)
    # 3) Нужно гарантировать создание этой таблицы
    def __set_sku(self):
        "Инициализирует временную таблицу SKU"
        if self.skuInit:
            return
        try:
            # self.predictSKU == 1 тогда и только тогда, когда таблица со сторонними SKU непуста
            # другими словами, в данном случае мы выкачиваем данные для прогноза по этим SKU
            # self.predictSKU == 0 когда мы хотим выкачить данные для обучения модели
            self.cursor.execute(bi_sku_initialized_query[self.predict_SKU])
            self.cursor.commit()
            self.skuInit = True
        except:
            raise Exception((self.exception_create % bi_table[0][0]))

    def get_sku(self):
        "Возвращает sql-строку для получения всех SKU, которые продаются на сайте"
        try:
            self.__set_sku()
            return self.cursor.execute("""select * from #SKU""")
        except:
            raise Exception((self.exception_load % bi_table[0][0]))

    def __set_feature(self, feature_id):
        "Создает временную таблицу bi_table[feature_id][0] в BeeEye вида (SKU, bi_table[feature_id][1:])"
        if not self.skuInit:
            raise Exception("SKU table is not initialized")
        if feature_id not in bi_table:
            raise Exception("that feature doesn't exist")
        try:
            # если feature является историческим
            if bi_table[feature_id][0] in bi_historical_tables:
                # то создаем временные таблички для всех feature на все дни в self.days
                for day in self.days:
                    # нулевой день не вытащить, так как данные загружаются в 3 часа ночи на след день
                    if day == 0:
                        # тут мб вообще это не нужно, не знаю пока
                        continue
                    # генерируем кол-во %s day (day, day, ...)
                    str_format_days_tuple = [day] * bi_sql_queries[feature_id][1]
                    self.cursor.execute((bi_sql_queries[feature_id][0] % tuple(str_format_days_tuple)))
                    self.cursor.commit()
            else:
                self.cursor.execute(bi_sql_queries[feature_id][0])
                self.cursor.commit()
        except:
            raise Exception(self.exception_create % bi_table[feature_id][0])

    def get_feature(self, feature_id):
        "Каждую временную табличку можно выкачить отдельно"
        try:
            sql_query = "select * from %s" % bi_table[feature_id][0]
            return pd.read_sql(sql_query, self.pyodbc_conn)
        except:
            raise Exception(self.exception_load % bi_table[feature_id][0])

    # генерирует строку для создания временной таблицы, в которую объединяются все остальные
    def __get_result_query(self):
        "Возвращает строку для создания финальной таблицы в BeeEye"
        query = """drop table if exists #result\nselect """
        group_by_columns = """"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    query += "#SKU.SKU"
                    group_by_columns += "#SKU.SKU"
                    continue
                for j in range(1, len(bi_table[i])):
                    if bi_table[i][0] in bi_historical_tables:
                        for day in self.days:
                            query += ", %s%s.%s%s" % (bi_table[i][0], day, bi_table[i][j], day)
                            group_by_columns += ", %s%s.%s%s" % (bi_table[i][0], day, bi_table[i][j], day)
                    else:
                        query += ", %s.%s" % (bi_table[i][0], bi_table[i][j])
                        group_by_columns += ", %s.%s" % (bi_table[i][0], bi_table[i][j])
        query += """\ninto #result\nfrom #SKU\n"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    continue
                if bi_table[i][0] in bi_historical_tables:
                    for day in self.days:
                        query += "left join %s%s on %s%s.SKU=%s.SKU\n" % (
                            bi_table[i][0], day, bi_table[i][0], day, bi_table[0][0])
                else:
                    query += "left join %s on %s.SKU=%s.SKU\n" % (bi_table[i][0], bi_table[i][0], bi_table[0][0])
        query += "group by " + group_by_columns
        return query

    # создает таблицу
    def __set_result(self):
        try:
            self.__set_sku()
            for i in range(1, len(self.initializeColumns)):
                if self.initializeColumns[i]:
                    self.__set_feature(i)
            # print(self.__get_result_query())
            self.cursor.execute(self.__get_result_query())
        except:
            raise Exception("Can't create temporary table #Result")

    def get_result(self):
        try:
            self.__set_result()
            return pd.read_sql("""SELECT * FROM #result""", self.pyodbc_conn)
        except:
            raise Exception("Can't load temporary table #result")

    def __del__(self):
        self.cursor.close()
