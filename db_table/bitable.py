import pyodbc
import pandas as pd
from db_table.bi_sql_queries import *


class BeeEye:
    def __init__(self, day=1, N=1, predict_SKU=False, initialize_columns=None):
        try:
            self.server = 'bidb04z1.o3.ru'
            self.database = 'BeeEye'
            self.username = 'spros'
            self.password = 'spros'
            self.conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password
            self.skuInit = False
            # день, для которого скачивается SKU с признаками
            self.day = day
            # промежуток N, за которые мы считаем определенные в bi_sql_queries historical_features признаки
            self.N = N
            # признаки, которые будут юзаться
            if initialize_columns is not None:
                self.initialize_columns = initialize_columns
            else:
                self.initializeColumns = [sql_query[3] for sql_query in bi_sql_queries.values()]
            # скачивать данные для предсказания или обучения
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
            # self.predict_SKU == 1 тогда и только тогда, когда таблица со сторонними SKU непуста
            # другими словами, в данном случае мы выкачиваем данные для прогноза по этим SKU
            # self.predict_SKU == 0 когда мы хотим выкачить данные для обучения модели
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
            # генерируем кол-во %s day (day, day, ...), которые нужно вставить в sql-запрос
            str_format_days = [self.day] * bi_sql_queries[feature_id][1]
            # если feature является историческим
            # то к предпоследнему элементу добавляем промежуток N
            # тут он равен 0, если признак не использует N, иначе self.N * 1/2/3/...
            if bi_sql_queries[feature_id][2] > 0:
                str_format_days[-2] += self.N * bi_sql_queries[feature_id][2]
            # выполняем запрос
            self.cursor.execute((bi_sql_queries[feature_id][0] % tuple(str_format_days)))
            self.cursor.commit()
        except:
            raise Exception(self.exception_create % bi_table[feature_id][0])

    def get_feature(self, feature_id):
        "Каждую временную табличку можно выкачить отдельно"
        # эта функция нужна на тот случай,
        # если понадобится спарк(итоговая таблица со всеми признаками окажется слишком большой).
        try:
            sql_query = "select * from %s" % bi_table[feature_id][0]
            return pd.read_sql(sql_query, self.pyodbc_conn)
        except:
            raise Exception(self.exception_load % bi_table[feature_id][0])

    # генерирует строку для создания временной таблицы, в которую объединяются все остальные
    def __get_result_query(self):
        "Возвращает строку для создания финальной таблицы в BeeEye"
        query = """select """
        group_by_columns = """"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    query += "#SKU.SKU"
                    group_by_columns += "#SKU.SKU"
                    continue
                for j in range(1, len(bi_table[i])):
                    query += ", %s.%s" % (bi_table[i][0], bi_table[i][j])
                    group_by_columns += ", %s.%s" % (bi_table[i][0], bi_table[i][j])
        query += """\nfrom #SKU\n"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    continue
                else:
                    query += "left join %s on %s.SKU=%s.SKU\n" % (bi_table[i][0], bi_table[i][0], bi_table[0][0])
        query += "group by " + group_by_columns
        return query

    def get_result(self):
        "Возвращает объединенную таблицу, состоящую из всех фичей в BeeEye"
        try:
            self.__set_sku()
            for feature_id in range(1, len(self.initializeColumns)):
                if self.initializeColumns[feature_id]:
                    self.__set_feature(feature_id)
            # print(self.__get_result_query())
            return pd.read_sql(self.__get_result_query(), self.pyodbc_conn)
        except:
            raise Exception("Can't load result table")

    def __del__(self):
        self.cursor.close()


data = BeeEye(day=1, N=1).get_result()
print(data)