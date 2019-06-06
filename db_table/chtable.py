from clickhouse_driver.client import Client
from db_table.ch_sql_queries import *
import pandas as pd
import sys
from global_constants import *


# Добавить исторические данные следующим образом:
# Выкачивать датафрейм вида SKU-RequiredFeature-Дата
# Потом парсить этот датафрейм и группировать по дням в питоне
class ClickHouse:
    def __init__(self, day=1, initialize_columns=[1, 1]):
        self.ckhouse_ip = 'b.clickhouse.s.o3.ru'
        self.client = Client(self.ckhouse_ip)
        self.exception_load = "Can't load %s"
        self.day = day
        self.initialize_columns = initialize_columns

    def get_feature(self, feature_id):
        if feature_id not in ch_sql_queries:
            raise Exception("That feature doesn't exist")
        try:
            str_format_days_tuple = tuple([self.day] * ch_sql_queries[feature_id][1])
            return pd.DataFrame(self.client.execute(ch_sql_queries[feature_id][0] % str_format_days_tuple))
        except:
            raise Exception(self.exception_load % ch_features[feature_id])

    def get_result(self):
        features = []
        for feature_id in range(len(self.initialize_columns)):
            if feature_id == 0:
                pass
            features.append(self.get_feature(feature_id))
            features[-1].columns = ['SKU', ch_features[feature_id]]
            # встречаются SKU, которые не влезают в python int64, поэтому их отсекаем.
            # они не важны, т.к. мы матчим эту таблицу с BeeEye таблицей, а в ней все SKU влезают
            features[-1]['SKU'] = pd.to_numeric(features[-1]['SKU'], errors='coerce')
            features[-1]['SKU'] = features[-1][features[-1]['SKU'] < sys.maxsize]
            features[-1] = features[-1].dropna(axis=0, subset=['SKU'])
            features[-1]['SKU'] = features[-1]['SKU'].astype(dtype='int64')
        for i in range(1, len(features)):
            features[0] = pd.merge(features[0], features[1], on="SKU", how="outer")
        return features[0]

    def __del__(self):
        self.client.disconnect()
