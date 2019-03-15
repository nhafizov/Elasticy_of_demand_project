from clickhouse_driver.client import Client
import ch_sql_queries

# Добавить исторические данные следующим образом:
# Выкачивать датафрейм вида SKU-RequiredFeature-Дата
# Потом парсить этот датафрейм и группировать по дням в питоне
class ClickHouse:
    def __init__(self, days=[1, 2, 3, 4]):
        self.ckhouse_ip = 'b.clickhouse.s.o3.ru'
        self.client = Client(self.ckhouse_ip)
        self.features = ['ViewsQty', 'CartQty']
        self.exception_load = "Can't load %s"
        self.days = days

    def __set_feature(self, id):
        pass

    def get_feature(self, id):
        if id not in ch_sql_queries.ch_sql_queries:
            raise Exception("That feature doesn't exist")
        try:
            if self.features[id] in ch_sql_queries.ch_historical_tables:
                for day in self.days:
                    self.client.execute(ch_sql_queries.ch_sql_queries[id] % (day, day))
        except:
            raise Exception(self.exception_load + self.features[id])

    def __del__(self):
        self.client.disconnect()
