import pyodbc
import pandas as pd
from bi_sql_queries import *


class BeeEye:
    def __init__(self, days=[0, 1, 2], elasticity_SKU=False, initialize_columns=[1, 1, 1, 0, 0]):
        try:
            self.days = []
            self.server = 'bidb04z1.o3.ru'
            self.database = 'BeeEye'
            self.username = 'spros'
            self.password = 'spros'
            self.conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password
            self.skuInit = False
            self.table = {0: ["#SKU", "SKU"], 1: ["#Sales", "Sales"],
                          2: ["#Price", "BasePrice", "WebPrice", "StartPrice"],
                          3: ["#Review", "Rate", "ReviewQty"],
                          4: ["#Size", "Width", "Height", "Depth", "Weight", "VolumeLiter"]}
            self.days = days
            self.initializeColumns = initialize_columns
            self.exception_create = "Can't create temporary table %s"
            self.exception_load = "Can't load temporary table %s"
            self.pyodbc_conn = pyodbc.connect(self.conn_info)
            self.cursor = self.pyodbc_conn.cursor()
            self.elasticity_SKU = elasticity_SKU
        except:
            raise Exception("Can't connect to the database")

    def __set_sku(self):
        if self.elasticity_SKU:
            self.cursor.execute("""SELECT sku_elas.SKU, ItemTypeHierarchyPrx.Type AS CatType, BrandPrx.Name AS BrandName, Item.ID AS ItemID
                                   INTO #SKU
                                   FROM zzzTemp.dbo.NK_SKU_elasticity sku_elas
                                   LEFT JOIN BeeEye.dbo.Item ON Item.RezonItemID = sku_elas.SKU
                                   LEFT JOIN BeeEye.dbo.BrandPrx ON Item.BrandID = BrandPrx.ID
                                   LEFT JOIN BeeEye.dbo.ItemTypeHierarchyPrx
                                   ON ItemTypeHierarchyPrx.TypeID = Item.ItemGroupID""")
            self.cursor.commit()
            self.skuInit = True
            return
        if self.skuInit:
            return
        try:
            self.cursor.execute("""drop table if exists #SKU
            SELECT Item.RezonItemID AS SKU, ItemTypeHierarchyPrx.Type AS CatType, BrandPrx.Name AS BrandName, Item.ID as ItemID
            INTO #SKU
            FROM BeeEye.dbo.Item
            JOIN BeeEye.dbo.BrandPrx
            ON Item.BrandID = BrandPrx.ID
            JOIN BeeEye.dbo.ItemTypeHierarchyPrx
            ON ItemTypeHierarchyPrx.TypeID = Item.ItemGroupID
            WHERE Item.EnabledForSale = 1 AND Item.FreeQty > 0""")
            self.cursor.commit()
            self.skuInit = True
        except:
            raise Exception((self.exception_create % self.table[0][0]))

    def get_sku(self):
        "Возвращает sql-строку для получения всех SKU, которые продаются на сайте"
        try:
            self.__set_sku()
            return self.cursor.execute("""select * from #SKU""")
        except:
            raise Exception((self.exception_load % self.table[0][0]))

    def __set_feature(self, id):
        if not self.skuInit:
            raise Exception("SKU table is not initialized")
        if id not in self.table:
            raise Exception("that feature doesn't exist")
        try:
            if self.table[id][0] in bi_historical_tables:
                for day in self.days:
                    if day == 0:
                        self.cursor.execute(bi_sql_queries[id][0] % (day, day, day))
                        self.cursor.commit()
                    else:
                        self.cursor.execute((bi_sql_queries[id][1] % (day, day, day, day - 1, day)))
                        self.cursor.commit()
            else:
                self.cursor.execute(bi_sql_queries[id])
                self.cursor.commit()
        except:
            raise Exception(self.exception_create % self.table[id][0])

    def get_feature(self, id):
        try:
            sql_query = "select * from %s" % self.table[id][0]
            return pd.read_sql(sql_query, self.pyodbc_conn)
        except:
            raise Exception(self.exception_load % self.table[id][0])

    def get_result_query(self):
        query = """drop table if exists #result\nselect """
        group_by_columns = """"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    query += "#SKU.SKU"
                    group_by_columns += "#SKU.SKU"
                    continue
                for j in range(1, len(self.table[i])):
                    if self.table[i][0] in bi_historical_tables:
                        for day in self.days:
                            query += ", %s%s.%s%s" % (self.table[i][0], day, self.table[i][j], day)
                            group_by_columns += ", %s%s.%s%s" % (self.table[i][0], day, self.table[i][j], day)
                    else:
                        query += ", %s.%s" % (self.table[i][0], self.table[i][j])
                        group_by_columns += ", %s.%s" % (self.table[i][0], self.table[i][j])
        query += """\ninto #result\nfrom #SKU\n"""
        for i in range(len(self.initializeColumns)):
            if self.initializeColumns[i]:
                if i == 0:
                    continue
                if self.table[i][0] in bi_historical_tables:
                    for day in self.days:
                        query += "left join %s%s on %s%s.SKU=%s.SKU\n" % (
                            self.table[i][0], day, self.table[i][0], day, self.table[0][0])
                else:
                    query += "left join %s on %s.SKU=%s.SKU\n" % (self.table[i][0], self.table[i][0], self.table[0][0])
        query += "group by " + group_by_columns
        return query

    def __set_result(self):
        try:
            self.__set_sku()
            for i in range(1, len(self.initializeColumns)):
                if self.initializeColumns[i]:
                    self.__set_feature(i)
            #             self.__set_review() почему-то не может создать эту таблицу
            print(self.get_result_query())
            self.cursor.execute(self.get_result_query())
        except:
            raise Exception("Can't create #result table")

    def get_result(self):
        try:
            self.__set_result()
            print(self.get_result_query)
            return pd.read_sql("""SELECT * FROM #result""", self.pyodbc_conn)
        except:
            raise Exception("Can't load #result table")

    def __del__(self):
        self.cursor.close()
