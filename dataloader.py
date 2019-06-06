from db_table.bitable import BeeEye
from db_table.chtable import ClickHouse
import pandas as pd
import numpy as np


class DataLoader:
    def __init__(self, hist_days, N=1, bi_features=None, ch_features=None, predict_SKU=False):
        self.predict_SKU = predict_SKU
        self.hist_days = hist_days
        self.N = N

    def get_result(self):
        "Скачивает данные из Clickouse/BeeEye/... и объединяет их с помощью left join по SKU из BeeEye"
        results = []
        cur_day = 0
        # выкачивает данные для указанных дней
        try:
            for day in self.hist_days:
                cur_day = day
                bi_result = BeeEye(day=day, N=self.N).get_result()
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
