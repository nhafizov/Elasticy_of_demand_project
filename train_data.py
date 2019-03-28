from bitable import BeeEye
import pandas as pd

columns_template = \
    ['SKU', 'Sales', 'BasePrice', 'WebPrice', 'StartPrice', 'Width', 'Height', 'Depth', 'Weight', 'VolumeLiter']


def get_train_data(hist_days):
    test = BeeEye(days=[1]).get_result()
    test.columns = columns_template
    tmp_train = []
    for day in hist_days:
        tmp_train.append(BeeEye(days=[day]).get_result())
        tmp_train[-1].columns = columns_template
    train = pd.concat(tmp_train, ignore_index=True)
    train.to_csv('train.csv', index=False, header=train.columns)
    test.to_csv('test.csv', index=False, header=test.columns)
    return train, test


get_train_data(hist_days=range(2, 3))
