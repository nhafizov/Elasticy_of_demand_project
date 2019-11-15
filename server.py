from dataloader import *
import pandas as pd
from global_constants import *
from sklearn.ensemble import RandomForestRegressor as RFR
from sklearn.metrics import mean_absolute_error as MAE
import pickle
from datetime import datetime
import numpy as np
from data_io_into_db import *

def get_data_for_model(hist_days, N=1):
    # т.к. на текущий день данных нет, будем обучать на train, состоящем из данных за 2, ..., n дней назад
    # а тестировать на test, данные в котором лежат за вчерашний день (т.е. за 1 день назад)
    test = DataLoader(hist_days=[1], N=N).get_result()
    tmp_train = []
    tmp_train.append(DataLoader(hist_days=hist_days, N=N).get_result())
    # соединяем загруженные данн
    train = pd.concat(tmp_train, ignore_index=True)
    # рандомно оставляем Sales==None в количестве train['Sales' > 0].shape[0]
    train_more0 = train[train['Sales'] > 0]
    train_0 = train[np.isnan(train['Sales'])].sample(n=train_more0.shape[0])
    # сохраняем как время_train.csv/test.csv
    train.to_csv(datetime.today().strftime('%Y-%m-%d') + train_csv_path, index=False, header=train.columns)
    test.to_csv(datetime.today().strftime('%Y-%m-%d') + test_csv_path, index=False, header=test.columns)
    return train, test


def prepare_data(dataframe):
    dataframe['Sales'].fillna(0, inplace=True)
    fill = dataframe.apply(lambda s: s.mode()[0] if s.dtype == 'object' else s.median(), axis=0)
    dataframe = dataframe.fillna(value=fill)
    return dataframe


def save_model(model):
    with open(datetime.today().strftime('%Y-%m-%d') + trained_model_pickle_path, 'wb') as file:
        pickle.dump(model, file)


def get_model(hist_days):
    if len(hist_days) == 0:
        return
    # загружаем и подготавливаем данные
    train, test = get_data_for_model(hist_days)
    train, test = prepare_data(train), prepare_data(test)
    # некоторые столбцы пока работают криво хз почему
    model_columns = ['Sales']
    # делим на train и test MAE0, MAE>0
    x_train = train[[x for x in train.columns if x not in model_columns]]
    y_train = train['Sales']
    test_0 = test[test['Sales'] == 0]
    x_test_0 = test_0[[x for x in test_0.columns if x not in model_columns]]
    y_test_0 = test_0['Sales']
    test_more_0 = test[test['Sales'] > 0]
    x_test_more_0 = test_more_0[[x for x in test_more_0.columns if x not in model_columns]]
    y_test_more_0 = test_more_0['Sales']
    # обучаем модель
    model = RFR(criterion='mae')
    model.fit(x_train, y_train)
    # Сохраняем
    save_model(model)
    y_predicted_0 = model.predict(x_test_0)
    print('MAE_0:', MAE(y_test_0, y_predicted_0))
    y_predicted_more_0 = model.predict(x_test_more_0)
    print('MAE_>0:', MAE(y_test_more_0, y_predicted_more_0))
    return model


get_data_for_model(hist_days=range(3, 4), N=1)
