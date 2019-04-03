import pandas as pd
from sklearn.ensemble import RandomForestRegressor as RFR
from global_constants import *
from bitable import BeeEye


def get_optimal_values(model=None):
    predict_elasticity_data = BeeEye(elasticity_SKU=True).get_result()
    sku = predict_elasticity_data['SKU']
    predict_elasticity_data.drop(['SKU', 'Sales'], inplace=True, axis=1)
    optimal_price_result = None
    elasticity_results = []
    for i in range(0, 101):
        tmp_predict_elasticity_data = predict_elasticity_data.copy(deep=True)
        tmp_predict_elasticity_data['WebPrice'] *= (1 + i / 100)
        