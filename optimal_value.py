import pandas as pd
from bitable import BeeEye

columns_template = \
    ['SKU', 'Sales', 'BasePrice', 'WebPrice', 'StartPrice', 'Width', 'Height', 'Depth', 'Weight', 'VolumeLiter']
train_csv_path = 'train.csv'
test_csv_path = 'test.csv'


def get_optimal_values(model=None):
    predict_elasticity_data = BeeEye(predict_SKU=True, days=[1]).get_result()
    sku = predict_elasticity_data['SKU']
    predict_elasticity_data.drop(['SKU', 'Sales1'], inplace=True, axis=1)
    optimal_price_result = None
    # print(predict_elasticity_data) ########
    elasticity_df = []
    for i in range(0, 101):
        print(i)
        tmp_predict_elasticity_data = predict_elasticity_data.copy(deep=True)
        tmp_predict_elasticity_data['WebPrice1'] *= (1 + i / 100)
        tmp_predict_elasticity_data.drop('BasePrice1', inplace=True, axis=1)
        tmp_predict_elasticity_data['PredictedSales'] = np.floor(model.predict(tmp_predict_elasticity_data))
        tmp_predict_elasticity_data['SKU'] = sku
        tmp_predict_elasticity_data['profit'] = tmp_predict_elasticity_data['PredictedSales'] * (
                    tmp_predict_elasticity_data['WebPrice1'] - predict_elasticity_data['BasePrice1'])
        elasticity_df.append(tmp_predict_elasticity_data)
    print("here")
    result = pd.concat(elasticity_df, ignore_index=True)
    return result
