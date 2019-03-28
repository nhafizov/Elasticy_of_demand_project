from collector import SKU_collector
from bitable import BeeEye
from sklearn.ensemble import RandomForestRegressor as RFR
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error as mae
import numpy
import pandas

data = BeeEye().get_result()
print(data)