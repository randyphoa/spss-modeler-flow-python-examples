import sys
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "ibm-watson-machine-learning", "scikit-learn", "lightgbm", "--no-input"])

import spss.pyspark.runtime
from pyspark.sql.types import *

import joblib
import numpy as np
import lightgbm as lgb
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import make_column_selector
from sklearn.compose import make_column_transformer
from sklearn.preprocessing import OneHotEncoder

cxt = spss.pyspark.runtime.getContext()
df = cxt.getSparkInputData().toPandas()

target = "MortgageDefault"
y = df[target]
X = df.drop(target, axis=1)

ct = make_column_transformer(
    (OneHotEncoder(), make_column_selector(dtype_include=object)),
    remainder="passthrough"
)

pipeline = Pipeline(steps=[("transform", ct), ("clf", lgb.LGBMClassifier(objective="binary"))])

pipeline.fit(X, y)
joblib.dump(pipeline, "/tmp/pipeline.pkl")

