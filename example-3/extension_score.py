import sys
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "ibm-watson-machine-learning", "scikit-learn", "lightgbm", "--no-input"])

import spss.pyspark.runtime
from pyspark.sql.types import *
from pyspark.sql import SQLContext

import joblib

cxt = spss.pyspark.runtime.getContext()
sqlContext = cxt.getSparkSQLContext()

target = "MortgageDefault"
prediction = f"$PRED-{target}"
probability = f"$PROB-{target}"

fieldList = [StructField(x.name, x.dataType, x.nullable) for x in cxt.getSparkInputSchema()]
fieldList.append(StructField(prediction, StringType(), nullable=False))
fieldList.append(StructField(probability, FloatType(), nullable=False))
outputSchema = StructType(fieldList)
cxt.setSparkOutputSchema(outputSchema)

if not cxt.isComputeDataModelOnly():
    df = cxt.getSparkInputData().toPandas()
    pipeline = joblib.load("/tmp/pipeline.pkl") 
    df[prediction] = pipeline.predict(df)
    df[probability] = pipeline.predict_proba(df)[:,-1]
    cxt.setSparkOutputData(sqlContext.createDataFrame(df))
    
