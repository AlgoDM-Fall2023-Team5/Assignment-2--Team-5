import json
import pandas as pd
from snowflake.snowpark.functions import udf
import snowflake.snowpark.types as T
import streamlit as st
from snowflake.snowpark import functions as F
from snowflake.snowpark import version as v
from snowflake.snowpark.session import Session
import joblib
from cachetools import cached

# Ensure that your credentials are stored in creds.json
with open('creds.json') as f:
    data = json.load(f)
    USERNAME = data['user']
    PASSWORD = data['password']
    SF_ACCOUNT = data['account']
    SF_WH = data['warehouse']

CONNECTION_PARAMETERS = {
   "account": SF_ACCOUNT,
   "user": USERNAME,
   "password": PASSWORD,
}

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
session.use_warehouse('snowpark_opt_wh')
session.use_database('tpcds_xgboost')
session.use_schema('demo')

test_sdf = session.table('temp_test')
feature_cols = session.table('temp_test').columns
feature_cols.remove('TOTAL_SALES')
target_col = 'TOTAL_SALES'

@cached(cache={})
def load_model(model_path: str) -> object:
    from joblib import load
    model = load(model_path)
    return model

def udf_score_xgboost_model_vec_cached(df: pd.DataFrame) -> pd.Series:
    import os
    import sys
    # file-dependencies of UDFs are available in snowflake_import_directory
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    model_name = 'model.joblib.gz'
    model = load_model(import_dir+model_name)
    df.columns = feature_cols
    scored_data = pd.Series(model.predict(df))
    return scored_data

# Register UDF
udf_clv = session.udf.register(func=udf_score_xgboost_model_vec_cached, 
                               name="TPCDS_PREDICT_CLV", 
                               stage_location='@ML_MODELS',
                               input_types=[T.FloatType()]*len(feature_cols),
                               return_type = T.FloatType(),
                               replace=True, 
                               is_permanent=True, 
                               imports=['@ML_MODELS/model.joblib.gz'],
                               packages=['pandas',
                                         'xgboost',
                                         'joblib',
                                         'cachetools'], 
                               session=session)


test_sdf_w_preds = test_sdf.with_column('PREDICTED', udf_clv(*feature_cols))
st.write(test_sdf_w_preds.limit(2).to_pandas())