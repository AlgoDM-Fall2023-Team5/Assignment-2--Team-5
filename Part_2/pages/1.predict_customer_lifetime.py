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
import plotly.express as px
import secrets



chart_types = ["Bar Chart","Pie Chart","Scatter Plot","Line Chart"]
#======================================================================
# bar chart 
def bar_chart_maker(df):
    col1,col2 = st.columns([1,1])
    try:
        with col1:
            x_axis = st.selectbox("Select X_axis: ",df.columns)
        with col2:
            y_axis = st.selectbox("Select Y axis :",df.columns)
        
        fig = px.bar(df, x=x_axis, y=y_axis, title=f'Bar Chart: {x_axis} vs {y_axis}')
        st.plotly_chart(fig)
    except Exception as e:
        st.error("Please Check your Bar chart",e)

#=======================================================================
#pie chart maker
def pie_chart_maker(df):
    try:
        column = st.selectbox("Select a Column :",df.columns)
        
        fig = px.pie(df, names=column, values=column, title=f'Pie Chart: {column}')
        st.plotly_chart(fig)
    except Exception as e:
        st.error("please check you pie chart",e)
#========================================================================
# scatter plot maker
def scatter_plot_maker(df):
    col1,col2 = st.columns([1,1])
    try:
        with col1:
            x_axis = st.selectbox("Select X_axis: ",df.columns)
        with col2:
            y_axis = st.selectbox("Select Y axis :",df.columns)
        
        fig = px.scatter(df, x=x_axis, y=y_axis, title=f'Scatter Plot: {x_axis} vs {y_axis}')
        st.plotly_chart(fig)
    except Exception as e:
        st.error("Please Check your Scatter Plot",e)
#=========================================================================
# line chart
def line_chart_maker(df):
    col1,col2 = st.columns([1,1])
    try:
        with col1:
            x_axis = st.selectbox("Select X_axis: ",df.columns)
        with col2:
            y_axis = st.selectbox("Select Y axis :",df.columns)
        
        fig = px.line(df, x=x_axis, y=y_axis, title=f'Line Chart: {x_axis} vs {y_axis}')
        st.plotly_chart(fig)
    except Exception as e:
        st.error("Please Check your Line chart",e)
#=========================================================================
#Chart maker
def chart_maker(df):
    chart_selection = st.selectbox("Select Chart Type",chart_types)

    # chart display based on the selection
    if chart_selection == "Bar Chart" and len(df) > 0:
        bar_chart_maker(df)
    elif chart_selection == "Pie Chart" and len(df) > 0:
        pie_chart_maker(df)
    elif chart_selection == "Scatter Plot" and len(df) > 0:
        scatter_plot_maker(df)
    elif chart_selection == "Line Chart" and len(df)>0:
        line_chart_maker(df)
    else:
        st.write("Empty Table Returned")

#======================================================================
# Ensure that your credentials are stored in creds.json
# Load credentials from the secrets.toml file
snowflake_access = st.secrets["1_credentials"]

# Access your credentials
USERNAME = st.secrets["3_credentials"]['user']
PASSWORD = st.secrets["3_credentials"]['password']
SF_ACCOUNT = st.secrets["3_credentials"]['account']
#SF_WH =st.secrets["1_credentials"]['warehouse']



CONNECTION_PARAMETERS = {
   "account": SF_ACCOUNT,
   "user": USERNAME,
   "password": PASSWORD,
}
#=============================================================================
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

st.title('Predict Customer Lifetime Value')

length_button = st.number_input("Enter an integer:", step=1)

check_button = st.checkbox("Show DataFrame")
df = test_sdf_w_preds.limit(length_button).to_pandas()
if check_button:
    st.write(df)

chart_maker(df)