import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,text
import plotly.express as px
import datetime

snowflake_url = st.secrets["forecasting_snowflake"]["url"]

chart_types = ["Bar Chart","Pie Chart","Scatter Plot","Line Chart"]
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

#=========================================
# used to forecast based on the input
@st.cache_data
def forecast(input_parameter):

    try:
        with engine.connect() as conn:
            sql_query = f"CALL impressions_forecast!FORECAST(FORECASTING_PERIODS => {input_parameter})"
            result = conn.execute(sql_query)

            

            forecast_data = pd.DataFrame(result.fetchall(), columns=result.keys())
            # Display the forecasted results

    except Exception as e:
        st.error(f'Error: {e}')
    finally:
        # Close the connection
        conn.close()
    return forecast_data

#============================================
# Anamoly detection model
@st.cache_data
def Anamoly(date_input_button,impressions_button):
    #formatted_date = date_input_button.replace('/', '-'))
    query = f"""CALL impression_anomaly_detector!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('select ''{date_input_button}''::timestamp as day, {impressions_button} as impressions'),
  TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'impressions');"""
    try:
        with engine.connect() as conn:

            result = conn.execute(query)
            
            anamoly_data = pd.DataFrame(result.fetchall(), columns=result.keys())

    except Exception as e:
        st.error(f'Error: {e}')
    finally:
        # Close the connection
        conn.close()
    return anamoly_data







# Initializing the engine
engine = create_engine(snowflake_url)


radio_button = st.sidebar.radio("Select Mode", ["Forecasting Model", "Anomaly Detection"])

#### if block
if radio_button == "Forecasting Model":
    st.title("Forecasting Model")
    col1,col2 =st.columns(2)

    with col1:
        input_parameter = st.slider('Input Parameter', min_value=0, max_value=100, value=50)
    #submit_button = st.button('Submit')
    # calling the forecast function and stored in dataframe
    df = forecast(input_parameter)
    with col2:
        st.write("")
        st.write("")
        check_button = st.checkbox("Show DataFrame") # check button

    # Check box to show the dataframe
    if check_button:
        st.dataframe(df)
    # Chartmaker used to draw charts based on the user's need
    chart_maker(df)
    
####   else block
else:
    st.title("Anomaly Detection")
    col3,col4 = st.columns(2)

    with col3:
        date_input_button = st.date_input("Date",datetime.date(2022, 12, 6),format="YYYY-MM-DD")

    with col4:
        impressions_button = st.number_input("Enter an integer:", step=1)
    
    Ana_out = Anamoly(date_input_button,impressions_button)
