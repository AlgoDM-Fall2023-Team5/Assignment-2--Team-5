import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,text

snowflake_url = st.secrets["forecasting_snowflake"]["url"]


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






# Initializing the engine
engine = create_engine(snowflake_url)


radio_button = st.sidebar.radio("Select Mode", ["Forecasting Model", "Anomaly Detection"])


if radio_button == "Forecasting Model":
    st.title("Forecasting Model")
    col1,col2 =st.columns(2)
    with col1:
        input_parameter = st.slider('Input Parameter', min_value=0, max_value=100, value=50)
    #submit_button = st.button('Submit')

    df = forecast(input_parameter)
    with col2:
        check_button = st.checkbox("Show DataFrame") # check button


    if check_button:
        st.table(df)
    

else:
    st.title("Anomaly Detection")
