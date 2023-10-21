import streamlit as st
import snowflake.connector
import joblib
import gzip


# Snowflake ML model path
model_path = '@"TPCDS_XGBOOST"."DEMO"."ML_MODELS"/model.joblib.gz'

# Snowflake connection settings
snowflake_settings = {
    'user': 'SAISRINIVAS1',
    'password': 'Algodm123',
    'account': 'QXOMKSN-KIB31585',
    'warehouse': 'snowpark_opt_wh',
    'database': 'tpcds_xgboost',
    'schema': 'demo',
}

# Function to retrieve data from Snowflake
def get_test_data():
    conn = snowflake.connector.connect(**snowflake_settings)
    cursor = conn.cursor()

    # Replace with SQL query to fetch test data from Snowflake
    query = "SELECT * FROM TPCDS_XGBOOST.DEMO.TEMP_TEST limit 200"
    cursor.execute(query)
    test_data = cursor.fetchall()

    conn.close()

    return test_data

# Function to load the model from Snowflake
def load_model_from_snowflake():
    conn = snowflake.connector.connect(**snowflake_settings)
    cursor = conn.cursor()

    # Replace with SQL query to fetch the model from Snowflake
    query = f"SELECT MODEL.PREDICT(USING '{model_path}', temp_input.*) FROM temp_input"

    cursor.execute(query)
    model_binary = cursor.fetchone()[0]

    with gzip.open(model_binary, 'rb') as model_file:
        model = joblib.load(model_file)

    conn.close()

    return model

# Streamlit app
st.title('Snowflake Model Deployment for Testing')

st.write('Retrieving test data from Snowflake...')
test_data = get_test_data()

# Check if test data is available
if test_data:
    st.write('Test data retrieved successfully.')

    st.write('Loading the model from Snowflake...')
    predictions = run_model_from_snowflake(test_data)

    st.write('Model loaded successfully.')

    st.write('Making predictions on the test data...')

    # You'll need to adapt this part to preprocess and make predictions on your test data
    #predictions = model.predict(test_data)

    st.write('Predictions:', predictions)

else:
    st.write('Test data not available.')

# Run the Streamlit app
if __name__ == '__main__':
    st.run()
