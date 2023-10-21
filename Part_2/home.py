import streamlit as st

# Page title and header
st.title("Welcome to Our Snowflake Journey!")

st.write("Each page on left we implemented different kinds of appliactions in each domain")
# Some fun content
st.markdown("### Why Snowflake?")
st.write("Snowflake is a fantastic platform for data analytics and machine learning, and we're excited to show you what we've learned.")

st.markdown("### About Us")
st.write("We are a team of data enthusiasts on a journey to explore Snowflake applications and learn new things. Join us on this adventure!")


st.write("Snowflake applications are cool, just like these snow boots! ❄️👢")

# Footer
st.write("© 2023, Team5")

if st.button("References"):
    st.success("https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main")
    st.balloons()


# Customize your page's layout, colors, and more if needed
