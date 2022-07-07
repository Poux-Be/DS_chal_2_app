# App file

# ----- Imports -----
import os
import folium
import requests
import datetime
import matplotlib
import snowflake.connector

import pandas as pd
import streamlit as st
import plotly.express as px
from urllib.error import URLError

# ------------------------
# ------ Main code -------
# ------------------------

# Variables
PATH = os.getcwd()

# Functions
# Fetch Snowflake data
def execute_sf_query_table(query):
    # Connect to Snowflake
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])

    with my_cnx.cursor() as my_cur:
        # Execute the query
        my_cur.execute(query)

        # Get the table header
        header = [x[0] for x in my_cur.description]

        # get the table rows
        rows = my_cur.fetchall()
        
    # Close the query
    my_cnx.close()

    # Return the query in a dataframe
    return(pd.DataFrame(rows, columns = header))

# Get a table in snowflake based on its name only
def get_table(table_name):
    return(execute_sf_query_table("select * from "+table_name+" limit 20"))

# Add a row into Snowflake - Not used
def insert_row_snowflake(new_fruit):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("insert into fruit_load_list values ('"+new_fruit+"')")
        return('Thanks for adding ' + add_my_fruit)

# ------------------------
# ----- Main display -----
# ------------------------
st.title("🎅 D&A Challenge - 2 🎅")

# Display the data received in a dataframe
st.header('Data received')
st.text('Here is a snapshot of the data provided for this exercise.')

# Query snowflake
# Add a button to query the fruit list
if st.button("Display the intial data"):
    st.dataframe(get_table("sales"))

# ------------------------
# Frist exercise, query the data to count the number of appartments sold between two dates
# ------------------------
# Exercise title
st.header('Frist query: Appartment sales between two dates 📆')

# Select the first date
d1 = st.date_input(
     "Study period first day",
     datetime.date(2020, 1, 1))

# Select the second date
d2 = st.date_input(
     "Study period last day",
     datetime.date(2020, 3, 31))

# Query snowflake
my_query_results = execute_sf_query_table("select transaction_date, local_type, sum(count(*)) over (partition by transaction_date, local_type) as daily_sales_count from sales_view where (transaction_date <= '"+d2.strftime('%Y-%m-%d')+"' and transaction_date >= '"+d1.strftime('%Y-%m-%d')+"') group by transaction_date, local_type order by transaction_date asc")

# Answer the exercise question
st.subheader(''+str(sum((my_query_results[my_query_results['LOCAL_TYPE']=='Appartement']['DAILY_SALES_COUNT'].to_list())))+' appartments have been sold during this period of time')

# Dataframe formatting to have a beautiful chart
fig = px.bar(my_query_results, x="TRANSACTION_DATE", y="DAILY_SALES_COUNT", color="LOCAL_TYPE", title="Daily sales from the "+d1.strftime('%Y-%m-%d')+" to the "+d2.strftime('%Y-%m-%d')+" per local type")
fig.show()

st.plotly_chart(fig)


# ------------------------
# Second exercise, get the ratio of sales per room number
# ------------------------
# Exercise title
st.header('Second query: Appartment sales per room number #️⃣')

# Query snowflake
my_query_results = execute_sf_query_table("select rooms_number, sum(count(*)) over (partition by rooms_number) as sales_count from sales_view where local_type='Appartement' group by rooms_number order by rooms_number asc")

# Answer the exercise question
fig2 = px.pie(my_query_results, values='SALES_COUNT', names='ROOMS_NUMBER', title='Appartment sales per room number')
fig2.show()
st.plotly_chart(fig2)


# ------------------------
# Third exercise, get the top 10 higher-priced regions
# ------------------------

# Exercise title
st.header('Thrid query: Average price per squarred meter per department 💵')

# Snowflake query
my_query_results = execute_sf_query_table("select dept_code, avg(transaction_value/carrez_surface) as avg_sqm_price from sales_view group by dept_code order by avg_sqm_price desc limit 10")

#answer the exercise question
st.dataframe(my_query_results)

# Draw the map
# Load the department informations
df_departement=get_table('dept_info')
df_departement['INSEE_CODE'] = df_departement['INSEE_CODE'].astype(str)
my_query_results['DEPT_CODE'] = my_query_results['DEPT_CODE'].astype(str)

temp_list = df_departement['INSEE_CODE'].to_list()
for x in my_query_results['DEPT_CODE'].to_list():
    if x not in temp_list:
        temp_list.append(x)

st.text(type(df_departement['INSEE_CODE'].to_list()[0]))
st.text(type(my_query_results['DEPT_CODE'].to_list()[0]))
st.text(temp_list)

my_query_results = pd.merge(my_query_results, df_departement, how='left', left_on = 'DEPT_CODE', right_on = 'INSEE_CODE')

#answer the exercise question
st.dataframe(my_query_results)

# Map initialisation
map = folium.Map(location=[43.634, 1.433333],zoom_start=6)

# Transform dataframe into lists
lat_list = my_query_results['LAT'].tolist()
lon_list = my_query_results['LON'].tolist()
lat_lon_list= []
sqm_price_list = my_query_results['AVG_SQM_PRICE'].tolist()
name_list = my_query_results['NAME'].tolist()

# For all the departments
for i in range(len(lat_list)):
    lat_lon_list.append([lat_list[i],lon_list[i]])

# Add markers
for i in range(len(lat_list)):
    folium.Marker(lat_lon_list[i],popup='Prix moyen dans le département {} : {}€/m²'.format(name_list[i],sqm_price_list[i])).add_to(map)

#Print the map on the app
st.map(map)

# Don't run anything past here while troubleshooting
st.stop()


# ---------- PREVIOUS CODE ---------- 
# Fruityvice advice
st.header("Fruityvice Fruit Advice!")

try: 
    fruit_choice = st.text_input('What fruit would you like information about?')

    if not fruit_choice:
        st.error("Please select a fruit to get information.")

    else:
        # Display the dataframe
        #st.dataframe(get_fruityvice_data(fruit_choice))
        pass

except URLError as e:
    st.error()


# Add fruit
add_my_fruit = st.text_input('What fruit would you like to add?')

if st.button('Add a fruit to the list'):
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    st.text(insert_row_snowflake(add_my_fruit))
    my_cnx.close()

#https://poux-be-first-st-app-st-app-6vwpjp.stapp.com/