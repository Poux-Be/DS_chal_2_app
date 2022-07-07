# App file

# ----- Imports -----
import os
import requests
import datetime
import matplotlib
import snowflake.connector

import math as ma
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
def get_table(table_name, limit):
    if type(limit) == int:
        return(execute_sf_query_table("select * from "+table_name+" limit "+str(limit)))
    else:
        return(execute_sf_query_table("select * from "+table_name+""))

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
    st.dataframe(get_table("sales", 20))

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
my_query_results_2 = execute_sf_query_table("select rooms_number, sum(count(*)) over (partition by rooms_number) as sales_count from sales_view where local_type='Appartement' group by rooms_number order by rooms_number asc")

# Answer the exercise question
fig2 = px.pie(my_query_results_2, values='SALES_COUNT', names='ROOMS_NUMBER', title='Appartment sales per room number')
fig2.show()
st.plotly_chart(fig2)


# ------------------------
# Third exercise, get the top x higher-priced regions
# ------------------------

# Exercise title
st.header('Thrid query: Average squarred meter price per department 💵')
# Snowflake query
my_query_results_3 = execute_sf_query_table("select dept_code, avg(transaction_value/carrez_surface) as avg_sqm_price from sales_view group by dept_code order by avg_sqm_price desc")

# Data formating
my_query_results_3['AVG_SQM_PRICE'] = my_query_results_3['AVG_SQM_PRICE'].apply(ma.ceil)

st.text('This will display a top of the higher priced departments. Please select the number of departments you want to see.')
default = my_query_results_3 if len(my_query_results_3) <= 10 else 10
top = st.slider('How many departments do you want to see?', 0, len(my_query_results_3), default)

#answer the exercise question
#st.dataframe(my_query_results_3[:top].set_index('DEPT_CODE'))
fig3 = px.bar(my_query_results_3[:top], x="DEPT_CODE", y="AVG_SQM_PRICE", title="Average squarred meter price per department")
fig3.show()
st.plotly_chart(fig3)

# ------------------------
# Fourth exercise, get the average squarred meter price per region
# ------------------------

# Exercise title
st.header('Fourth query: Average squarred meter price per region 🏡/🏢')

# Dept code input
region_list = execute_sf_query_table("select distinct new_region from dept_info")['NEW_REGION'].to_list()
selected_region = st.selectbox("Veuillez choisir la région dont vous voulez foir le prix moyen", region_list)

# Snowflake query
dept_list = execute_sf_query_table("select insee_code from dept_info where new_region ='"+str(selected_region).replace("'","''")+"'")['INSEE_CODE'].to_list()
my_query_results_4 = execute_sf_query_table("select local_type, avg(transaction_value/carrez_surface) as avg_sqm_price from sales_view where dept_code in ("+str(dept_list).replace('[','').replace(']','')+") group by local_type order by avg_sqm_price desc")

# Display the different average prices with metrics
col1, col2 = st.columns(2)
col1.metric("🏡", str(int(my_query_results_4[my_query_results_4['LOCAL_TYPE']=='Maison']['AVG_SQM_PRICE'].values[0]))+ " €")
col2.metric("🏢", str(int(my_query_results_4[my_query_results_4['LOCAL_TYPE']=='Appartement']['AVG_SQM_PRICE'].values[0]))+ " €")

# ------------------------
# Fifth exercise, get the top 10 higher-priced appartments
# ------------------------

# Exercise title
st.header('Fifth query: Top 10 higher priced appartments 🏢')

# Answer the question
st.dataframe(execute_sf_query_table("select transaction_value, street_number, street_type, city_name, dept_code, carrez_surface, rooms_number from sales_view where (transaction_value is not null and local_type='Appartement') order by transaction_value desc limit 10"))

# ------------------------
# Sixth exercise, get the sales number evolution
# ------------------------

# Exercise title
st.header('Sixth query: Sales number evolution 📈')

# Answer the question
first_sem_sales_count = execute_sf_query_table("select count(*) from sales_view where (transaction_date>='2020-01-01' and transaction_date<'2020-03-31')").values[0][0]
second_sem_sales_count = execute_sf_query_table("select count(*) from sales_view where (transaction_date>='2020-04-01' and transaction_date<='2020-07-31')").values[0][0]
st.text(second_sem_sales_count)
st.text(first_sem_sales_count)
st.text(type(first_sem_sales_count))
st.metric("Second semester sales number",second_sem_sales_count, (second_sem_sales_count - first_sem_sales_count))

# ------------------------
# Seventh exercise, get thesales number evolution
# ------------------------

# Exercise title
st.header('Seventh query: Departments with a high sales number increase between the first and the second semester 💸')

# Answer the question
df_7 = execute_sf_query_table("select dept_code, date_part(quarter,transaction_date::date) as t_quarter, sum(count(*)) over (partition by dept_code, t_quarter) as sales_count from sales_view group by dept_code, t_quarter")

# Split the df per semester
df_7_1 = df_7[df_7['T_QUARTER']==1]
df_7_2 = df_7[df_7['T_QUARTER']==2]

# Merge the dict again
df_7 = df_7_2.merge(df_7_1, on='DEPT_CODE', how='left')

df_7['EVOL'] = df_7_2['SALES_COUNT']/ df_7_1['SALES_COUNT']

st.dataframe(df_7)
# Don't run anything past here while troubleshooting
st.stop()

#https://poux-be-first-st-app-st-app-6vwpjp.stapp.com/

# old code to have a map
# import folium
# from streamlit_folium import st_folium
# Load the department informations
# df_departement=get_table('dept_info', None)

# Left join to add the department informations
# my_query_results = my_query_results.merge(df_departement, left_on=['DEPT_CODE'], right_on=['INSEE_CODE'], how='left')

# # Print merged table
# st.dataframe(my_query_results)

# # Map initialisation
# map = folium.Map(location=[43.634, 1.433333],zoom_start=6)

# # Transform dataframe into lists
# lat_list = my_query_results['LAT'].to_list()
# lon_list = my_query_results['LON'].to_list()
# name_list = my_query_results['NAME'].to_list()
# lat_lon_list= []
# sqm_price_list = my_query_results['AVG_SQM_PRICE'].tolist()

# # For all the departments
# for i in range(len(lat_list)):
#     lat_lon_list.append([lat_list[i],lon_list[i]])

# # Add markers
# for i in range(len(lat_list)):
#     folium.Marker(lat_lon_list[i],popup='Prix moyen dans le département {} : {}€/m²'.format(name_list[i],sqm_price_list[i])).add_to(map)

# #Print the map on the app
# st_folium(map, width = 725)