# App file

# ----- Imports -----
import os
from xml.etree.ElementInclude import LimitedRecursiveIncludeError
from numpy import sort
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
def get_table(table_name):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("select * from "+table_name+" limit 20")
        header = [x[0] for x in my_cur.description]
        df = pd.DataFrame(my_cur.fetchall(), columns = header)
        return (df)

# Add a row into Snowflake
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
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    my_table = get_table("sales")
    my_cnx.close()
    st.dataframe(my_table)

# ------------------------
# Frist exercise, query the data to count the number of appartments sold between two dates
# ------------------------
# Exercise title
st.header('Frist query: Appartment sales between two dates 📆')

# Select the first date
d1 = st.date_input(
     "Study period first day",
     datetime.date(2019, 12, 31))

# Select the second date
d2 = st.date_input(
     "Study period last day",
     datetime.date(2020, 4, 1))

# Query snowflake
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
    my_cur.execute("select transaction_date, local_type, sum(count(*)) over (partition by transaction_date, local_type) as daily_sales_count from sales_view where (transaction_date <= '"+d2.strftime('%Y-%m-%d')+"' and transaction_date >= '"+d1.strftime('%Y-%m-%d')+"') group by transaction_date, local_type order by transaction_date asc")
    header = [x[0] for x in my_cur.description]
    my_query_results = pd.DataFrame(my_cur.fetchall(), columns = header)

# Answer the exercise question
st.subheader(''+str(sum((my_query_results[my_query_results['LOCAL_TYPE']=='Appartement']['DAILY_SALES_COUNT'].to_list())))+' appartments have been sold during this period of time')
my_cnx.close()

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
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
    my_cur.execute("select rooms_number, sum(count(*)) over (partition by rooms_number) as sales_count from sales_view where local_type='Appartement' group by rooms_number order by rooms_number asc")
    header = [x[0] for x in my_cur.description]
    my_query_results = pd.DataFrame(my_cur.fetchall(), columns = header)
my_cnx.close()

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
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
    my_cur.execute("select dept_code, avg(transaction_value/carrez_surface) as avg_sqm_price from sales_view group by dept_code order by avg_sqm_price desc limit 10")
    header = [x[0] for x in my_cur.description]
    my_query_results = pd.DataFrame(my_cur.fetchall(), columns = header)
my_cnx.close()

#answer the exercise question
st.dataframe(my_query_results)

# Don't run anything past here while troubleshooting
st.stop()


# ---------- PREVIOUS CODE ---------- 

st.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')
# Let's put a pick list here so they can pick the fruit they want to include 
fruits_selected = st.multiselect("Pick some fruits:", list(my_fruit_list.index), ['Avocado', 'Strawberries'])
fruits_to_show = my_fruit_list.loc[fruits_selected]

# Display the table on the page.
st.dataframe(fruits_to_show)

# Fruityvice advice
st.header("Fruityvice Fruit Advice!")

try: 
    fruit_choice = st.text_input('What fruit would you like information about?')

    if not fruit_choice:
        st.error("Please select a fruit to get information.")

    else:
        # Display the dataframe
        st.dataframe(get_fruityvice_data(fruit_choice))

except URLError as e:
    st.error()



# Query snowflake
st.header("The fruit list contains:")
# Add a button to query the fruit list
if st.button("Get Fruit Load List"):
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    my_data_rows = get_fruit_load_list()
    my_cnx.close()
    st.dataframe(my_data_rows)

# Add fruit
add_my_fruit = st.text_input('What fruit would you like to add?')

if st.button('Add a fruit to the list'):
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    st.text(insert_row_snowflake(add_my_fruit))
    my_cnx.close()

#https://poux-be-first-st-app-st-app-6vwpjp.stapp.com/