# App file

# ----- Imports -----
import os
from xml.etree.ElementInclude import LimitedRecursiveIncludeError
import requests
import datetime
import matplotlib

import snowflake.connector

import pandas as pd
import streamlit as st
import plotly.express as px


from urllib.error import URLError

# ----- Main code -----
# Variables
PATH = os.getcwd()

# Read fruits list
# my_fruit_list = pd.read_csv(PATH + "/resources/fruit_macros.csv")
my_fruit_list = pd.read_csv("https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt")
my_fruit_list = my_fruit_list.set_index('Fruit')

# Functions
# Get fruityvice data and put it in a dataframe
def get_fruityvice_data(this_fruit_choice):
    # Query Fruityvice and display the response code
    fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+this_fruit_choice)

    # Normalize the fruityvice response in a dataframe
    fruityvice_normalized = pd.json_normalize(fruityvice_response.json())

    return(fruityvice_normalized)

# Fetch Snowflake data
def get_table(table_name):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("select * from "+table_name+" limit 20")
        header = [x[0] for x in my_cur.description]
        df = pd.DataFrame(my_cur.fetchall(), columns = header)
        return (df)

def get_table_with_conditions_on_(table_name):
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


# ----- Main display -----
st.title("D&A Challenge - 2")

# Simple menu
st.header('Data received')
st.text('Here is a snapshot of the data provided for this exercise.')

# Query snowflake
# Add a button to query the fruit list
if st.button("Display the intial data"):
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    my_table = get_table("sales")
    my_cnx.close()
    st.dataframe(my_table)

# Frist exercise, query the data to count the number of appartments sold between two dates
st.header('Frist query: Appartment sales between two dates')
d1 = st.date_input(
     "Study period first day",
     datetime.date(2019, 12, 31))

d2 = st.date_input(
     "Study period last day",
     datetime.date(2020, 4, 1))

my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
    my_cur.execute("select transaction_date, local_type, count(*) over (partition by transaction_date, local_type) as daily_sales_count from sales where (transaction_date <= '"+d2.strftime('%Y-%m-%d')+"' and transaction_date >= '"+d1.strftime('%Y-%m-%d')+"') order by transaction_date asc")
    header = [x[0] for x in my_cur.description]
    my_query_results = pd.DataFrame(my_cur.fetchall(), columns = header)

#answer the exercise question
st.subheader(''+str(sum((my_query_results[my_query_results['LOCAL_TYPE']=='Appartement']['DAILY_SALES_COUNT'].to_list())))+' appartments have been sold during this period of time')
my_cnx.close()

#dataframe formatting to have a beautiful chart
fig = px.bar(my_query_results, x="TRANSACTION_DATE", y="DAILY_SALES_COUNT", color="LOCAL_TYPE", title="Daily sales from the "+d1.strftime('%Y-%m-%d')+" to the "+d2.strftime('%Y-%m-%d')+" per local type")
fig.show()

st.plotly_chart(fig)

# Second exercise, get the ratio of sales per room number
st.header('Second query: Appartment sales per room number')
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
    my_cur.execute("select rooms_number, sum(*) over (partition by rooms_number, local_type) as sales_count from sales where local_type='Appartement' order by rooms_number asc")
    header = [x[0] for x in my_cur.description]
    my_query_results = pd.DataFrame(my_cur.fetchall(), columns = header)
my_cnx.close()

#answer the exercise question
fig2 = px.pie(my_query_results, values='SALES_COUNT', names='ROOMS_NUMBER', title='Apprtment sales per room number')
fig2.show()
st.plotly_chart(fig2)


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