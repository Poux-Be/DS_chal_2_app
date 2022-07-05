# App file

# ----- Imports -----
from ast import Add
import os
import requests
import streamlit
import snowflake.connector

import pandas as pd

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

# Fetch the fruit list from Snowflake
def get_fruit_load_list():
    with my_cnx.cursor() as my_cur:
        my_cur.execute("select * from fruit_load_list")
        return (my_cur.fetchall())

# Add a row into Snowflake
def insert_row_snowflake(new_fruit):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("insert into fruit_load_list values ('"+new_fruit+"')")
        return('Thanks for adding ' + add_my_fruit)

# ----- Main display -----
streamlit.title("My Parents New Healthy Diner")

# Simple menu
streamlit.header('Breakfast Menu')
streamlit.text('🥣 Omega 3 & Blueberry Oatmeal')
streamlit.text('🥗 Kale, Spinach & Rocket Smoothie')
streamlit.text('🐔 Hard-Boiled Free-Range Egg')
streamlit.text(" 🥑🍞 Avocado Toast")

streamlit.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')
# Let's put a pick list here so they can pick the fruit they want to include 
fruits_selected = streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index), ['Avocado', 'Strawberries'])
fruits_to_show = my_fruit_list.loc[fruits_selected]

# Display the table on the page.
streamlit.dataframe(fruits_to_show)

# Fruityvice advice
streamlit.header("Fruityvice Fruit Advice!")

try: 
    fruit_choice = streamlit.text_input('What fruit would you like information about?')

    if not fruit_choice:
        streamlit.error("Please select a fruit to get information.")

    else:
        # Display the dataframe
        streamlit.dataframe(get_fruityvice_data(fruit_choice))

except URLError as e:
    streamlit.error()

# Don't run anything past here while troubleshooting
#streamlit.stop()

# Query snowflake
streamlit.header("The fruit list contains:")
# Add a button to query the fruit list
if streamlit.button("Get Fruit Load List"):
    my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    my_data_rows = get_fruit_load_list()
    my_cnx.close()
    streamlit.dataframe(my_data_rows)

# Add fruit
add_my_fruit = streamlit.text_input('What fruit would you like to add?')

if streamlit.button('Add a fruit to the list'):
    my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    streamlit.text(insert_row_snowflake(add_my_fruit))
    my_cnx.close()

#https://poux-be-first-streamlit-app-streamlit-app-6vwpjp.streamlitapp.com/