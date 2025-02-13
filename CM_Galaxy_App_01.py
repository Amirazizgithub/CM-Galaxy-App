import re
import os
import pandas as pd
import streamlit as st
from openai import OpenAI
from pymongo import MongoClient
from pyspark.sql import SparkSession
os.environ["OPENAI_API_KEY"] = "sk-TpBf......................."

# Connection details
host = 'srvr1px.cyberads.io'
port = 27017
username = 'cmrslpx1'
password = 'y@PdjJF#)Gj&7dC'
auth_source = 'admin'
client = MongoClient(host=host, port=port, username=username, password=password, authSource=auth_source)

## Build function to find list of matching collections according to CPT selected by user
def find_matching_collections(schema,CPT):
    matching_collections = []
    db = client[schema]
    list_collection_names = db.list_collection_names()

    for col in list_collection_names:
        pattern = re.compile(fr'\b\w*{CPT}\w*\b', re.IGNORECASE)
        if re.search(pattern, col):
            matching_collections.append(col)
    
    return matching_collections

## Build function to find collection search keywords in the users' input
def find_collection_keywords(user_input):
    matching_keywords = []
    user_search_collections = ["region","age","gender","demograph","device"]

    for word in user_input.split():
        for col in user_search_collections:
            pattern = re.compile(fr'\b\w*{col}\w*\b', re.IGNORECASE)
            if re.search(pattern, word):
                matching_keywords.append(col)
    if len(matching_keywords) == 0:
        matching_keywords.append('_data')
    
    return matching_keywords

## Build function to find collection name according to searched by user
def find_collections(collected_keywords,collections):

    for item in collected_keywords:
        for col in collections:
            pattern = re.compile(fr'\b\w*{item}\w*\b', re.IGNORECASE)
            if re.search(pattern, col):
                break
                
    return col

## Build function to convert the documents data of MongoDB into Dataframe
def create_dataframe(schema,collection_db):
    
    # Access the desired database and collection
    db = client[schema]
    
    ## Access the desired database and collection
    collection = db[collection_db]
    
    ## Fetch data from MongoDB
    data = list(collection.find({},{"_id":0,"iso_date":0}))
    
    ## Convert list of documents into DataFrame
    spark = SparkSession.builder.getOrCreate()
    data_frame = spark.createDataFrame(data)
    
    ## Register the DataFrame as a temporary SQL table
    data_frame.createOrReplaceTempView("data_frame")
        
    return data_frame, spark

## Build function to find columns name in the users' input
def find_column_keywords(user_input,data_frame):
    matching_columns = []
    user_search_columns = data_frame.columns

    for word in user_input.split():
        for column in user_search_columns:
            pattern = re.compile(fr'\b\w*{column}\w*\b', re.IGNORECASE)
            if re.search(pattern, word):
                matching_columns.append(column)

    for word in user_input.split():
        pattern = re.compile(fr'\b\w*(day|month|year)\w*\b', re.IGNORECASE)
        if re.search(pattern, word):
            if "date" not in matching_columns:
                matching_columns.append("date")    
    
    for word in user_input.split():
        pattern = re.compile(fr'\b\w*(age)\w*\b', re.IGNORECASE)
        if re.search(pattern, word):
            if "age_range" or "age" not in matching_columns:
                matching_columns.append("age_range")
            
    for word in user_input.split():
        pattern = re.compile(fr'\b\w*(region|state|area)\w*\b', re.IGNORECASE)
        if re.search(pattern, word):
            if "state_name" or "region" not in matching_columns:
                matching_columns.append("state_name")

    return matching_columns

## Build the function to qenerate SQL query from natural language query
def generate_sql_query(user_input,columns_searched):
    
    ## Specify the prompt for GPT-3
    prompt_01 = f'''Generate a SQL query for spark.sql used columns {columns_searched} to {user_input} and table name is data_frame.'''

    client_instant = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    
    ## Call the OpenAI GPT-3 API
    response = client_instant.completions.create(
        model="gpt-3.5-turbo-instruct",  # You can experiment with other engines
        prompt=prompt_01,
        max_tokens=150,  # Adjust the number of tokens based on your needs
        temperature=0.5  # Adjust the temperature for randomness
    )

    ## Extract the generated SQL query from the GPT-3 response
    generated_query = response.choices[0].text.strip()

    return generated_query

## Build the function data visualization
def data_visualization(schema,CPT,user_input):
    try:
        ## List of collections of search scheama
        collections = find_matching_collections(schema,CPT)
        
        ## List of collection keywords
        collected_keywords = find_collection_keywords(user_input)
        
        ## Collection_db searched by user
        collection_db = find_collections(collected_keywords,collections)
        
        ## Load the dataframe from MongoDB & Create SPARK
        data_frame = create_dataframe(schema,collection_db)[0]
        spark = create_dataframe(schema,collection_db)[1]
        
        ## List of columns which are searched by user
        columns_searched = find_column_keywords(user_input,data_frame)
        
        ## Generate SQL Query from user input
        sql_query = generate_sql_query(user_input,columns_searched)

        ## Get the result
        result = spark.sql(sql_query)

        return result
        
    except Exception:
        try:
            ## List of collections of search scheama
            collections = find_matching_collections(schema,CPT)
            
            ## List of collection keywords
            collected_keywords = find_collection_keywords(user_input)
            
            ## Collection_db searched by user
            collection_db = find_collections(collected_keywords,collections)
            
            ## Load the dataframe from MongoDB & Create SPARK
            data_frame = create_dataframe(schema,collection_db)[0]
            spark = create_dataframe(schema,collection_db)[1]
            
            ## List of columns which are searched by user
            columns_searched = find_column_keywords(user_input,data_frame)
            
            ## Generate SQL Query from user input
            sql_query = generate_sql_query(user_input,columns_searched)
            ## Get the result
            result = spark.sql(sql_query)

            return result
        
        except Exception:
            return "Error"
        
## Define options for dropdown lists
Brands = ["okana", "namjosh"]
user_select_CPT = ["adword", "meta"]
Graphs = ["Bar Chart", "Line Chart", "Area Chart", "Scatter Chart"]

## Create dropdown lists and text input field
schema = st.sidebar.selectbox("Select Clients' Brand Name", Brands)
CPT = st.sidebar.selectbox("Select Campaign Plateform Type", user_select_CPT)
user_input = st.sidebar.text_input("Enter text:")
graph = st.sidebar.selectbox("Select type of graph", Graphs)

## Create empty DataFrame for output
df = pd.DataFrame(columns=["Brand", "Campaign", "Text Input"])

## Check if button is clicked
if st.sidebar.button("Submit"):
  ## Add user choices to the DataFrame
  new_row = pd.DataFrame([[schema, CPT, user_input]], columns=df.columns)
  df = pd.concat([df, new_row], ignore_index=True)
  
  ## Call the Data Visualization Function
  result = data_visualization(schema,CPT,user_input)

  st.title("Your Query")
  ## Display the DataFrame in a table
  st.dataframe(df)

  st.title("Result of Query")
  if not isinstance(result, str):  ## Correct way to check if result is not a string
    ## Convert Spark DataFrame to pandas DataFrame (cautious with large datasets)
    pandas_df = result.toPandas()
    ## Display the Result in a table
    st.dataframe(pandas_df)

    if graph == "Bar":
        st.bar_chart(pandas_df)
    elif graph == "Line Chart":
        st.line_chart(pandas_df)
    elif graph == "Area Chart":
        st.area_chart(pandas_df)
    elif graph == "Scatter Chart":
        st.scatter_chart(pandas_df)

  elif isinstance(result, str):  ## Correct way to check if result is a string
    st.write("Please, Re-enter text carefully with correct spelling & correct column names")
    pass
