import requests
import sqlite3
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np

# Define URL and file paths
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = r'C:\Users\manny\Countries_by_GDP.csv'  # Use raw string for the file path
table_attribs = {}  # Define an empty dictionary if no specific attributes are needed

# Function to fetch HTML content and extract data
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=["Country", "GDP_USD_millions"])
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) > 2:  # Ensure there are enough columns
            if col[0].find('a') is not None and '—' not in col[2].text:
                data_dict = {
                    "Country": col[0].a.text.strip(),
                    "GDP_USD_millions": col[2].text.strip()
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

# Function to transform data
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

# Function to load data into CSV
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

# Function to load data into SQLite database
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Function to run a query on the database
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Function to log progress
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Corrected timestamp format
    now = datetime.now()  # Get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

# Main ETL Process
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process complete.')

sql_connection.close()
