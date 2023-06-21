import sys
import luigi
import pandas as pd
import requests
import json
import psycopg2
from src.logger.logger import logger

local_file_path = r"C:\Users\Nivetha Vijayakumar\PycharmProjects\pythonProject2\src\raw_data\posts.json"
url = 'https://jsonplaceholder.typicode.com/posts'
json_file_path = r"C:\Users\Nivetha Vijayakumar\PycharmProjects\pythonProject2\src\raw_data\posts.json"
csv_file_path = r"C:\Users\Nivetha Vijayakumar\PycharmProjects\pythonProject2\src\raw_data\processed.csv"


class DataImportTask(luigi.Task):
    def run(self):
        try:
            logger.info(f"Connecting to API endpoint to import data")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            with open(local_file_path, "w") as f:
                json.dump(data, f)
            logger.info(f"JSON file downloaded and saved successfully.")

        except requests.exceptions.RequestException as error:
            logger.Exception(f"Failed to fetch JSON data: {str(error)}")
            sys.exit(400)

    def output(self):
        return luigi.LocalTarget(local_file_path)

class TransformTask(luigi.Task):
    def requires(self):
        return DataImportTask()

    def run(self):
        try:
            # Reading the json data file
            logger.info(f"Reading the json data for data transformation")
            df = pd.read_json(json_file_path)
            # Performing the data cleansing on certain columns and storing the resultant data as csv
            df["body"] = df["body"].str.replace("\n", "")
            df.to_csv(csv_file_path, index=False)
            logger.info(f"Transformation successful saving the resultant file in CSV format")

        except Exception as e:
            logger.ExceptionException(f"Error in transforming data: {str(e)}")
            sys.exit(400)

    def output(self):
        return luigi.LocalTarget(csv_file_path)


class DataLoadTask(luigi.Task):
    def requires(self):
        return TransformTask()

    def run(self):
        # PostgreSQL database connection details
        host = 'localhost'
        database = 'Namastesql'
        user = 'postgres'
        password = 'Qwerty@123'

        try:
            logger.info(f"Establish a connection to the PostgreSQL database")
            # Table name
            table_name = 'posts'

            # Establish a connection to the PostgreSQL database
            conn = psycopg2.connect(host=host, database=database, user=user, password=password)

            # Create a cursor object to execute SQL queries
            cursor = conn.cursor()

            # Create the table if it doesn't exist
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (userid INT,id INT PRIMARY KEY, title VARCHAR(1000),body VARCHAR(1000))"
            cursor.execute(create_table_query)

            logger.info(f"Opening the Transformed CSV file for data load")
            with open(csv_file_path, 'r') as f:
                # Skipping the header row
                next(f)
                #copying the data from the csv file to postgreSQL table
                cursor.copy_from(f, table_name, sep=',')
            #Closing the established connections
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Loaded data in the target table")

        except Exception as error:
            logger.Exception(f"Failed while loading the target table: {str(error)}")
            sys.exit(400)

    def output(self):
        return luigi.LocalTarget(csv_file_path)

