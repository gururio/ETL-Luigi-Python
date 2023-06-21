# ETL-Luigi-TrustedShops-Python
Data Pipeline Based on Luigi Framework
Objective:
The Purpose of this code is to create a ETL pipeline which retrieve data from the API endpoint, transform it and load it into a table in database.

An API endpoint is given https://jsonplaceholder.typicode.com/posts to retrieve data which act as source. Our ETL pipeline should download this data and store it in local. Then read the downloaded data and do the data cleaning activity and store it in local in processed space.
The final outcome is to copy the transformed data to the database. 

Methodology:
To achieve all this, I decided using POSTGRESQL as my datastore it is faster and for connection we have support libraries in python.
In order to build the pipeline as per the requirement I have used luigi framework. It helps us to chain the task dependencies and run one after the other. Also, we can achieve idempotent and if any task fails in middle, we don't need to rebuild the whole pipeline.
Also, once after the transformation we are saving the file as csv since it allows easy integration with database loading tools or SQL statements for efficient data insertion and provide compatibility in working with tabular data.

Execution Steps:
To run the code:
Make sure to have PostgreSQL installed on your machine.
Update the local file path where ever needed.
From the terminal navigate to the requirements.txt. Run the following command to install all the necessary pip packages to run the program:
    sudo pip install -r requirements.txt
To run the Luigi Pipeline, run the following command:
    python main.py
To view the results of the ETL transformation, find the newly created file processed data in the csv_folder.
To run the unit tests, simply run:
    python pytest test.py
