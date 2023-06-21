import os
import pytest
import requests
import pandas as pd
from src.logger.logger import logger
from src.data_process_etl import DataImportTask, TransformTask

local_file_path = r"C:\Users\Nivetha Vijayakumar\PycharmProjects\pythonProject2\src\raw_data\posts.json"
url = 'https://jsonplaceholder.typicode.com/posts'
csv_file_path = r"C:\Users\Nivetha Vijayakumar\PycharmProjects\pythonProject2\src\raw_data\processed.csv"

# creating sample data for test
@pytest.fixture(scope="module")
def sample_data():
    return [
        {"userid": 1, "id": 1, "title": "et ea vero quia laudantium autem", "body": "delectus reiciendis molestiae occaecati non minima eveniet qui voluptatibus\naccusamus in eum beatae sit\nvel qui neque voluptates ut commodi qui incidunt\nut animi commodi"},
        {"userid": 1, "id": 2, "title": "in quibusdam tempore odit est dolorem", "body": "fuga et accusamus dolorum perferendis illo voluptas\nnon doloremque neque facere\nad qui dolorum molestiae beatae\nsed aut voluptas totam sit illum"},

    ]

def test_data_import(sample_data, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url: create_mock_response(sample_data))
    monkeypatch.setattr(logger, "info", lambda msg: print(msg))

    DataImportTask()
    # check whether file exists in the mentioned folder
    assert os.path.exists(local_file_path), "JSON file should be downloaded and saved"

def test_transform(sample_data, monkeypatch):
    monkeypatch.setattr(pd, "read_json", lambda path: pd.DataFrame(sample_data))
    monkeypatch.setattr(logger, "info", lambda msg: print(msg))

    TransformTask()
    # checking the transformation logic based on the assert condition
    df = pd.read_csv(csv_file_path)
    assert "body" in df.columns, "Column 'body' should be present"
    assert df["body"].str.contains("\n").sum() == 0, "Newline should be removed"



# this function creates a mock response for requests.get
def create_mock_response(data):
    response = requests.Response()
    response.status_code = 200
    response.json = lambda: data
    return response


# #Entry point to Run the tests
if __name__ == "__main__":
    pytest.main()
