import requests
from time import sleep
import random
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime
import json  # Importing json module for JSON handling

# Seed for reproducibility
random.seed(100)

# Load database credentials from the YAML file
with open('db_creds.yaml', 'r') as creds_file:
    db_creds = yaml.safe_load(creds_file)

# Define a class for database connection
class AWSDBConnector:
    def __init__(self, db_creds):
        self.HOST = db_creds['database']['host']
        self.USER = db_creds['database']['user']
        self.PASSWORD = db_creds['database']['password']
        self.DATABASE = db_creds['database']['database']
        self.PORT = db_creds['database']['port']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

# Initialize the DB Connector
new_connector = AWSDBConnector(db_creds)

# Function to convert datetime objects to strings
def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
    return data

# Function to send data to the Kinesis stream via API
def send_data_to_stream(api_url, stream_name, data):
    headers = {'Content-Type': 'application/json'}
    response = requests.put(api_url, headers=headers, data=json.dumps(data))  # Convert data to JSON format
    if response.status_code != 200:
        print(f"Failed to send data to {stream_name}. Status code: {response.status_code}")
        print(response.json())
    else:
        print(f"Successfully sent data to {stream_name}")
        print(response.json())
    return response

# Function to fetch data from the database and send it to the Kinesis streams
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Fetching data from Pinterest Pin table
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = convert_datetime_to_string(dict(row._mapping))

            # Fetching data from Geolocation table
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = convert_datetime_to_string(dict(row._mapping))

            # Fetching data from User table
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = convert_datetime_to_string(dict(row._mapping))

            pin_result = json.dumps({
                "records": [
                    {
                    "value": pin_result
                    }
                ]
            }, default=str)
            geo_result = json.dumps({
                "records": [
                    {
                    "value": geo_result
                    }
                ]
            }, default=str)
            user_result = json.dumps({
                "records": [
                    {
                    "value": user_result
                    }
                ]
            }, default=str)
            
            streams = ['streaming-0eaa2e755d1f-pin', 'streaming-0eaa2e755d1f-geo', 'streaming-0eaa2e755d1f-user']

            # URLs for sending data to Kinesis streams
            url_pin = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest'
            url_geo = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest/streams/streaming-0eaa2e755d1f-geo'
            url_user = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest/streams/streaming-0eaa2e755d1f-user'

            # Sending data to Kinesis streams
            send_data_to_stream(url_pin, streams[0], pin_result)
            send_data_to_stream(url_geo, streams[1], geo_result)
            send_data_to_stream(url_user, streams[2], user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
