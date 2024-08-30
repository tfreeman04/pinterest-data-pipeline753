import requests
import json

# Function to send data to the Kinesis stream via API
def send_data_to_stream(api_url, data):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send data. Status code: {response.status_code}")
        print(response.json())
    else:
        print(f"Successfully sent data.")
        print(response.json())
    return response

# URLs for sending data to Kinesis streams
url_pin = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest/streams/streaming-0eaa2e755d1f-pin'
url_geo = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest/streams/streaming-0eaa2e755d1f-geo'
url_user = 'https://26a10gk4qd.execute-api.us-east-1.amazonaws.com/pintrest/streams/streaming-0eaa2e755d1f-user'

# Example data to send
pin_data = {"example_key": "example_value"}
geo_data = {"example_key": "example_value"}
user_data = {"example_key": "example_value"}

# Sending data to Kinesis streams
send_data_to_stream(url_pin, pin_data)
send_data_to_stream(url_geo, geo_data)
send_data_to_stream(url_user, user_data)
