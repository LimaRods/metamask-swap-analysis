
import json

# Getting the Shroomdk API key
with open('sdk-api-key.json', 'r') as file:
    api_key_dict = json.load(file)
    
sdk_api_key = api_key_dict['key']
