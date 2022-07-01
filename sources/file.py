import json

def read_data(data_file):
  with open(data_file) as file:
    return json.loads(file.read())