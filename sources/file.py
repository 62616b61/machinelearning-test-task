import json

def read_data():
  with open("clean-data.json") as file:
    return json.loads(file.read())