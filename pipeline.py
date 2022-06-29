import json
import apache_beam as beam


def read_data():
    with open("clean-data.json") as file:
        return json.loads(file.read())


p = beam.Pipeline()
p | beam.Create(read_data()) | beam.Map(print)
p.run()
