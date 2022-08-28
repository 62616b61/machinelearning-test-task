import apache_beam as beam

class ExtractPropertyFromKeyedPCollection(beam.CombineFn):
  def __init__(self, prop_name):
    self.prop_name = prop_name 

  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    prop = input[self.prop_name]

    accumulator.append(prop)

    return accumulator

  def merge_accumulators(self, accumulators):
    merged = []

    for accum in accumulators:
      merged += accum

    return merged

  def extract_output(self, accumulator):
    return accumulator