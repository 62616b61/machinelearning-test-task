from lib.message.word_count import message_word_count
from lib.tone import message_tone

import apache_beam as beam

class MessageMetrics(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, message):
    message['word_count'] = message_word_count(message['html_body'])
    message['tone'] = message_tone(message['html_body'])

    accumulator.append(message)

    return accumulator

  def merge_accumulators(self, accumulators):
    merged = []

    for accum in accumulators:
      merged += accum

    return merged

  def extract_output(self, accumulator):
    return accumulator
