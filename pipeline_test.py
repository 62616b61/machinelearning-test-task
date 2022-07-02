import json
import logging
import tempfile
import unittest
import pytest

import pipeline

from apache_beam.testing.util import open_shards
from sources.file import read_data

class PipelineTest(unittest.TestCase):
  def create_temp_file(self):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      return f.name

  def test_pipeline(self):
    temp_path = self.create_temp_file()

    pipeline.main(['--input=test-data.json', '--output=%s.result' % temp_path])

    # Parse result file and compare.
    with open_shards(temp_path + '.result-*-of-*') as result_file:
      result = json.loads([*result_file][0])
      message1, message2 = result['messages']

    # assert metrics fields exist on conversation object and are correct
    self.assertEqual(result['total_word_count'], 22)
    self.assertEqual(result['min_message_word_count'], 10)
    self.assertEqual(result['max_message_word_count'], 12)
    self.assertEqual(result['average_message_word_count'], 11.0)
    self.assertEqual(result['tone'], 'negative')

    # assert metrics fields exist on message 1 and are correct
    self.assertEqual(message1['word_count'], 10)
    self.assertEqual(message1['tone'], 'negative')

    # assert metrics fields exist on message 2 and are correct
    self.assertEqual(message2['word_count'], 12)
    self.assertEqual(message2['tone'], 'negative')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()