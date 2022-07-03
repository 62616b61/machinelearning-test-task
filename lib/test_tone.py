import logging
import unittest
import pytest

from tone import message_tone

class MessageToneTest(unittest.TestCase):
  def test_tone_positive(self):
    tone = message_tone('even')

    self.assertEqual(tone, 'positive')

  def test_tone_negative(self):
    tone = message_tone('odd')

    self.assertEqual(tone, 'negative')

  def test_tone_unknown(self):
    tone = message_tone('')

    self.assertEqual(tone, 'unknown')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()