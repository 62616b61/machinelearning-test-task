import re

def message_word_count(message_body):
  words = re.findall(r'[A-Za-z\']+', message_body)
  return len(words)
