import re

def messageWordCount(messageBody):
  words = re.findall(r'[A-Za-z\']+', messageBody)
  return len(words)
