from lib.message.word_count import messageWordCount
from lib.message.tone import messageTone

def calculateMessageMetrics(message):
  wordCount = messageWordCount(message['html_body'])
  tone = messageTone(message['html_body'])

  message['word_count'] = wordCount
  message['tone'] = tone

  return message
