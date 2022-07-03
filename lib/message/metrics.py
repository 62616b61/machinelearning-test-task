from lib.message.word_count import message_word_count
from lib.tone import message_tone

def calculate_message_metrics(message):
  word_count = message_word_count(message['html_body'])
  tone = message_tone(message['html_body'])

  message['word_count'] = word_count
  message['tone'] = tone

  return message
