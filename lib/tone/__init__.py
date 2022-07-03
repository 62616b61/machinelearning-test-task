def message_tone(message_body):
  if (len(message_body) == 0):
    return 'unknown'

  first_word = message_body.split(' ', 1)[0]
  is_even = len(first_word) % 2 == 0
  tone = 'positive' if is_even else 'negative'
  
  return tone

