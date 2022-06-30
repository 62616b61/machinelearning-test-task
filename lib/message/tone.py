def messageTone(messageBody):
  if (len(messageBody) == 0):
    return 'unknown'

  firstWord = messageBody.split(' ', 1)[0]
  isEven = len(firstWord) % 2 == 0
  tone = 'positive' if isEven else 'negative'
  
  return tone
