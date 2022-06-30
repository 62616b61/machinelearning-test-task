def formatConversationResult(result):
  metrics = result['metrics'][0]
  messages = result['messages'][0]

  conversation = messages[0]['conversation']
  conversation['messages'] = messages

  # remove conversation ref from messages
  for message in conversation['messages']:
    del message['conversation']

  # copy metrics to result
  for key, value in metrics.items():
    conversation[key] = value

  return conversation
