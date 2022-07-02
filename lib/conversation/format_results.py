def format_conversation_results(result):
  metrics = result['metrics'][0]
  messages = result['messages'][0]
  conversation = result['conversation'][0]
  
  conversation['messages'] = messages

  # copy metrics to conversation
  for key, value in metrics.items():
    conversation[key] = value

  return conversation
