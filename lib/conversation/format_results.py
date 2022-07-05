def format_conversation_results(result):
  conversation = result['conversation'][0]
  messages = result['messages'][0]

  metrics = {k: v for k, v in result.items() if k not in ['messages', 'conversation']}
  
  conversation['messages'] = messages

  # copy metrics to conversation
  for key, value in metrics.items():
    conversation[key] = value[0]

  return conversation
