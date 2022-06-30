import copy

def splitConversationIntoMessages(conversation):
  conversationRef = copy.deepcopy(conversation)
  if 'messages' in conversationRef:
    del conversationRef['messages']

  for message in conversation['messages']:
    message['conversation'] = conversationRef
  
  return conversation['messages']
