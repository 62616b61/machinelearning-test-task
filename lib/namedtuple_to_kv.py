def namedTupleToKV(t):
  dictionary = {}

  for key, value in t._asdict().items():
    if key != 'key':
      dictionary[key] = value
      
  return (t.key, dictionary)
