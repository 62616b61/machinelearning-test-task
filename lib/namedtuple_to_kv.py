def namedtuple_to_kv(t):
  dictionary = {}

  for key, value in t._asdict().items():
    if key != 'key':
      dictionary[key] = value
      
  return (t.key, dictionary)
