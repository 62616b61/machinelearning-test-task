import apache_beam as beam

from sources.file import read_data
from lib.conversation.avg_word_count import ConversationAvgWordCount
from lib.conversation.tone import ConversationTone
from lib.conversation.format_results import formatConversationResult
from lib.message.metrics import calculateMessageMetrics
from lib.message.group_by_ticket import byTicket
from lib.namedtuple_to_kv import namedTupleToKV

p = beam.Pipeline()

# Clean conversations, will be used later
conversations = p | beam.Create(read_data())

# Flat map all conversations' messages and calculate metrics
messagesWithMetrics = (conversations
  | beam.FlatMap(lambda x: x['messages'])
  | beam.Map(calculateMessageMetrics)
)

# Group messages by ticket id and calculate conversation metrics
metricsByTicket = (messagesWithMetrics
  | "group metrics" >> beam.GroupBy(byTicket)
    .aggregate_field(lambda message: message['word_count'], sum, 'total_word_count')
    .aggregate_field(lambda message: message['word_count'], min, 'min_message_word_count')
    .aggregate_field(lambda message: message['word_count'], max, 'max_message_word_count')
    .aggregate_field(lambda message: message['word_count'], ConversationAvgWordCount(), 'average_message_word_count')
    .aggregate_field(lambda message: message['tone'], ConversationTone(), 'tone')

  # Aggregation via aggregate_field produces named tuples,
  # which must be translated to key values pairs in order to be used in CoGroupByKey
  | beam.Map(namedTupleToKV)
)

# Group messages by ticket id
messagesByTicket = (messagesWithMetrics
  | "group messages" >> beam.GroupBy(byTicket)
)

# Group conversations by ticket id
conversationsByTicket = (conversations
  | "group conversations" >> beam.GroupBy(byTicket)
  # Assuming ticket_id corresponds to only one conversation object
  | "get first element" >> beam.MapTuple(lambda key, value: (key, value[0]))
)

# Combine clean conversations, messages with metrics and conversation metrics
results = ({
  'metrics': metricsByTicket,
  'messages': messagesByTicket,
  'conversation': conversationsByTicket
}
  | beam.CoGroupByKey()
  | beam.Values()
  | beam.Map(formatConversationResult)
)

# Sink results
results | beam.io.WriteToText('results.txt')

p.run()
