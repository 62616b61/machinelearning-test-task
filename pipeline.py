import apache_beam as beam

from sources.file import read_data
from lib.conversation.split import splitConversationIntoMessages
from lib.conversation.avg_word_count import ConversationAvgWordCount
from lib.conversation.format_results import formatConversationResult
from lib.message.metrics import calculateMessageMetrics
from lib.message.group_by_ticket import byTicket
from lib.namedtuple_to_kv import namedTupleToKV

p = beam.Pipeline()

messages = (p
  | beam.Create(read_data())
  | beam.FlatMap(splitConversationIntoMessages)
  | beam.Map(calculateMessageMetrics)
)

metricsByTicket = (messages
  | "group metrics" >> beam.GroupBy(byTicket)
    .aggregate_field(lambda message: message['word_count'], sum, 'total_word_count')
    .aggregate_field(lambda message: message['word_count'], min, 'min_message_word_count')
    .aggregate_field(lambda message: message['word_count'], max, 'max_message_word_count')
    .aggregate_field(lambda message: message['word_count'], ConversationAvgWordCount(), 'average_message_word_count')
  | beam.Map(namedTupleToKV)
)

messagesByTicket = messages | "group messages" >> beam.GroupBy(byTicket)

combined = { 'metrics': metricsByTicket, 'messages': messagesByTicket } | beam.CoGroupByKey()

results = (combined
  | beam.Values()
  | beam.Map(formatConversationResult)
)

results | beam.io.WriteToText('results.txt')

p.run()
