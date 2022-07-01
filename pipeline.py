import json
import argparse
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from sources.file import read_data
from lib.conversation.avg_word_count import ConversationAvgWordCount
from lib.conversation.tone import ConversationTone
from lib.conversation.format_results import formatConversationResult
from lib.message.metrics import calculateMessageMetrics
from lib.message.group_by_ticket import byTicket
from lib.namedtuple_to_kv import namedTupleToKV

def main(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--input',
    dest='input',
    default='clean-data.json',
    help='Input file to process.'
  )
  parser.add_argument(
    '--output',
    dest='output',
    default='processed-data.json',
    help='Output file to write results to.'
  )
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:
    # Read conversations
    conversations = p | "read data" >> beam.Create(read_data(known_args.input))

    # Flat map all messages and calculate metrics
    messagesWithMetrics = (conversations
      | "extract messages" >> beam.FlatMap(lambda x: x['messages'])
      | "calculate metrics" >> beam.Map(calculateMessageMetrics))

    # Group messages by ticket id and calculate conversation metrics
    metricsByTicket = (messagesWithMetrics
      | "group metrics by ticket" >> beam.GroupBy(byTicket)
        .aggregate_field(lambda message: message['word_count'], sum, 'total_word_count')
        .aggregate_field(lambda message: message['word_count'], min, 'min_message_word_count')
        .aggregate_field(lambda message: message['word_count'], max, 'max_message_word_count')
        .aggregate_field(lambda message: message['word_count'], ConversationAvgWordCount(), 'average_message_word_count')
        .aggregate_field(lambda message: message['tone'], ConversationTone(), 'tone')

      # Aggregation via aggregate_field produces named tuples,
      # which must be translated to key values pairs in order to be used in CoGroupByKey
      | "namedtuple to kv" >> beam.Map(namedTupleToKV))

    # Group messages by ticket id
    messagesByTicket = messagesWithMetrics | "group messages by ticket" >> beam.GroupBy(byTicket)

    # Group conversations by ticket id
    conversationsByTicket = (conversations
      | "group conversations by ticket" >> beam.GroupBy(byTicket)
      # Assuming ticket_id corresponds to only one conversation object
      | "get first element" >> beam.MapTuple(lambda key, value: (key, value[0])))

    # Combine clean conversations, messages with metrics and conversation metrics
    results = ({
      'metrics': metricsByTicket,
      'messages': messagesByTicket,
      'conversation': conversationsByTicket
    }
      | "group by common key" >> beam.CoGroupByKey()
      | "extract values" >> beam.Values()
      | "format result" >> beam.Map(formatConversationResult))

    # Sink results
    (results
      | "convert to json string" >> beam.Map(json.dumps)
      | "write to file" >> beam.io.WriteToText(known_args.output, file_name_suffix='.json'))

    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()