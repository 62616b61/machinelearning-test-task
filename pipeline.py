import json
import argparse
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from lib.conversation.avg_word_count import ConversationAvgWordCount
from lib.conversation.tone import ConversationTone
from lib.conversation.format_results import format_conversation_results
from lib.message.metrics import MessageMetrics
from lib.read_data import read_data
from lib.group_by_ticket import by_ticket
from lib.prop_from_keyed_pcol import ExtractPropertyFromKeyedPCollection

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
    data = (p
      | "read data" >> beam.Create(read_data(known_args.input))
      | "group conversatins by ticket" >> beam.GroupBy(by_ticket)
      | "get first element" >> beam.MapTuple(lambda key, value: (key, value[0]))
    )
    
    def parse_conversations(key, conversation):
      # todo: look into dictionary comprehensions
      clean_conversation = {k: v for k, v in conversation.items() if k != 'messages'}
      return (key, clean_conversation)
    
    conversations = (data
      | "conversations" >> beam.MapTuple(parse_conversations))
    
    messages = (data
      | "messages" >> beam.MapTuple(lambda key, value: (key, value['messages']))
      | "message metrics" >> beam.CombineValues(MessageMetrics()))
      
    keyed_word_counts = (messages
      | "extract word_count prop" >> beam.CombineValues(ExtractPropertyFromKeyedPCollection('word_count')))

    keyed_tones = (messages
      | "extract tone prop" >> beam.CombineValues(ExtractPropertyFromKeyedPCollection('tone')))
        
    total_word_count = keyed_word_counts | "sum" >> beam.CombineValues(sum)
    min_message_word_count = keyed_word_counts | "min" >> beam.CombineValues(min)
    max_message_word_count = keyed_word_counts | "max" >> beam.CombineValues(max)
    average_message_word_count = keyed_word_counts | "avg" >> beam.CombineValues(ConversationAvgWordCount())
    conversation_tone = keyed_tones | "tone" >> beam.CombineValues(ConversationTone())

    # Combine clean conversations, messages with metrics and conversation metrics
    results = ({
      'conversation': conversations,
      'messages': messages,
      'total_word_count': total_word_count,
      'min_message_word_count': min_message_word_count,
      'max_message_word_count': max_message_word_count,
      'average_message_word_count': average_message_word_count,
      'tone': conversation_tone,
    }
      | "group by common key" >> beam.CoGroupByKey()
      | "extract values" >> beam.Values()
      | "format result" >> beam.Map(format_conversation_results))

    # Sink results
    (results
      | "convert to json string" >> beam.Map(json.dumps)
      | "write to file" >> beam.io.WriteToText(known_args.output, file_name_suffix='.json'))

    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()