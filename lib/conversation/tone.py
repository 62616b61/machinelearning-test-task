from apache_beam import CombineFn

class ConversationTone(CombineFn):
  def create_accumulator(self):
    # positive_count, negative_count
    return (0, 0)

  def add_input(self, accumulator, input):
    positive, negative = accumulator

    if input == 'positive':
      return (positive + 1, negative)

    if input == 'negative':
      return (positive, negative + 1)

    return accumulator

  def merge_accumulators(self, accumulators):
    totalPositive = 0
    totalNegative = 0

    for positive, negative in accumulators:
      totalPositive += positive
      totalNegative += negative
      
    return (totalPositive, totalNegative)

  def extract_output(self, accumulator):
    positive, negative = accumulator
    
    if positive > negative:
      return 'positive'

    if negative > positive:
      return 'negative'

    return 'mixed'