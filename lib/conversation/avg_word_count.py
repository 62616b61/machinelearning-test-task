from apache_beam import CombineFn

class ConversationAvgWordCount(CombineFn):
  def create_accumulator(self):
    # (sum, count)
    return (0, 0)

  def add_input(self, accumulator, input):
    sum, count = accumulator

    return (sum + input, count + 1)

  def merge_accumulators(self, accumulators):
    total_sum = 0
    total_count = 0

    for sum, count in accumulators:
      total_sum += sum
      total_count += count
      
    return (total_sum, total_count)

  def extract_output(self, accumulator):
    sum, count = accumulator

    return round(sum / count, 2)