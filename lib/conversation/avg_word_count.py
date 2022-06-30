from apache_beam import CombineFn

class CombineAverage(CombineFn):
  def create_accumulator(self):
    # (sum, count)
    return (0, 0)

  def add_input(self, accumulator, input):
    sum, count = accumulator

    return (sum + input, count + 1)

  def merge_accumulators(self, accumulators):
    totalSum = 0
    totalCount = 0

    for sum, count in accumulators:
      totalSum += sum
      totalCount += count
      
    return (totalSum, totalCount)

  def extract_output(self, accumulator):
    sum, count = accumulator

    return round(sum / count, 2)