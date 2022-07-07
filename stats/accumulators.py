from pyspark.accumulators import AccumulatorParam


class CountAccumulator(AccumulatorParam):

    def __init__(self, initial_count=0):
        self.count = initial_count

    def zero(self, value):
        return None

    def addInPlace(self, val1, val2):
        if isinstance(val1, CountAccumulator):
            new_count = val1.count
        elif val1 is None:
            new_count = 0
        else:
            new_count = 1

        if isinstance(val2, CountAccumulator):
            new_count += val2.count
        elif val2 is not None:
            new_count += 1
        return CountAccumulator(new_count)


class MeanAccumulator(AccumulatorParam):

    def __init__(self, initial_count=0, initial_sum=0):
        self.count = initial_count
        self.sum = initial_sum

    def zero(self, value):
        return MeanAccumulator()

    def addInPlace(self, val1, val2):
        if isinstance(val1, MeanAccumulator):
            new_count = val1.count
            new_sum = val1.sum
        elif val1 is None:
            new_count = 0
            new_sum = 0
        else:
            new_count = 1
            new_sum = val1

        if isinstance(val2, MeanAccumulator):
            new_count += val2.count
            new_sum += val2.sum
        elif val2 is not None:
            new_count += 1
            new_sum += val2
        return MeanAccumulator(new_count, new_sum)

    @property
    def mean(self):
        if self.count == 0:
            return 0
        return self.sum / self.count
