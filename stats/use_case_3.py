from stats.accumulators import CountAccumulator, MeanAccumulator
from stats.input import get_big_int_list


def run_use_case_3(sc):
    """
    Those use cases use a big list that must be computed in several runners
    And they also show the usage of the accumulators
    """

    distributed_data = sc.parallelize(get_big_int_list())

    # The first example use the built-in accumulator of int
    acc = sc.accumulator(0)
    distributed_data.foreach(lambda l: acc.add(l))
    print(acc.value)

    # The second example use a custom accumulator to obtain the same result as the first one
    acc = sc.accumulator(None, CountAccumulator())
    distributed_data.foreach(lambda l: acc.add(l))
    print(acc.value.count)

    # The last example use a custom accumulator to obtain a complex result
    acc = sc.accumulator(None, MeanAccumulator())
    distributed_data.foreach(lambda l: acc.add(l))
    print(acc.value.mean)
