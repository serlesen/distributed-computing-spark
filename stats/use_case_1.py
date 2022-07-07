from stats.input import get_small_int_list


def run_use_case_1(sc):
    """
    Basic use case handling a small list of data.
    """

    distributed_data = sc.parallelize(get_small_int_list())

    # the first example is using the action count() to count the number of elements in the dataset
    print(distributed_data.count())

    # the second example is using the action reduce() to reduce the dataset to a single value
    print(distributed_data.reduce(lambda a, b: a + b))

    # the last example is to use a transformation followed by an action
    print(distributed_data.map(lambda a: a + 1).reduce(lambda a, b: a+b))

