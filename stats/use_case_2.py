from stats.input import get_tuples


def run_use_case_2(sc):
    """
    Those use cases have a list of tuples
    """

    distributed_data = sc.parallelize(get_tuples())

    # The first example group the values from the tuples
    print(distributed_data.reduceByKey(lambda a, b: a+b).collect())

    # The second example group the values from the tuples into a dictionary
    print(distributed_data.reduceByKey(lambda a, b: a+b).collectAsMap())

    # The last example show the usage of the flatMap() transformation
    print(distributed_data.flatMap(lambda a: [c for c in a[0]]).filter(lambda a: a.islower()).countByKey())

