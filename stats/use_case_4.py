from stats.input import get_available_big_filename


def run_use_case_4(sc):
    """
    Those use cases use a big file that must be computed in several runners
    And they also show the usage of the broadcast
    """

    broadcast_value = sc.broadcast({"column": 5, "op": ["count"]})
    distributed_file = sc.textFile(get_available_big_filename())

    # the first example show a complex pipeline chaining multiple transformation and a final action with the broadcast
    print(distributed_file.map(lambda line: line.split(",")).filter(lambda line: "col" not in line[0]).map(lambda line: [int(c) for c in line]).map(lambda line: line[broadcast_value.value["column"]]).sum())

    def read_value_from_line(line):
        columns = line.split(",")
        if "col" in columns[0]:
            # it's the header
            return 0
        return int(columns[broadcast_value.value["column"]])

    # the last example show the usage of an external method where the broadcast is used
    print(distributed_file.map(read_value_from_line).sum())
