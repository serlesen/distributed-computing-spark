from pyspark import SparkContext, SparkConf

from stats.use_case_1 import run_use_case_1
from stats.use_case_2 import run_use_case_2
from stats.use_case_3 import run_use_case_3
from stats.use_case_4 import run_use_case_4

app_name = "stats"
master = "local[5]"

conf = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=conf)

run_use_case_1(sc)
run_use_case_2(sc)
run_use_case_3(sc)
run_use_case_4(sc)
