# Distributed Computing with PySpark

The content of this repository is explained in my Youtube channel.

https://www.youtube.com/playlist?list=PLab_if3UBk9-trelPibnQGzz2g3j0QF_3

## Chapter 1

In this first chapter, I explain what are the RDD, Resilient Distributed Datasets, in Spark. The RDD is the ability
to distribute the datasets on multiple servers. When building a pipeline of operations, with transformation and
action operations, the dataset will be split in multiple servers to obtain a final and grouped result.

I will explain some transformations and actions to obtain statistical information of numerical datasets, as the 
count, sum or mean. I will explain basic transformations as the map and filter, then I will go with some other as
reduce.

Finally, I explain two important objects in RDD, the accumulators and broadcast. The accumulator is necessary when
a pipeline of operations needs to store transition data. This transition data must also be grouped at the end of
the pipeline. The broadcast is used for read-only data that is necessary for the pipeline execution.

### Run on localhost

```
poetry run python -m stats
```
