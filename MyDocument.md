### Infra issue

#### Spark UI not opening

## Infra issue

### Spark UI not opening

**Issue 1.** Port binding was not configured in `jupytor` node. 
**Fix:**
Assigning 4040 to jupyter node and starting spark master and worker port from 4041 port
Before:
```yaml
jupyter:
    ports:
        - 8888:8888
```
After
```yaml    
jupyter:
    ports:
        - "8888:8888"
        - "4040:4040"
```
**Issue 2.** Spark UI is not redirecting to right hostname from spark application UI.
**Fix:**: `In-Progress`

**Status**: After fixing Issue 1, Spark UI is opening on url http://127.0.0.1:4040


## Spark Configuration Guide
### Setting up spark session configuration in jupytor
1. Before creation spark session set the spark context level configuration
```python
    spark_conf = SparkConf()
    spark_conf.set("spark.executor.instances", "4") 
```

2. Reducing shuffle partition number since data is very less.
```python
spark.conf.set('spark.sql.shuffle.partitions',4)
```

## Assumptions
1. Code will be executed in jupytor notebook. Hence using display Command instead of show to presenting the dataframe content.
2. Most of the time categorical column should be string type but for this analysis assuming that categorical columns can be any type.
3. Rounding is required for salary related columns in KPI.

## Learnings
#### Data Exploration
1. A categorical variable has values that you can put into a countable number of distinct groups based on a characteristic.

## Findings
1. Salary range is linked with salary frequency and Hours/Shift. To get any metrics which depends on salary we need to have salary in same frequency. 
2. Need to use excape charater `'"'` because data is getting parsed incorretly. This is the observation from data profiling.


## References
https://s3.amazonaws.com/assets.datacamp.com/email/other/Data+Visualizations+-+DataCamp.pdf
