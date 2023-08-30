## Table of content
1. [ Infra issues ](#infa_issues)
2. [ Design ](#design)
3. [ Plan of action ](#plan_of_action)
4. [ Deployment ](#deployment)
5. [ Assumptions ](#assumptions)
6. [ Learnings ](#learnings)
    * [ Spark Configuration Guide ](#spark_config_guide)
    * [ Data Exploration ](#data_exploration)
6. [ Findings ](#findings)
7. [ References ](#references)
 
<a name="infa_issues"></a>
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

<a name="design"></a>
## Design
### Folder Structure
    -- jupyter
        |-- notebook
            |-- dataproducts  # folder which can consist multiple data products
                |-- assesment_nyc_job_posting
                    |-- lib # all the transformation scripts which can be unit tests
                    |-- main # main file only to call transformation. It should not have any logic. This folder will be excluded from unit tests.
                    |-- utils # all the utilities scripts. This folder will be excluded from unit tests.
                    |-- unit_test # folder will have all the unit test scripts. 

<a name="plan_of_action"></a>
## Plan of Action
### Data Exploration
1. Code for Data Exploration - Done
2. Integrate with main file. - Done
3. Write UTs for Data Explorations. - Done
4. Documentation - Done

### Data Cleaning
1. Code for Data Cleaning. - Done
2. Integrate with main file. - Done
3. Write UTs for data cleaning. - In-Progress
4. Documentaion - Done

### KPIs
1. Code for KPIs. - Done
2. Integrate with main file. - Done
3. Write UTs for KPIs. - In-Progress
4. Documentation. - Done

### Visualization
1. Perform computation in spark. - Done
2. Convert it to pandas after computation to visualize it. - Done
3. Visualize data profiling. In-Progress

<a name="deployemnt"></a>
### Deployment
Need to follow below steps for deployment. Assuming sonarqube is available to show coverage report and third party repository to save the artifacts.
CI - 
1. Run pytest to run the unit test cases and create the coverage reports. 
2. Once pytest is successful push the report to sonerqube. 
3. Creating artifacts by creating the zip file of code and jupyter folder.
4. Push the artifactes to thrid part repository.

CD -
1. Download the artifacts from repository.
2. Unzip it.
3. Push the files into required locations. 


<a name="assumptions"></a>
## Assumptions
1. Code will be executed in jupytor notebook. Hence using display Command instead of show to presenting the dataframe content.
2. Most of the time categorical column should be string type but for this analysis assuming that categorical columns can be any type.
3. Rounding is required for salary related columns in KPI.
4. Considering KPI as separate task since it might required data preparation.
5. Converting pyspark to pandas df to show visualization. Here computaion will be done in pyspark and viz will be shown using dataframe.
6. Assuming nltk library will be installed in all the nodes. This we can achive using initialization script.

<a name="learnings"></a>
## Learnings

<a name="spark_config_guide"></a>
### Spark Configuration Guide
#### Setting up spark session configuration in jupytor
1. Before creation spark session set the spark context level configuration
```python
    spark_conf = SparkConf()
    spark_conf.set("spark.executor.instances", "4") 
```

2. Reducing shuffle partition number since data is very less.
```python
spark.conf.set('spark.sql.shuffle.partitions',4)
```
<a name="data_exploration"></a>
### Data Exploration
1. A categorical variable has values that you can put into a countable number of distinct groups based on a characteristic.

### Text Preproccessing
1. Cleaning - Remove punctuation
2. Tokenization - Split into words
3. Remove Stop words - Remove words which doesn't hold any value
4. Lemmatization - converting the word into its dictionary form

<a name="findings"></a>
## Findings
1. Salary range is linked with salary frequency and Hours/Shift. To get any metrics which depends on salary we need to have salary in same frequency. 
2. Need to use excape charater `'"'` because data is getting parsed incorretly. This is the observation from data profiling.

<a name="references"></a>
## References
https://s3.amazonaws.com/assets.datacamp.com/email/other/Data+Visualizations+-+DataCamp.pdf
https://www.geeksforgeeks.org/what-is-feature-engineering/
https://www.youtube.com/watch?v=hhjn4HVEdy0
