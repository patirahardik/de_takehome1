from pyspark.sql import SparkSession

def get_spark_session(spark_conf):

    spark = SparkSession.builder.config(conf=spark_conf).appName("MySparkApp").getOrCreate()

    return spark