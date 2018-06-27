from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


sc = SparkContext()
glueContext = GlueContext(sc)


df = glueContext.read\
     .format("com.databricks.spark.redshift")\
     .option("url", "jdbc:redshift://redshift01.ap-southeast-2.redshift.amazonaws.com:5439/gluetest?user=dongaws&password=<password>")\
     .option("dbtable", "datetest")\
     .option("forward_spark_s3_credentials",'true')\
     .option("tempdir", "s3://dongaws/tmp/")\
     .load()


df2=df.filter(df.insertdate<>"2018-06-12 00:00:00")


df2.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://redshift01.ap-southeast-2.redshift.amazonaws.com:5439/gluetest?user=dongaws&password=<password>")\
  .option("dbtable", "datetest")\
  .option("forward_spark_s3_credentials",'true')\
  .option("tempdir", "s3://dongaws/tmp/")\
  .mode("overwrite") \
  .save()
