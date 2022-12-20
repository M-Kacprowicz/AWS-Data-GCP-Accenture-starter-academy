import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import rand,col,max,when,datediff,greatest, md5, concat, lit
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
date = (
    glueContext.create_dynamic_frame.from_catalog(
        database="clean-coffe-shop",
        table_name="sales_reciept"
    ).toDF().select(col('transactiondate').cast(StringType()), col('transactiontime').cast(StringType()), col('dayofweekno'), col('dayofweekname'), col('dayofmonthno'), col('weekno'), col('weekname'), col('monthno'),
    col('monthname'), col('quarterno'), col('quartername'), col('year'))
)
date = date.distinct()
date = date.withColumn('dateid', md5(concat(col('transactiondate'), lit('_'), col('transactiontime'))))
# convert to dynamic frame
date = DynamicFrame.fromDF(date, glueContext, "date")
glueContext.purge_s3_path("s3://coffe-shop-conform/date/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = date,
          connection_type = "s3",
          connection_options = {"path": "s3://coffe-shop-conform/date/"},
          format = "parquet")
job.commit()