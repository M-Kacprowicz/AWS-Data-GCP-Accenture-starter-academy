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
from pyspark.sql.functions import rand,col,max,when,datediff,greatest, md5
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
staff = (
    glueContext.create_dynamic_frame.from_catalog(
        database="clean-coffe-shop",
        table_name="sales_reciept"
    ).toDF().select(col('staffno').cast(StringType()), col('stafffirstname'), col('stafflastname'), col('staffposition'), col('staffstartdate'))
)
staff = staff.distinct()
staff = staff.withColumn('staffid', md5(col('staffno')))
# convert to dynamic frame
staff = DynamicFrame.fromDF(staff, glueContext, "staff")
glueContext.purge_s3_path("s3://coffe-shop-conform/staff/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = staff,
          connection_type = "s3",
          connection_options = {"path": "s3://coffe-shop-conform/staff/"},
          format = "parquet")
job.commit()