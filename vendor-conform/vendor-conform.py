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
vendor = (
    glueContext.create_dynamic_frame.from_catalog(
        database="clean-lowa-liquor-sales",
        table_name="lowa_liquor_sales",
        push_down_predicate ="date == '2021-01-04'"
    ).toDF().select(col('vendor_number').cast(StringType()), col('vendor_name'))
)
vendor.printSchema()
vendor = vendor.distinct()
vendor.count()
vendor = vendor.withColumn('vendor_id', md5(col('vendor_number')))
vendor.show(10)
# convert to dynamic frame
vendor = DynamicFrame.fromDF(vendor, glueContext, "vendor")
glueContext.purge_s3_path("s3://conform-data-catalog-mk/vendor/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = vendor,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-data-catalog-mk/vendor/"},
          format = "parquet")
job.commit()