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
county = (
    glueContext.create_dynamic_frame.from_catalog(
        database="clean-lowa-liquor-sales",
        table_name="lowa_liquor_sales",
        push_down_predicate ="date == '2021-01-04'"
    ).toDF().select(col('county_number').cast(StringType()), col('county'))
)
county.printSchema()
county = county.distinct()
county.count()
county = county.withColumn('county_id', md5(col('county_number')))
county.show(10)
# convert to dynamic frame
county = DynamicFrame.fromDF(county, glueContext, "county")
glueContext.purge_s3_path("s3://conform-data-catalog-mk/county/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = county,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-data-catalog-mk/county/"},
          format = "parquet")
job.commit()