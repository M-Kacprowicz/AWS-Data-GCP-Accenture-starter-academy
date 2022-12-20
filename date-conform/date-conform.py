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
from pyspark.sql.functions import rand,col,max,when,datediff,greatest, md5, to_timestamp, date_format, dayofmonth, year, month, dayofweek, weekofyear
from pyspark.sql.types import StringType, DateType
from awsglue.dynamicframe import DynamicFrame
date = glueContext.create_dynamic_frame.from_catalog(
        database="clean-lowa-liquor-sales",
        table_name="lowa_liquor_sales"
    ).toDF().select(col('date'))
date.printSchema()
date = date.distinct()
date.count()
date = date.withColumn('date_id', md5(col('date')))
date.show(10)
date = date.withColumn('date', col("date").cast(DateType()))
date.show(10)
date = date.withColumn('day_of_week_number', dayofweek(col("date")))
date = date.withColumn("day_name", date_format(col("date"), "EEEE"))
date = date.withColumn("day_name_short", date_format(col("date"), "E"))
date = date.withColumn('day_of_month', date_format(col("date"), "d"))
date = date.withColumn("day_of_year", date_format(col("date"), "D"))
date = date.withColumn("week_of_year", weekofyear(col("date")))
date = date.withColumn("month", date_format(col("date"), "M"))
date = date.withColumn("year", date_format(col("date"), "y"))
date.show(10)
# convert to dynamic frame
date = DynamicFrame.fromDF(date, glueContext, "date")
glueContext.purge_s3_path("s3://conform-data-catalog-mk/date/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = date,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-data-catalog-mk/date/"},
          format = "parquet")
job.commit()