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
product_category = (
    glueContext.create_dynamic_frame.from_catalog(
        database="clean-lowa-liquor-sales",
        table_name="lowa_liquor_sales",
        push_down_predicate ="date == '2021-01-04'"
    ).toDF().select(col('category').cast(StringType()), col('category_name'))
)
product_category.printSchema()
product_category = product_category.distinct()
product_category.count()
product_category = product_category.withColumn('product_category_id', md5(col('category')))
product_category.show(10)
# convert to dynamic frame
product_category = DynamicFrame.fromDF(product_category, glueContext, "product_category")
glueContext.purge_s3_path("s3://conform-data-catalog-mk/product-category/", {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(frame = product_category,
          connection_type = "s3",
          connection_options = {"path": "s3://conform-data-catalog-mk/product-category/"},
          format = "parquet")
job.commit()