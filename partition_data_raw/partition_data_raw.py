import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

raw = (
    glueContext.create_dynamic_frame.from_catalog(database='raw-lowa-liquor-sales', table_name='lowa_liquor_sales').withColumn('invoice_date', col('date'))
    )
glueContext.write_dynamic_frame.from_options(frame = raw, connection_type = 's3', connection_options = {'path': 's3://clean-data-catalog-mk/lowa-liquor-sales/', 'partitionKeys': ['date']}, format = 'parquet')