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


sales_reciept = (
    glueContext.create_dynamic_frame.from_catalog(database='raw-coffe-shop', table_name='sales_reciept')
    )
pastry_inventory = (
    glueContext.create_dynamic_frame.from_catalog(database='raw-coffe-shop', table_name='pastry_inventory')
    )
sales_target = (
    glueContext.create_dynamic_frame.from_catalog(database='raw-coffe-shop', table_name='sales_target')
    )
glueContext.write_dynamic_frame.from_options(frame = sales_reciept, connection_type = 's3', connection_options = {'path': 's3://coffe-shop-clean/coffe-shop/sales-reciept', 'partitionKeys': ['transactiondate']}, format = 'parquet')
glueContext.write_dynamic_frame.from_options(frame = pastry_inventory, connection_type = 's3', connection_options = {'path': 's3://coffe-shop-clean/coffe-shop/pastry-inventory'}, format = 'parquet')
glueContext.write_dynamic_frame.from_options(frame = sales_target, connection_type = 's3', connection_options = {'path': 's3://coffe-shop-clean/coffe-shop/sales-target'}, format = 'parquet')
job.commit()