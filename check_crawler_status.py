import boto3
import time
from botocore.exceptions import ClientError



def check_a_crawler(crawler_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response['Crawler']['State']
        print(status)
        if status == 'READY':
            print('Crawler finished the job')
            break
        else:
            print('waiting 15 seconds for crawler to finish')
        time.sleep(15)
def lambda_handler(event, context):
   check_a_crawler(crawler_name = 'crawler-lowa-liquor-sales-conform')