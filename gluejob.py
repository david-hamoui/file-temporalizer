import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
from io import BytesIO

def read_all_objects_in_path():
    response = s3.list_objects_v2(Bucket=env['bucket'],Prefix=f"specialized/interactions/company=Locaweb/{env['path']}")
    s3_objects = response['Contents']

    return s3_objects

def convert_to_days(objects):
    for item in objects:
        month_df = pd.DataFrame(data=item['Key'])
        sorted_df = month_df.sort_values(by='created_at')

        last_day = -1
        new_dfs = []
        index = -1

        for i in range(len(sorted_df['created_at'])):
            if sorted_df['created_at'].iloc[i].item() == last_day:
                new_dfs[index] = pd.concat([sorted_df.iloc[[i]],new_dfs[index]],ignore_index=True)
            else:
                new_dfs.append(pd.DataFrame(sorted_df.iloc[[i]]))
                last_day = sorted_df['created_at'].iloc[i]
                index += 1

        for i in new_dfs:
            logger.info(str(i))
            


## @params: [JOB_NAME]
env = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','convert_to','path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(env['JOB_NAME'], env)

logger = glueContext.get_logger()
logger.info(f"***INITIALIZING GLUEJOB SCRIPT***     Parameters: Bucket = {env['bucket']}, Path = {env['path']}, Convert to = {env['convert_to']}")

s3 = boto3.client('s3')

objects = read_all_objects_in_path()

if env['convert_to'] == 'Days':
    convert_to_days(objects)


logger.info("***FINISHED GLUEJOB SCRIPT***")
job.commit()