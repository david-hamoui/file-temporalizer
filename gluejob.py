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
    logger.info(f"_______ READING ALL FILES FROM BUCKET {env['bucket']}, FOLDER {env['path']}, TABLE {env['table_name']} _______")
    response = s3.list_objects_v2(Bucket=env['bucket'],Prefix=f"{env['path']}/{env['table_name']}/")

    return response

def convert_to_partitions(s3_objects):

    logger.info(f"_______ PARTITIONING ALL OBJECTS IN {env['table_name']} TO {env['job_mode']} _______")

    joined_df = pd.DataFrame()
    
    for item in s3_objects:
        
        if not check_for_validity(item):
            logger.info(f"_______ Unvalid job_mode: {env['job_mode']}, table is already converted to {env['job_mode']}, STOPPING JOB _______")
            return False
        
        response = s3.get_object(Bucket=env['bucket'],Key=item['Key'])
        body = BytesIO(response['Body'].read())
        data_frame = pd.read_parquet(body)
        data_frame['created_at'] = pd.to_datetime(data_frame['created_at'])
        

        key = item['Key'].split('/')
        for k in key:
            if k.startswith('company='):
                company = k.split('=')[1]

        data_frame['company_temp'] = company
        
        joined_df = pd.concat(objs=[joined_df,data_frame],ignore_index=True)
    
    joined_df = joined_df.sort_values(by='created_at')

    grouped = joined_df.groupby(by=[pd.Grouper(key='created_at',freq=env['job_mode'][0].upper()), 'company_temp'])

    list_of_all_partitions = []
    for (date,company_temp),df in grouped:
        list_of_all_partitions.append([date,company_temp,df.copy()])

    return list_of_all_partitions

def check_for_validity(item):
    last_partition = item['Key'].split('/')[-2].split('_')[-1].split('=')[0]
    if last_partition == env['job_mode'].lower()[:-1]:
        return False
    return True

def upload_partitions_to_s3(list_of_all_partitions):
    logger.info(f"_______ UPLOADING ALL PARTITIONS TO {env['path']}/{env['table_name']}/ _______")

    key = f"{env['path']}/{env['table_name']}/"

    for date,company_temp,df in list_of_all_partitions:
        df = df.drop(columns='company_temp')
        parquet_bytes = BytesIO(df.to_parquet())

        if env['job_mode'] == 'Years':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}company={company_temp}/{env['table_name']}_year={date.year}/{env['table_name']}.parquet")
        elif env['job_mode'] == 'Months':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}company={company_temp}/{env['table_name']}_year={date.year}/{env['table_name']}_month={date.month}/{env['table_name']}.parquet")
        elif env['job_mode'] == 'Days':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}company={company_temp}/{env['table_name']}_year={date.year}/{env['table_name']}_month={date.month}/{env['table_name']}_day={date.day}/{env['table_name']}.parquet")

def delete_previous_files(s3_objects):
    logger.info(f"_______ DELETING EXISTING TABLE: {env['path']}/{env['table_name']}/ _______")

    objects_to_delete = s3_objects

    if 'Contents' in objects_to_delete:
        deleted_objects = []

        for obj in objects_to_delete['Contents']:
            deleted_objects.append({'Key': obj['Key']})

        s3.delete_objects(Bucket=env['bucket'], Delete={'Objects': deleted_objects})
    
    logger.info(f"_______ SUCCESSFULLY DELETED {len(deleted_objects)} OBJECTS FROM {env['path']}/{env['table_name']}/ _______")


## @params: [JOB_NAME]
env = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','table_name','job_mode', 'path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(env['JOB_NAME'], env)

logger = glueContext.get_logger()
logger.info(f"***INITIALIZING GLUEJOB SCRIPT***     Parameters: Bucket = {env['bucket']}, Path = {env['path']}, Table = {env['table_name']} Convert to = {env['job_mode']}")

s3 = boto3.client('s3')

objects = read_all_objects_in_path()

response_from_convertion = convert_to_partitions(objects['Contents'])

if response_from_convertion != False:
    partitioned_dfs = response_from_convertion
    upload_partitions_to_s3(partitioned_dfs)
    delete_previous_files(objects)

logger.info("***FINISHED GLUEJOB SCRIPT***")
job.commit()