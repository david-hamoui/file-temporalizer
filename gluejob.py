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
    s3_objects = response['Contents']

    return s3_objects

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
        
        joined_df = pd.concat(objs=[joined_df,data_frame],ignore_index=True)
    
    joined_df = joined_df.sort_values(by='created_at')

    grouped = joined_df.groupby(by=pd.Grouper(key='created_at',freq=env['job_mode'][0].upper()))

    list_of_all_partitions = []
    for date,df in grouped:
        list_of_all_partitions.append([date,df.copy()])

    return list_of_all_partitions

    '''
        grouped_df = data_frame.groupby(by=pd.Grouper(key='created_at', freq='D'))

        separated_dfs = []
        for date, group in grouped_df:
            separated_dfs.append(group.copy())

        for i, day_df in enumerate(separated_dfs, start=1):
            logger.info(f"DataFrame for Day {i}:")
            logger.info(str(day_df))
            logger.info()'''

    '''
        last_day = -1
        new_dfs = []
        index = -1

        for i in range(len(sorted_df['created_at'])):
            day = sorted_df['created_at'].iloc[i].split(' ')[0].split('-')[2]
            if day == last_day:
                new_dfs[index][1] = pd.concat([sorted_df.iloc[[i]],new_dfs[index][1]],ignore_index=True)
            else:
                new_dfs.append([day, pd.DataFrame(sorted_df.iloc[[i]])])
                last_day = day
                index += 1

        #for i in new_dfs:
            #logger.info(str(i[1]['created_at']))

        month = new_dfs[0][1]['created_at'].iloc[0].split(' ')[0].split('-')[1]
        #logger.info(f"_______ Month {month} _______")
        all_dfs.append([month, new_dfs])

    return all_dfs'''


def check_for_validity(item):
    last_partition = item['Key'].split('/')[-2].split('_')[-1].split('=')[0]
    if last_partition == env['job_mode'].lower()[:-1]:
        return False
    return True

def upload_partitions_to_s3(list_of_all_partitions):
    logger.info(f"_______ UPLOADING ALL PARTITIONS TO {env['path']}/{env['table_name']}/ _______")

    key = f"{env['path']}/{env['table_name']}/company=Locaweb/{env['table_name']}_year="

    for date,df in list_of_all_partitions:
        parquet_bytes = BytesIO(df.to_parquet())

        if env['job_mode'] == 'Years':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}{date.year}/{env['table_name']}.parquet")
        elif env['job_mode'] == 'Months':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}{date.year}/{env['table_name']}_month={date.month}/{env['table_name']}.parquet")
        elif env['job_mode'] == 'Days':
            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key = f"{key}{date.year}/{env['table_name']}_month={date.month}/{env['table_name']}_day={date.day}/{env['table_name']}.parquet")


    '''
    key = f"specialized/incidents/company=Locaweb/{env['path']}test_folder/"
    s3.put_object(Bucket=env['bucket'], Key=key)

    logger.info(f"_______ UPLOADING ALL OBJECTS IN all_dfs TO {key} _______")
    
    for month in all_dfs:
        s3.put_object(Bucket=env['bucket'],Key=f"{key}month={month[0]}/")
        for day in month[1]:
            parquet_bytes = BytesIO(day[1].to_parquet())

            s3.upload_fileobj(Fileobj=parquet_bytes, Bucket=env['bucket'], Key=f"{key}month={month[0]}/day={day[0]}.parquet")'''

def delete_previous_table():
    logger.info(f"_______ DELETING EXISTING TABLE: {env['path']}/{env['table_name']}/ _______")

    objects_to_delete = s3.list_objects_v2(Bucket=env['bucket'], Prefix=f"{env['path']}/{env['table_name']}/")

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

response_from_convertion = convert_to_partitions(objects)

if response_from_convertion != False:
    delete_previous_table()
    partitioned_dfs = response_from_convertion
    upload_partitions_to_s3(partitioned_dfs)



logger.info("***FINISHED GLUEJOB SCRIPT***")
job.commit()