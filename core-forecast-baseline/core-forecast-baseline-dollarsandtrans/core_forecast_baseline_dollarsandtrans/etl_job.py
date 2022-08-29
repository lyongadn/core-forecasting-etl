"""
This file is used to run the Step 2 of BASELINE forecast.
Prior to this file is run, the output of Part 1 is loaded into Aurora DB.
Step 2:
    Fetches the 15 min level metric wise data from RDS based on the output of Part 1
    which is loaded into Aurora. Then forecast_average for each prediction_date is calculated.
    Output of Part 2 which consist of trend for each store, prediction date is fetched from S3
    and final forecast is calculated for a particular metric and saved in S3.
"""
import time as time1
import sys
import math
import json
from datetime import datetime,date,time,timedelta
import pytz
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
print("Starting Execution")
t0 = time1.time()
EST = pytz.timezone('America/New_York')

PARAMS = {
    "test": {
        # "mysql-dbtable": "ml_preprod.initial_itemlevelcount_15min",
        "secret_name": "cfa-rdskey-integration",
        "metric-tables":{
            "dollartrans": "ml_preprod.initial_dollarsalesandtranscount_15min",
            "itemcount": "ml_preprod.initial_itemlevelcount_15min"
        },
        "metrics":{
            "dollartrans": ["sales_sub_total","trans_count"],
            "itemcount": ["sum_daily_quantity"]
        },
        "store_list_filepath": "s3://test-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"test-q-forecasting-artifacts",
        "forecast_output_path": {
        "dollartrans": "baseline/forecast/dollartrans",
        "itemcount": "baseline/forecast/itemcount"
        },
        "baseline_dates_table": "tmp.baseline_dates",
        "baseline_dates_path":"baseline/Dates",
        "baseline_trend_path":"baseline/Trend",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x))  for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x))  for x in range(21,25)]
        },
        "step_1_data_fetch_days_limit": 200,
        "last_year_dates_lag": 364,
        "default_product_number": 99999,
        "lag_after_high_low_removal": 3,
        "saturday_off_stores_table": "ml_preprod.saturday_off_stores",
        "forecast_file_name": "Forecast_File.csv",
        "batch_size":{
            "dollartrans":50,
            "itemcount":50
        }
    },
    "prod": {
        # "mysql-dbtable": "ml_preprod.initial_itemlevelcount_15min",
        "secret_name": "cfa-rdskey-prod",
        "metric-tables":{
            "dollartrans": "ml_preprod.initial_dollarsalesandtranscount_15min",
            "itemcount": "ml_preprod.initial_itemlevelcount_15min"
        },
        "metrics":{
            "dollartrans": ["sales_sub_total","trans_count"],
            "itemcount": ["sum_daily_quantity"]
        },
        "store_list_filepath": "s3://prod-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"prod-q-forecasting-artifacts",
        "forecast_output_path": {
        "dollartrans": "baseline/forecast/dollartrans",
        "itemcount": "baseline/forecast/itemcount"
        },
        "baseline_dates_table": "tmp.baseline_dates",
        "baseline_dates_path":"baseline/Dates",
        "baseline_trend_path":"baseline/Trend",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x))  for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x))  for x in range(21,25)]
        },
        "step_1_data_fetch_days_limit": 200,
        "last_year_dates_lag": 364,
        "default_product_number": 99999,
        "lag_after_high_low_removal": 3,
        "saturday_off_stores_table": "ml_preprod.saturday_off_stores",
        "forecast_file_name": "Forecast_File.csv",
        "batch_size":{
            "dollartrans":500,
            "itemcount":300
        }
    }
}

# Get args to initiate job based on env and Metric
args                      = getResolvedOptions(sys.argv, ['JOB_NAME','METRIC', 'ENV'])
ENV                       = args['ENV']
metric                    = args['METRIC']
PARAMS[ENV]['metric']     = args['METRIC']
store_list                = pd.read_csv(PARAMS[ENV]['store_list_filepath'])['location_num'].tolist()
PARAMS[ENV]['store_list'] = store_list
sc                        = SparkContext(appName='baseline_15min')
glueContext               = GlueContext(sc)
spark                     = glueContext.spark_session
job                       = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_secret():
    """
      This function fetches the required secrets from AWS Secrets Manager to get the
      RDS url, username and password to connect to Aurora Database.
    """
    secret_name = PARAMS[ENV]['secret_name']
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_string = get_secret_value_response['SecretString']

    except Exception as exc:
        print(f'Error: {exc}')

    return json.loads(secret_string)

secret = get_secret()

batch_size = PARAMS[ENV]['batch_size'][metric]
batch_count = int(math.ceil(len(PARAMS[ENV]['store_list'])/batch_size))
batches = [PARAMS[ENV]['store_list']] if batch_size == 0 else \
          [PARAMS[ENV]['store_list'][i*batch_size:(i+1)*batch_size] for i in range(batch_count)]
print(f"Total number of stores:- {len(PARAMS[ENV]['store_list'])}")
print(f"Total number of batches:- {batch_count}")
print(f"Batch size :- {batch_size}")

for batch_no,batch in zip(range(1,batch_count+1),batches):
    t1 = time1.time()
    PARAMS[ENV]['store_list'] = batch
    print("Started running for batch number:-",batch_no)


    ############ Getting Part 1 and Part 2 results and filtering the required stores data
    stores = spark.createDataFrame([[x] for x in PARAMS[ENV]['store_list']],schema=['location_num'])
    dates_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['baseline_dates_path']}"
    trend_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['baseline_trend_path']}"

    dates = spark.read.csv(dates_path,header=True)
    dates = stores.join(dates,on='location_num',how='left')\
                  .repartition(len(PARAMS[ENV]['store_list']),'location_num')
    trend = spark.read.csv(trend_path,header=True)
    trend = stores.join(trend,on='location_num',how='left')\
                  .repartition(len(PARAMS[ENV]['store_list']),'location_num')

    ############ Generating the conditions for each store
    COND = """
    location_num = __store_num__
    """
    pred_date = max(PARAMS[ENV]['prediction_dates'])
    predicates = [ COND.replace('__store_num__',str(store)) for store in PARAMS[ENV]['store_list'] ]
    for x in predicates:
        print(x)

    ############ Query on which the condition will be performed
    table = f"""
    (select
        distinct location_num as store,business_date as prior_date
    from
        baseline.dates) dt
    inner join {PARAMS[ENV]['metric-tables'][metric]} id 
    on
        dt.store = id.location_num and
        dt.prior_date = id.business_date
    """

    ############ Fetching the data from RDS for each store in parallel
    df = spark.read.jdbc(
        secret["mysql-url"],
        table,
        predicates=predicates,
        properties={ 'user' : secret["username"], 'password' : secret["password"] }
    )
    
    df = df.distinct()

    ############ Getting only the required columns and repartitioning the data
    if metric != 'itemcount':
        df = df.withColumn('product',F.lit(PARAMS[ENV]['default_product_number']))

    # Remove duplicates when there are 2 rows for same date,interval and different generation date
    # One with latest generation_date is taken
    dedup_window = Window.partitionBy('location_num','product','timequarter')
    df = df.withColumn('max_gen_date',F.max('generation_date').over(dedup_window))\
           .where('generation_date = max_gen_date')

    df = df.select('location_num','product','business_date',
                   F.col('timequarter').substr(12,8).alias('interval'),*PARAMS[ENV]['metrics'][metric])\
           .withColumn('dayofweek',F.dayofweek('business_date'))\
           .repartition(len(predicates),'location_num','product')
    df.persist()
    print("Fetched Data:- RowCount-",df.count())

    ############ Getting list of store,products combinations
    if metric != 'itemcount':
        products = stores.withColumn('product',F.lit(PARAMS[ENV]['default_product_number']))
    else:
        products = df.select('location_num','product').distinct()


    ############ Generating the final forecast dataframe
    dates_2weeks = PARAMS[ENV]['prediction_dates']['2weeks_ahead']
    dates_3weeks = PARAMS[ENV]['prediction_dates']['3weeks_ahead']
    prediction_dates_2weeks = spark.createDataFrame(
                                      [[x] for x in dates_2weeks],
                                      schema=['business_date']
                                    )
    prediction_dates_3weeks = spark.createDataFrame(
                                      [[x] for x in dates_3weeks],
                                      schema=['business_date']
                                    )
    prediction_dates = prediction_dates_2weeks.union(prediction_dates_3weeks).repartition(1)

    intervals = [
                  [time(hour=math.floor(t*15/60),minute=(t*15)%60,second=0).strftime("%H:%M:%S")]
                  for t in range(96)
                ]
    intervals = spark.createDataFrame(intervals,schema=['interval']).repartition(1)

    prediction_dates = prediction_dates.crossJoin(intervals)
    forecast = products.crossJoin(prediction_dates).repartition(len(predicates),'location_num')


    ############ Aggregating and calculating the forecast average value for each interval
    df1 = df.join(dates,on=['location_num','business_date'],how='right')\
             .groupBy(['location_num','product','prediction_date',
                       'interval','lookup_weeks','forecast_avg_dr'])\
             .agg(
                  F.collect_list('business_date').alias('dates_list'),
                  *[F.collect_list(m).alias('metric_values') for m in PARAMS[ENV]['metrics'][metric]],
                  *[F.sum(m).alias('cy_sum_'+m) for m in PARAMS[ENV]['metrics'][metric]]
              )
    for m in PARAMS[ENV]['metrics'][metric]:
        df1 = df1.withColumn('fcast_avg_'+m,F.when(
                  F.col('forecast_avg_dr')>0,
                  F.col('cy_sum_'+m)/(F.col('forecast_avg_dr'))
              ).otherwise(0.0))
    df1.persist()
    print("Aggregated data:- ",df1.count())

    ############ Calculating the final forecast
    df2 = df1.join(trend,on=['location_num','prediction_date'],how='inner')
    for m in PARAMS[ENV]['metrics'][metric]:
        df2 = df2.withColumn('forecast_'+m, F.col('fcast_avg_'+m)+\
                    (F.col('fcast_avg_'+m)*F.col('trend')*F.col('smoothing_fact')))\

    df2 = df2.select('location_num','product',F.col('prediction_date').alias('business_date'),\
               'interval',*['forecast_'+m for m in PARAMS[ENV]['metrics'][metric] ])
    df2.persist()
    print("Forecast with missing data:- ",df2.count())

    ############ Adding missing data and pad forecast with 0's to forecast
    output3 = forecast.join(df2,how='left',
                        on=['location_num','product','business_date','interval'])\
                      .orderBy('location_num','product','business_date','interval')\
                      .withColumn('store_num',F.col('location_num'))\
                      .withColumn('generation_date',F.lit(str(datetime.now(EST).date())))

    for m in PARAMS[ENV]['metrics'][metric]:
        output3 = output3.withColumn('forecast_'+m,F.coalesce(F.col('forecast_'+m),F.lit(0)))
        output3 = output3.select('business_date','product','interval',\
                        *['forecast_'+m for m in PARAMS[ENV]['metrics'][metric] ],\
                        'generation_date','location_num','store_num')
  #      output3 = output3.select('business_date','product',\
  #             'interval',*['forecast_'+m for m in PARAMS[ENV]['metrics'][metric] ],'generation_date','store_num')

    if metric != 'itemcount':
        output3 = output3.drop('product')

    output3.persist()
    print("Final forecast:- ",output3.count())

    ############ Fetch Saturday Off stores


    ############ Separating the 2 weeks out and 3 weeks out forecast
    output3_2weeks = output3.where(F.col('business_date').isin(dates_2weeks))
    output3_3weeks = output3.where(F.col('business_date').isin(dates_3weeks))


    result_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['forecast_output_path'][metric]}/"

    ############ Storing the 2 weeks out and 3 weeks out forecast in S3 partitioned by location_num
    MODE = "overwrite" if batch_no == 1 else "append"
    output3_2weeks.repartition('location_num')\
           .write\
           .partitionBy('store_num')\
           .format('csv')\
           .option("header",True)\
           .mode(MODE)\
           .save(result_path+"2weeks_ahead/")

    time1.sleep(30)

    output3_3weeks.repartition('location_num')\
           .write\
           .partitionBy('store_num')\
           .format('csv')\
           .option("header",True)\
           .mode(MODE)\
           .save(result_path+"3weeks_ahead/")

    t2 = time1.time()
    print(f"Done running for batch number {batch_no} for {len(batch)} stores")
    print(f"Time of execution for batch number {batch_no}:- {t2-t1}")

    spark.catalog.clearCache()

print("Forecast generated and stored in S3.")
# ############### Joining and Renaming Forecast files in S3

# t1 = time1.time()
# s3 = boto3.client('s3')
# s3_res = boto3.resource('s3')
# forecast_types = ['2weeks_ahead','3weeks_ahead']
# final_files = []

# def join_and_rename_files(files_list,bucket,final_path):
#     """
#     This function will combine all the files in files_list into single csv file
#     and will store at final_path in S3 bucket.
#     """
#     dest_file = f"s3://{bucket}/{final_path}"
#     files_list = [file for file in files_list if not file['Key'].endswith('/')]

#     if len(files_list) == 0:
#         print("No file found for "+final_path)
#         print()
#         return []

#     if len(files_list) == 1:
#         print("Only one file found")
#         if final_path == files_list[0]['Key']:
#             print('Source and destination name is the same so not doing anything.')
#         else:
#             s3_res.Object(bucket,final_path)\
#                   .copy_from(CopySource={"Bucket":bucket,"Key":files_list[0]['Key']})
#             s3_res.Object(bucket,files_list[0]['Key']).delete()

#     else:
#         print(f"{len(files_list)} files found.")
#         final_data = pd.DataFrame()
#         for file_x in files_list:
#             print("Reading file :- ",file_x['Key'])
#             data = pd.read_csv(f"s3://{bucket}/{file_x['Key']}")
#             s3_res.Object(bucket,file_x['Key']).delete()
#             final_data = final_data.append(data)

#         final_data.to_csv(dest_file,index=False,header=True)
#     print(f"Saved the final file at :- {dest_file}")
#     print()
#     return [dest_file]

# def get_s3_files_list(bucket,prefix):
#     """
#     This function will list all the files in one prefix and return a list of those files
#     """
#     files = []
#     res = s3.list_objects_v2(
#                 Bucket=bucket,
#                 Prefix=prefix
#             )
#     is_truncated = res['IsTruncated']
#     # print(res)

#     files.extend(res.get('Contents',[]))
#     while is_truncated:
#         res = s3.list_objects_v2(
#                     Bucket=bucket,
#                     Prefix=prefix,
#                     ContinuationToken=res['NextContinuationToken']
#                 )
#         is_truncated = res['IsTruncated']
#         files.extend(res.get('Contents',[]))
#     return files

# S3_BUCKET = PARAMS[ENV]['bucket']
# for store in store_list:
#     for forecast_type in forecast_types:
#         s3_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['forecast_output_path'][metric]}/{forecast_type}/store_num={store}/"
#         # print(s3_path)
#         all_files = get_s3_files_list(S3_BUCKET,s3_path)
#         x = join_and_rename_files(all_files,S3_BUCKET,s3_path+PARAMS[ENV]['forecast_file_name'])
#         final_files.extend(x)

# print("All the forecast files in S3 are:-")
# for file in final_files:
# print(file)


# t2 = time1.time()
# print("File name change done")
# print("Time of execution for renaming files:- ",t2-t1)

####################################### Step 2 done #######################################

t3 = time1.time()
print("Total Time of execution:- ",t3-t0)
print("Step 2 done")
