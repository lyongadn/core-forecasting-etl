"""
This is Step 1 of baseline architecture.
This file is used to run the Part 1 and Part 2 of baseline forecast.
Part 1:
    Fetches the daily level net sales data from RDS Aurora DB and calculates
    the list of dates for each prediction dates for each store of which 15 min
    level data will be needed to calculate the current year forecast_average.

Part 2:
    Fetches the daily level net sales data from RDS Aurora DB and
    calculates the trend for each prediction dates for each store.

Output of Part 1 and Part 2 are stored as a csv file in S3.
Output of Part 1 is loaded into Aurora DB and then used to fetch 15 min level data for Part 3.
"""
import sys
import os
import time
import json
from datetime import datetime,date,timedelta
import boto3
import pandas as pd
import pytz
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
print("Starting Execution")
t1 = time.time()
sc = SparkContext(appName='baseline_15min')

args = getResolvedOptions(sys.argv, ['JOB_NAME','ENV'])
ENV = args['ENV']
EST = pytz.timezone('America/New_York')

PARAMS = {
    "test" : {
        "secret_name": "cfa-rdskey-integration",
        # "mysql-dbtable": "ml_preprod.initial_itemlevelcount_15min",
        "netsales_daily_table": "ml_preprod.initial_dollarsalesandtranscount_daily",
        "itemcount_table":"ml_preprod.initial_itemlevelcount_daily",
        "saturday-offstores-table": "ml_preprod.saturday_off_stores",
        "store_list_filepath": "s3://test-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"test-q-forecasting-artifacts",
        "baseline_dates_path":"baseline/Dates",
        "baseline_trend_path":"baseline/Trend",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(21,25)]
        },
        "default_lookup_weeks": 5,
        "default_smoothing_factor": 0.0,
        "step_1_data_fetch_days_limit": 200,
        "last_year_dates_lag": 364,
        "default_product_number": 99999,
        "lag_after_high_low_removal": 3,
        "saturday_off_stores_table": "ml_preprod.saturday_off_stores"
    },
    "prod": {
        "secret_name": "cfa-rdskey-prod",
        # "mysql-dbtable": "ml_preprod.initial_itemlevelcount_15min",
        "netsales_daily_table": "ml_preprod.initial_dollarsalesandtranscount_daily",
        "itemcount_table":"ml_preprod.initial_itemlevelcount_daily",
        "saturday-offstores-table": "ml_preprod.saturday_off_stores",
        "store_list_filepath": "s3://prod-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"prod-q-forecasting-artifacts",
        "baseline_dates_path":"baseline/Dates",
        "baseline_trend_path":"baseline/Trend",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(21,25)]
        },
        "default_lookup_weeks": 5,
        "default_smoothing_factor": 0.0,
        "step_1_data_fetch_days_limit": 200,
        "last_year_dates_lag": 364,
        "default_product_number": 99999,
        "lag_after_high_low_removal": 3,
        "saturday_off_stores_table": "ml_preprod.saturday_off_stores"
    }
}

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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
PARAMS[ENV]['store_list'] = pd.read_csv(PARAMS[ENV]['store_list_filepath'])['location_num'].tolist()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

informs = spark.read.csv('informs.csv',header=True)
informs = informs.select('Location Number','New Forecast Setting Naming Convention')
informs = informs.withColumnRenamed('New Forecast Setting Naming Convention','naming_conv')\
                 .withColumnRenamed('Location Number','location_num')\
                 .withColumn('lookup_weeks',F.col('naming_conv').substr(1,1).astype('int'))\
                 .withColumn('smoothing_fact',F.col('naming_conv').substr(7,2).astype('int')/100)\
                 .select('location_num','lookup_weeks','smoothing_fact')\
                 .withColumn('location_num',F.col('location_num').cast('int'))

stores = spark.createDataFrame([[x] for x in PARAMS[ENV]['store_list']],schema=['location_num'])
pred_dates_2weeks = spark.createDataFrame(
                        [[x] for x in PARAMS[ENV]['prediction_dates']['2weeks_ahead']],
                        schema=['prediction_date']
                    )
pred_dates_3weeks = spark.createDataFrame(
                        [[x] for x in PARAMS[ENV]['prediction_dates']['3weeks_ahead']],
                        schema=['prediction_date']
                    )
informs_spark = stores.join(informs,on=['location_num'],how='left')\
                  .withColumn('lookup_weeks',F.coalesce(
                            F.col('lookup_weeks'),
                            F.lit(PARAMS[ENV]['default_lookup_weeks'])
                  ))\
                  .withColumn('smoothing_fact',F.coalesce(
                            F.col('smoothing_fact'),
                            F.lit(PARAMS[ENV]['default_smoothing_factor'])
                  ))

informs_2weeks = informs_spark.crossJoin(pred_dates_2weeks)

informs_3weeks = informs_spark.crossJoin(pred_dates_3weeks)\
                  .withColumn('lookup_weeks',F.col('lookup_weeks')-1)\

informs_spark_2 = informs_2weeks.union(informs_3weeks)\
                  .withColumn('dayofweek', F.dayofweek('prediction_date'))


################ Fetching list of stores from RDS which are off on saturdays
saturday_off_stores = spark.read.jdbc(
    secret["mysql-url"],
    PARAMS[ENV]["saturday-offstores-table"],
    properties={ 'user' : secret["username"], 'password' : secret["password"] }
).withColumn('saturday_off',F.lit(1))
saturday_off_stores.persist()
print('Saturday off stores:- ',saturday_off_stores.count())


################# Finding and removing prediction_dates on which stores are closed from output
all_prediction_dates = PARAMS[ENV]['prediction_dates']['2weeks_ahead'].copy()
all_prediction_dates.extend(PARAMS[ENV]['prediction_dates']['3weeks_ahead'])
# print(all_prediction_dates,saturday_off_stores.toPandas())

def holiday_check(year,month,day):
    """
    This function will check whether the given day is a holiday or not for stores
    """
    # Checking for thanksgiving which is fourth thursday in November month
    if month == 11 and 28>=day>=22 and date(year,month,day).weekday() == 3:
        check = 1
    # Checking for Christmas
    elif month == 12 and day==25:
        check = 1
    else:
        check = 0
    return check

all_dates = [datetime.strptime(x,"%Y-%m-%d").date() for x in all_prediction_dates]
all_year_months = [(x.year,x.month,x.day) for x in all_dates]
holiday_sunday_dates = spark.createDataFrame([
                    (date(*x).strftime('%Y-%m-%d'),holiday_check(*x))
                    for x in all_year_months],schema=['prediction_date','holiday']
                )\
                .withColumn('dayofweek',F.dayofweek('prediction_date'))\
                .withColumn('sunday',F.when(F.col('dayofweek')==1,F.lit(1)).otherwise(F.lit(0)))

# Final condition to use
filtered_dates = holiday_sunday_dates.crossJoin(stores)\
                  .join(saturday_off_stores,on='location_num',how='left')\
                  .withColumn('saturday_off',F.coalesce(F.col('saturday_off'),F.lit(0)))\
                  .where("""sunday = 0 and holiday = 0 and (saturday_off = 0 or dayofweek != 7)""")\
                  .select('location_num','prediction_date')

informs_spark_2 = informs_spark_2.join(
                                        filtered_dates,
                                        on=['location_num','prediction_date'],
                                        how='right'
                                      )\
                                 .orderBy('location_num','prediction_date')
informs_spark_2.persist()
print("Filtered informs_spark_2:-",informs_spark_2.count())

################ Part 1 :- Calculating location_num,prediction_date,business_date ################

COND = f"""
location_num = __store_num__
ORDER BY business_date desc
limit {PARAMS[ENV]['step_1_data_fetch_days_limit']}
"""

# query = f"""(Select *
#             from {PARAMS[ENV]['netsales_daily_table']}) a
#             inner join 
#             (Select distinct business_date as biz_date, location_num as store
#             from {PARAMS[ENV]['itemcount_table']}
#             where product = '160001') b
#             on a.location_num = b.store 
#             and a.business_date = b.biz_date"""
            

pred_date = max(PARAMS[ENV]['prediction_dates'])
predicates = [COND.replace('__store_num__',str(store))
              for store in PARAMS[ENV]['store_list']
             ]

# for x in predicates:
#     print(x)


df = spark.read.jdbc(
    secret["mysql-url"],
    PARAMS[ENV]['netsales_daily_table'],
    predicates=predicates,
    properties={ 'user' : secret["username"], 'password' : secret["password"] }
)

dedup_window = Window.partitionBy('location_num','business_date')
df = df.withColumn('max_gen_date',F.max('generation_date').over(dedup_window))\
      .where('generation_date = max_gen_date')\
      .withColumn('dayofweek',F.dayofweek('business_date'))\
      .select('location_num','business_date','sales_sub_total','dayofweek')\
      .distinct()

df.persist()

print("Part 1 Fetched data:- ",df.count(),"\nStores:-",df.select('location_num').distinct().count())
print("Part 1 data", df.show())

COND_ITEM = f"""
location_num = __store_num__
AND product IN ('160001', '403614','160067')
ORDER BY business_date desc
limit {PARAMS[ENV]['step_1_data_fetch_days_limit']}
"""

predicates = [COND_ITEM.replace('__store_num__',str(store))
              for store in PARAMS[ENV]['store_list']
              ]
   
df_item = spark.read.jdbc(
    secret["mysql-url"],
    PARAMS[ENV]['itemcount_table'],
    predicates=predicates,
    properties={ 'user' : secret["username"], 'password' : secret["password"] }
)

df_item = df_item.select('location_num','business_date').distinct()
df_item.persist()
df_item.show()

df = df.join(df_item,on=['location_num','business_date'],how='inner')
df.persist()

final_data = df.join(informs_spark_2,on=['location_num','dayofweek'],how='right')\
              .select(
                  F.col('location_num').cast('int'),'prediction_date','business_date',
                  'dayofweek','lookup_weeks','sales_sub_total'
              )\

rank_window = Window.partitionBy('location_num','prediction_date','dayofweek')\
                    .orderBy(F.col('business_date').desc())
final_data = final_data.withColumn('rank_col',F.rank().over(rank_window))\
                      .orderBy('location_num',F.col('business_date').desc())\
                      .where('rank_col<lookup_weeks')
final_data.persist()
print("Part 1 Filtered x-1 datapoints data:- ",final_data.count())

min_max_window = Window.partitionBy('location_num','prediction_date')
output1 = final_data.withColumn('min_sales',F.min('sales_sub_total').over(min_max_window))\
          .withColumn('max_sales',F.max('sales_sub_total').over(min_max_window))\
          .withColumn('sales',F.collect_list('sales_sub_total').over(min_max_window))\
          .withColumn('dates',F.collect_list('business_date').over(min_max_window))\
          .orderBy('location_num','dayofweek')\
          .filter('sales_sub_total != min_sales and sales_sub_total != max_sales')\
          .select('location_num','prediction_date','business_date','lookup_weeks')\
          .withColumn('forecast_avg_dr',F.count('business_date').over(min_max_window))\
          .withColumn('generation_date',F.lit(str(datetime.now(EST).date())))
output1.persist()
print("Part 1 Results data:- ",output1.count())

result_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['baseline_dates_path']}"

output1.orderBy('location_num','prediction_date','business_date').repartition(1)\
      .write\
      .format('csv')\
      .option("header",True)\
      .mode('overwrite')\
      .save(result_path)

t2 = time.time()
print("Time of execution:- ",t2-t1)
print("Part 1 done")

######################################### Part 1 done #########################################


################### Part 2:- Calculating location_num,prediction_date,trend ###################

informs_spark = stores.join(informs,on=['location_num'],how='left')\
                  .withColumn('lookup_weeks',F.coalesce(
                            F.col('lookup_weeks'),
                            F.lit(PARAMS[ENV]['default_lookup_weeks'])
                  ))\
                  .withColumn('smoothing_fact',F.coalesce(
                            F.col('smoothing_fact'),
                            F.lit(PARAMS[ENV]['default_smoothing_factor'])
                  ))


informs_2weeks = informs_spark.crossJoin(pred_dates_2weeks)
informs_3weeks = informs_spark.crossJoin(pred_dates_3weeks)
informs_spark_2 = informs_2weeks.union(informs_3weeks)\
      .withColumn('ly_fiscal_date', F.date_add('prediction_date',-1*PARAMS[ENV]['last_year_dates_lag']))\
      .withColumn('ly_min_date', F.expr('date_add(ly_fiscal_date,-lookup_weeks*7)'))\
      .withColumn('dayofweek', F.dayofweek('prediction_date'))


# Separating stores with zero and non-zero seasonality and
# Calculating trend only for stores with non-zero seasonality
informs_spark0 = informs_spark_2.where('smoothing_fact = 0')\
                              .select('location_num','prediction_date','smoothing_fact')\
                              .withColumn('trend',F.lit(0))
informs_spark1 = informs_spark_2.where('smoothing_fact != 0')

dates = informs_spark1.groupBy('location_num')\
                      .agg(
                          F.min('ly_min_date').alias('min_date'),
                          F.max('ly_fiscal_date').alias('max_date')
                        )\
                      .orderBy('location_num').toPandas()

COND = """
location_num = __store_num__ and
business_date <= "__max_date__" and business_date >= "__min_date__"
"""
pred_date = min(PARAMS[ENV]['prediction_dates'])
predicates = [COND.replace('__store_num__',str(store))\
                  .replace('__min_date__',str(min_date))\
                  .replace('__max_date__',str(max_date))
              for store,min_date,max_date in dates.values
             ]
for x in predicates:
    print(x)

df = spark.read.jdbc(
    secret["mysql-url"],
    PARAMS[ENV]['netsales_daily_table'],
    predicates=predicates,
    properties={ 'user' : secret["username"], 'password' : secret["password"] }
)
dedup_window = Window.partitionBy('location_num','business_date')
df = df.withColumn('max_gen_date',F.max('generation_date').over(dedup_window))\
      .where('generation_date = max_gen_date')\
      .withColumn('dayofweek',F.dayofweek('business_date'))\
      .select('location_num','business_date','sales_sub_total')\
      .distinct()
df.persist()
print("Part 2 Fetched data:- ",df.count())

prediction_data = informs_spark1
final_data = df.alias('df')\
              .join(
                    prediction_data.alias('pd'),
                    on=F.expr("""
df.location_num = pd.location_num and
df.business_date < pd.ly_fiscal_date and
df.business_date >= pd.ly_min_date and
dayofweek(df.business_date)=pd.dayofweek"""),
                    how='right')\
              .select(
                    F.col('pd.location_num').alias('location_num'),'prediction_date',
                    'business_date','dayofweek','lookup_weeks','smoothing_fact',
                    'sales_sub_total','ly_fiscal_date')\
              .withColumn('sales_sub_total',F.coalesce(F.col('sales_sub_total'),F.lit(0)))

final_data = final_data.groupBy('location_num','prediction_date','ly_fiscal_date',
                                'lookup_weeks','smoothing_fact')\
                      .agg(
                          F.sum('sales_sub_total').alias('ly_sales_sum'),
                          F.collect_list('sales_sub_total').alias('ly_values')
                        )\
                      .withColumn('ly_sales_avg',F.col('ly_sales_sum')/F.col('lookup_weeks'))\
                      .orderBy('location_num','prediction_date')
final_data.persist()
print("Part 2 Final data:- ",final_data.count())

output2 = final_data.alias('fd')\
          .join(
              df.alias('df'),
              F.expr('fd.location_num = df.location_num and fd.ly_fiscal_date = df.business_date'),
              how='left')\
          .withColumnRenamed('sales_sub_total','ly_sales')\
          .withColumn('ly_sales',F.coalesce(F.col('ly_sales'),F.lit(0)))\
          .select(F.col('fd.location_num').alias('location_num'),'prediction_date',
                        'ly_sales_avg','ly_sales','smoothing_fact','ly_values')\
          .withColumn('trend',F.when(
                                F.col('ly_sales_avg')!=0,
                                (F.col('ly_sales')-F.col('ly_sales_avg'))/F.col('ly_sales_avg')
                              ).otherwise(0))\
          .drop('ly_sales_avg','ly_sales','ly_values')

# Adding stores with 0 seasonality with trend as 0
output2 = output2.union(informs_spark0).orderBy('location_num','prediction_date')\
                 .withColumn('generation_date',F.lit(str(datetime.now(EST).date())))
output2.persist()
print("Part 2 Results data:- ",output2.count())

result_path = f"s3://{PARAMS[ENV]['bucket']}/{PARAMS[ENV]['baseline_trend_path']}"

output2.repartition(1)\
      .write\
      .format('csv')\
      .option("header",True)\
      .mode('overwrite')\
      .save(result_path)

t3 = time.time()
print("Time of execution:- ",t3-t2)
print("Part 2 done")

####################################### Part 2 done #######################################

print()
print("Total time of execution:- ",t3-t1)