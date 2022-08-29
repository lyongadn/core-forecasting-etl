import sys
import time
from datetime import datetime,date,timedelta
import pandas as pd
import pytz
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
print("Starting Execution")
t1 = time.time()
sc = SparkContext(appName='HS_15min')
spark = SparkSession(sc)
EST = pytz.timezone('America/New_York')


PARAMS = {
    "test": {
        "store_list_filepath": "s3://test-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"test-q-forecasting-artifacts",
        "lookup_ingredient_path": "s3://test-q-forecasting-artifacts/baseline/lookup_ingredients/lookup_ingredients.csv",
        "ycf_path": "s3://test-q-forecasting-artifacts/baseline/ycf/",
        "itemlevel_forecast_path": "s3://test-q-forecasting-artifacts/baseline/forecast/itemcount/",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(21,25)]
        },
        "forecast_file_name": "Forecast_File.csv",
        "ingredient_result_path": "s3://test-q-forecasting-artifacts/baseline/forecast/ingredient/"
    },
    "prod": {
        "store_list_filepath": "s3://prod-q-forecasting-artifacts/baseline/location_list/LSTM_Baseline_List.csv",
        "bucket":"prod-q-forecasting-artifacts",
        "lookup_ingredient_path": "s3://prod-q-forecasting-artifacts/baseline/lookup_ingredients/lookup_ingredients.csv",
        "ycf_path": "s3://prod-q-forecasting-artifacts/baseline/ycf/ycf.csv",
        "itemlevel_forecast_path": "s3://prod-q-forecasting-artifacts/baseline/forecast/itemcount/",
        "prediction_dates": {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(21,25)]
        },
        "forecast_file_name": "Forecast_File.csv",
        "ingredient_result_path": "s3://prod-q-forecasting-artifacts/baseline/forecast/ingredient/"
    }
}

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])

ENV = args['ENV']

PARAMS[ENV]['store_list'] = pd.read_csv(PARAMS[ENV]['store_list_filepath'])['location_num'].tolist()

dates_2weeks = PARAMS[ENV]['prediction_dates']['2weeks_ahead']
dates_3weeks = PARAMS[ENV]['prediction_dates']['3weeks_ahead']

# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)


print("Job execution starting...")
print("Reading lookup_ingredient from S3.")
lookup_ingr = spark.read.csv(PARAMS[ENV]['lookup_ingredient_path'],header=True)\
                  .select('pin','ingredient_quantity','ingredient_id','unit_of_measure')
lookup_ingr.persist()
print("Number of rows in lookup_ingr:- ",lookup_ingr.count())

print("Reading ycf from S3.")
ycf = spark.read.csv(PARAMS[ENV]['ycf_path'],header=True)
ycf_agg = ycf.groupBy('location_num','ingredient_id')\
             .agg(F.avg('ycf').alias('ycf'))\
             .withColumn('ycf',F.coalesce('ycf',F.lit(1)))
ycf_agg.persist()
print("Number of rows in ycf_agg:- ",ycf_agg.count())

# join = lookup_ingr.join(ycf_agg,on='ingredient_id',how='left')\
#                   .withColumnRenamed('pin','product')

# item_forecast = spark.read.csv(PARAMS[ENV]['itemlevel_forecast_path']+"/forecast",header=True)
item_forecast_2weeks = spark.read.csv(PARAMS[ENV]['itemlevel_forecast_path']+"/2weeks_ahead",header=True)
item_forecast_3weeks = spark.read.csv(PARAMS[ENV]['itemlevel_forecast_path']+"/3weeks_ahead",header=True)
item_forecast = item_forecast_2weeks.union(item_forecast_3weeks)
item_forecast.persist()
print("Reading the itemcount forecast. RowCount:- ",item_forecast.count())
print("Data read complete...")

# ingr_data = item_forecast.join(join,on=['location_num','product'],how='inner')

ingr_data = item_forecast.join(
                              lookup_ingr,
                              on=item_forecast.product==lookup_ingr.pin,
                              how='inner')\
                         .join(ycf_agg,on=['location_num','ingredient_id'],how='left')\
                         .drop('pin')

ingr_forecast = ingr_data.groupBy('location_num','ingredient_id','business_date',
                             'interval','unit_of_measure')\
                    .agg(F.round(F.sum(
                            F.col('forecast_sum_daily_quantity') * \
                            F.coalesce('ycf',F.lit(1)) * \
                            F.col('ingredient_quantity')
                        ),2).alias('sumingredient'))\
                    .withColumn('store_num',F.col('location_num'))\
                      .withColumn('generation_date',F.lit(str(datetime.now(EST).date())))

ingr_forecast = ingr_forecast.select('business_date','ingredient_id','sumingredient','unit_of_measure','interval','generation_date','location_num','store_num')
ingr_forecast.persist()
print("Ingredient forecast generated. RowCount:- ",ingr_forecast.count())

result_path = PARAMS[ENV]['ingredient_result_path']

MODE = "overwrite"
ingr_forecast_2weeks = ingr_forecast.where(F.col('business_date').isin(dates_2weeks))
ingr_forecast_3weeks = ingr_forecast.where(F.col('business_date').isin(dates_3weeks))
ingr_forecast_2weeks.orderBy('location_num','ingredient_id','business_date','interval')\
       .repartition('location_num')\
       .write.partitionBy('store_num')\
       .format('csv')\
       .option("header",True)\
       .mode(MODE)\
       .save(result_path+"2weeks_ahead/")

time.sleep(30)

ingr_forecast_3weeks.orderBy('location_num','ingredient_id','business_date','interval')\
       .repartition('location_num')\
       .write.partitionBy('store_num')\
       .format('csv')\
       .option("header",True)\
       .mode(MODE)\
       .save(result_path+"3weeks_ahead/")

print("Ingredient forecast successfully stored in S3.")

t3 = time.time()
print("Total time of execution:- ",t3-t1)