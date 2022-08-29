import sys
import logging
import boto3
#import rds_config
import pymysql
import time
import datetime
import os
import json
from boto3.s3.transfer import S3Transfer
#from sqlalchemy import create_engine
import pandas as pd
pymysql.install_as_MySQLdb()

# Connect to the database

def lambda_handler(event, context):
    connection = pymysql.connect(host=os.environ['replica_host_link'],
                                 user=os.environ['user_name'],
                                 password=os.environ['rds_password'],
                                 #db='ml_preprod'
                                 )
    print("Connection succeeded for RDS Test Instance!")

    with connection.cursor() as ForecastQC:

        dailydollarsaleslstm14daysahead='''
         select gen_date_dollars14thday forecast_generation_date,a.business_date,expected_locations,actual_locations,
         no_of_rows_updated,curdate() generation_date-- ,
         ,case when a.no_of_rows_updated<> c.no_of_rows_updated_trans14thday
           or actual_locations <expected_locations
           or location_trans<>actual_locations
           or (select min(business_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead)
        )
           <>date_add(curdate(), interval 16 day) or 
            (select max(business_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead)
        ) 
          <>date_add(curdate(), interval 19 day) 
           then 0 else 1
           end as forecast_qc
         from (
        select -- count(distinct business_date) no_of_days_forecasted_dollars_14thday ,
        generation_date gen_date_dollars14thday,count(distinct location_num)actual_locations
        ,(select  (actual_no_of_locations)   from data_forecast_qc.dataprep_dollarsalesandtranscount
         where generation_date=(select max(generation_date) from data_forecast_qc.dataprep_dollarsalesandtranscount))expected_locations
        ,count(1) no_of_rows_updated,business_date
         -- ,date_add('2018-06-29', interval +8 day)expected_forecast_date_dollars14thday
        from ui_preprod.daily_dollarsales_lstm_14days_ahead a
        where generation_date=(select max(generation_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead)
        group by business_date
        ) a
        inner join
         (
         select -- count(distinct business_date) no_of_days_forecasted_trans_14thday ,
         generation_date gen_date_trans14thday,
         count(distinct location_num)actual_location_trans14thday
        ,count(1) no_of_rows_updated_trans14thday,business_date,count(distinct location_num)location_trans
        --  ,date_add('2018-06-27', interval +8 day)expected_forecast_date_trans14thday
        from
        ui_preprod.daily_transcount_lstm_14days_ahead a
        where
        (generation_date)=(select max(generation_date) from ui_preprod.daily_dollarsales_lstm_14days_ahead)
        group by business_date
        ) c
         on
        (c.gen_date_trans14thday)=(a.gen_date_dollars14thday) and c.business_date=a.business_date;'''
        print dailydollarsaleslstm14daysahead
        print("Started loading")
        ForecastQC.execute(dailydollarsaleslstm14daysahead)
        dailydollarsaleslstm14daysahead = ForecastQC.fetchall()
        dailydollarsaleslstm14daysahead = pd.DataFrame(list(dailydollarsaleslstm14daysahead),columns = ['forecast_generation_date', 'business_date', 'expected_locations',' actual_locations', 'no_of_rows_updated', 'generation_date', 'forecast_qc'])
        dailydollarsaleslstm14daysahead.to_csv('/tmp/dailydollarsales14days.csv',header=False,index=False)
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
	target_dir = '/tmp/'
        transfer.upload_file('/tmp/dailydollarsales14days.csv','prod-q-forecasting-artifacts','Forecast/ForecastQC/dailydollarsales14days.csv')

        dollarsales15minlstm14days='''
        select
        gen_date_dollars14thday forecast_generation_date,a.bddollars business_date,expected_locations,actual_locations,
       no_of_rows_updated,curdate() generation_date
        ,case when a.no_of_rows_updated<> c.no_of_rows_updated_trans14thday
         or actual_locations < expected_locations
         or location_trans <> actual_locations
         or (select min(business_date) from ui_preprod.15min_dollarsales_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.15min_dollarsales_lstm_14days_ahead)
        )
         <>date_add(curdate(), interval 16 day) or 
         (select max(business_date) from ui_preprod.15min_dollarsales_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.15min_dollarsales_lstm_14days_ahead)
        )
         
         <>date_add(curdate(), interval 19 day) 
         then 0 else 1
         end as forecast_qc
        -- ,case when no_of_rows_updated_dollars30thday<> no_of_rows_updated_dollars30thday
         -- or no_of_rows_updated_trans30thday <>no_of_rows_updated_trans30thday then 0 else 1
        --  end as forecast_qc
        from (
       select (generation_date) gen_date_dollars14thday,(select  (actual_no_of_locations)   from data_forecast_qc.dataprep_dollarsalesandtranscount
        where generation_date=(select max(generation_date) from data_forecast_qc.dataprep_dollarsalesandtranscount))expected_locations
        ,count(distinct location_num)actual_locations
       ,count(1) no_of_rows_updated,business_date bddollars
        -- ,date_add('2018-06-29', interval +8 day)expected_forecast_date_dollars30thday
       from ui_preprod.15min_dollarsales_lstm_14days_ahead a
       where (generation_date)=(select max(generation_date) from ui_preprod.`15min_dollarsales_lstm_14days_ahead`)
       group by business_date,(generation_date)
       ) a
        inner join
        (
        select (generation_date) gen_date_trans14thday,count(distinct location_num)actual_location_trans14thday
       ,count(1) no_of_rows_updated_trans14thday,business_date bdtrans, count(distinct location_num)location_trans
       --  ,date_add('2018-06-27', interval +8 day)expected_forecast_date_trans14thday
       from
       ui_preprod.15min_transcount_lstm_14days_ahead a
       where
       (generation_date)=(select max(generation_date) from ui_preprod.`15min_dollarsales_lstm_14days_ahead`)
       group by business_date,(generation_date)
       ) c
        on
       (c.gen_date_trans14thday)=(a.gen_date_dollars14thday)  and c.bdtrans=a.bddollars;'''
        print dollarsales15minlstm14days
        print("Started loading")
        ForecastQC.execute(dollarsales15minlstm14days)
        dollarsales15minlstm14days = ForecastQC.fetchall()
        dollarsales15minlstm14days = pd.DataFrame(list(dollarsales15minlstm14days),columns = ['forecast_generation_date', 'business_date', 'expected_locations',' actual_locations', 'no_of_rows_updated', 'generation_date', 'forecast_qc'])
        dollarsales15minlstm14days.to_csv('/tmp/dollarsales15min14days.csv',header=False,index=False)
        s3 = boto3.client('s3')
	transfer = S3Transfer(s3)
	target_dir = '/tmp/'
        transfer.upload_file('/tmp/dollarsales15min14days.csv','prod-q-forecasting-artifacts','Forecast/ForecastQC/dollarsales15min14days.csv')


        forecastdailyitem14thday='''
        select *,curdate() generation_date,case when actual_items < expected_items
     or actual_locations<expected_locations
     or (select min(business_date) from ui_preprod.daily_itemcount_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.daily_itemcount_lstm_14days_ahead)
        ) 
     <>date_add(curdate(), interval 16 day) or  
     (select max(business_date) from ui_preprod.daily_itemcount_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.daily_itemcount_lstm_14days_ahead)
        )
     <>date_add(curdate(), interval 19 day) 
     then  0 else 1
       end as forecast_qc
      from (select  (generation_date)forecast_generation_date,business_date,
     (select  (actual_no_of_locations_daily_final)   from data_forecast_qc.dataprep_itemlevelcount
      where generation_date=(select max(generation_date) from data_forecast_qc.dataprep_itemlevelcount))expected_locations,
    (select  count(*) from ml_preprod.expected_forecasted_products)expected_items
     ,count(distinct item_id) actual_items
     ,count(distinct location_num) actual_locations
     -- date_add('2018-06-26', interval +8 day) expected_forecast_date_30thday,
     -- ( business_date) actual_forecast_date_30thday,
      from ui_preprod.daily_itemcount_lstm_14days_ahead
     where (generation_date) =(select max(generation_date) from ui_preprod.daily_itemcount_lstm_14days_ahead)
     group by business_date)final;'''
        print forecastdailyitem14thday
        print("Started loading")
        ForecastQC.execute(forecastdailyitem14thday)
        forecastdailyitem14thday = ForecastQC.fetchall()
        forecastdailyitem14thday = pd.DataFrame(list(forecastdailyitem14thday),columns = ['forecast_generation_date', 'business_date', 'expected_locations','expected_items','actual_items',' actual_locations', 'generation_date', 'forecast_qc'])
        forecastdailyitem14thday.to_csv('/tmp/forecastdailyitem14thday.csv',header=False,index=False)
        s3 = boto3.client('s3')
	transfer = S3Transfer(s3)
	target_dir = '/tmp/'
        transfer.upload_file('/tmp/forecastdailyitem14thday.csv','prod-q-forecasting-artifacts','Forecast/ForecastQC/forecastdailyitem14thday.csv')


        forecast15minitem14thday='''
        select *,curdate() generation_date,case when actual_locations < expected_locations
        or actual_items<expected_items
        or (select min(business_date) from ui_preprod.15min_itemcount_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.15min_itemcount_lstm_14days_ahead)
        ) 
        
        <>date_add(curdate(), interval 16 day) or  
        (select max(business_date) from ui_preprod.15min_itemcount_lstm_14days_ahead where generation_date=(select max(generation_date) from ui_preprod.15min_itemcount_lstm_14days_ahead)
        )  
        <>date_add(curdate(), interval 19 day) 
        then  0 else 1
          end as forecast_qc
         from (select  (generation_date)forecast_generation_date,business_date,
        (select  (actual_no_of_locations_daily_final)   from data_forecast_qc.dataprep_itemlevelcount
         where generation_date=(select max(generation_date) from data_forecast_qc.dataprep_itemlevelcount))expected_locations,
        (select  count(*) from ml_preprod.expected_forecasted_products)expected_items
        ,count(distinct item_id) actual_items
        ,count(distinct location_num) actual_locations
         from ui_preprod.15min_itemcount_lstm_14days_ahead
        where generation_date =(select max(generation_date) from ui_preprod.15min_itemcount_lstm_14days_ahead)
        group by business_date)final;'''
        print forecast15minitem14thday
        print("Started loading")
        ForecastQC.execute(forecast15minitem14thday)
        forecast15minitem14thday = ForecastQC.fetchall()
        forecast15minitem14thday = pd.DataFrame(list(forecast15minitem14thday),columns = ['forecast_generation_date', 'business_date', 'expected_locations','expected_items','actual_items',' actual_locations', 'generation_date', 'forecast_qc'])
        forecast15minitem14thday.to_csv('/tmp/forecast15minitem14thday.csv',header=False,index=False)
        s3 = boto3.client('s3')
	transfer = S3Transfer(s3)
	target_dir = '/tmp/'
        transfer.upload_file('/tmp/forecast15minitem14thday.csv','prod-q-forecasting-artifacts','Forecast/ForecastQC/forecast15minitem14thday.csv')
        connection.commit()
  

        connection_load = pymysql.connect(host=os.environ['host_link'],
                                            user=os.environ['user_name'],
                                            password=os.environ['rds_password'],
                                            db='ml_preprod'
                                            )
        
        with connection_load.cursor() as ForecastQC:
		
		    dollarsalesdaily14days = '''LOAD DATA FROM S3 prefix 
	        's3-us-east-1://prod-q-forecasting-artifacts/Forecast/ForecastQC/dailydollarsales14days.csv'
			INTO TABLE data_forecast_qc.forecast_daily_dollarsandtrans_14thday_ahead
			FIELDS TERMINATED BY ','
			ENCLOSED BY '"'
			LINES TERMINATED BY '\\n';'''
		    ForecastQC.execute(dollarsalesdaily14days)
			

		    dollarsales15min14days = '''LOAD DATA FROM S3 prefix 
			's3-us-east-1://prod-q-forecasting-artifacts/Forecast/ForecastQC/dollarsales15min14days.csv'
			INTO TABLE data_forecast_qc.forecast_15min_dollarsandtrans_14thday_ahead
			FIELDS TERMINATED BY ','
			ENCLOSED BY '"'
			LINES TERMINATED BY '\\n';'''
		    ForecastQC.execute(dollarsales15min14days)


		    itemcountdaily14days = '''LOAD DATA FROM S3 prefix 
	        's3-us-east-1://prod-q-forecasting-artifacts/Forecast/ForecastQC/forecastdailyitem14thday.csv'
			INTO TABLE data_forecast_qc.forecast_daily_item_14thday_ahead
			FIELDS TERMINATED BY ','
			ENCLOSED BY '"'
			LINES TERMINATED BY '\\n';'''
		    ForecastQC.execute(itemcountdaily14days)


		    itemcount15min14days = '''LOAD DATA FROM S3 prefix 
		    's3-us-east-1://prod-q-forecasting-artifacts/Forecast/ForecastQC/forecast15minitem14thday.csv'
			INTO TABLE data_forecast_qc.forecast_15min_item_14thday_ahead
			FIELDS TERMINATED BY ','
			ENCLOSED BY '"'
			LINES TERMINATED BY '\\n';'''
		    ForecastQC.execute(itemcount15min14days)
		    connection_load.commit()

        print("Process finished successfully!")
