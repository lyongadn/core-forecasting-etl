select features.aroundthanksgiving,
            features.aroundchristmas ,
            features.onedaypriorchristmas_and_new_year   ,
            features.federalholiday,
            features.holiday ,
            features.blackfridaycheck,
            features.business_date,
            features.dayofweek   ,
            features.sunday,
            features.location_num,
            coalesce(Actuals.sales_sub_total,0)sales_sub_total,
            coalesce(Actuals.trans_count,0)trans_count,
            features.federalholiday_name,(select max(dataprep_generation_date) from store_wise_qc.dataprep_dollarsalesandtranscount)dataprep_generation_date from (select *
            from
            ml_preprod.final_dollarsalesandtranscount_daily
            where location_num ='store_number' and business_date  >date_add(('MaxDate'),interval - 'actual_days' day)
            and business_date  <=('MaxDate')
              
                                                
            )Actuals right join (select *,'store_number' location_num from ml_preprod.inferencedates_daily
    where business_date>date_add('MaxDate', interval - 'actual_days' day) and business_date<=date_add(curdate(), interval -2 day)
    )features
    on Actuals.location_num=features.location_num and Actuals.business_Date=features.business_Date
                                              
                                                
     union
            select
            aroundthanksgiving,aroundchristmas  ,onedaypriorchristmas_and_new_year, federalholiday, holiday,blackfridaycheck,
            business_date,dayofweek,CASE
               WHEN location_num  IN (SELECT * FROM ml_preprod.saturday_off_stores)
                   AND dayofweek IN (6,7) THEN
               1
               ELSE sunday
               END AS sunday,location_num,sales_sub_total,  trans_count,federalholiday_name
               ,(select max(dataprep_generation_date) from store_wise_qc.dataprep_dollarsalesandtranscount)dataprep_generation_date
            from (select * from (select distinct location_num from ml_preprod.final_dollarsalesandtranscount_daily where location_num = 'store_number')a cross join ml_preprod.inferencedates_daily)A
            where  business_date >date_add(curdate(), interval -2 day) and business_date <=date_add(date_add(curdate(),interval -2 day),interval 'numberOfFeatures' day)
            order by business_date
     ;
