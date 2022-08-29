-- this query gives dollars and transcount (Metric forecast) at 15mins level for one location and a date.
SELECT   Distinct intervalstart , 
         servicetype   , 
         sales         , 
         transactioncount, 
         lastyearsales,
         lastyeartransactioncount
FROM     ( 
                    SELECT     Concat(Concat('%s', 'T' ), b.timequarter ) AS intervalstart,
                               -- substring(datetime,12,19),datetime, 
                               '3'                                             AS servicetype, 
                               Cast(Coalesce(a.forecast, 0) AS        DECIMAL(18, 2))    sales, 
                               Cast(Coalesce(a.forecast_tx, 0) AS       DECIMAL(18, 2))    transactioncount,
                               Cast(Coalesce(b.sales_sub_total, 0) AS DECIMAL(18, 2))    lastyearsales,
                               Cast(Coalesce(b.trans_count, 0) AS     DECIMAL(18, 2))    lastyeartransactioncount,
                               case when 
                               a.forecast in (0,NULL) and 
                               a.forecast_tx in (0,NULL) and 
                               b.sales_sub_total in (0,NULL) and 
                               b.trans_count in (0,NULL) 
                               then 0 else 1 end as removezeros
                    FROM       ( 
                                      SELECT Distinct *, 
                                             interval_start_time timequarter 
                                      FROM   database.15min_dollar_table a 
                                      WHERE  location_num = 'store_number' 
                                      AND    business_date = '%s' 
                                      AND    generation_date = 
                                             ( 
                                                    SELECT max(generation_date) 
                                                    FROM   database.15min_dollar_table 
                                                    WHERE  location_num = 'store_number' 
                                                    AND    business_date = '%s' ) ) a
                    RIGHT JOIN 
                               ( 
                                      SELECT * 
                                      FROM   ( 
                                                    SELECT Distinct location_num, 
                                                           sales_sub_total, 
                                                           business_date, 
                                                           substring(timequarter, 12, 19) timequarter,
                                                           trans_count, 
                                                           'date_last_year' AS date_last 
                                                    FROM   ml_preprod.final_table_15min 
                                                    WHERE  location_num = 'store_number' ) final 
                                      WHERE  business_date = date_last ) b 
                    ON         a.location_num = 'store_number' -- and cast(a.business_Date as date)=cast(b.business_Date  as date) 
                    AND        cast(a.timequarter AS CHAR(10)) = cast(b.timequarter AS CHAR(10)) 
                    UNION 
                    SELECT     concat( concat( substring(cast(a.business_date AS CHAR(10)), 1, 10), 'T' ), a.timequarter ) AS intervalstart,
                               -- substring(datetime,12,19),datetime, 
                               '3'                                             AS servicetype, 
                               cast(coalesce(a.forecast, 0) AS        DECIMAL(18, 2))    sales, 
                               cast(coalesce(a.forecast_tx, 0) AS       DECIMAL(18, 2))    transactioncount,
                               cast(coalesce(b.sales_sub_total, 0) AS DECIMAL(18, 2))    lastyearsales,
                               cast(coalesce(b.trans_count, 0) AS     DECIMAL(18, 2))    lastyeartransactioncount,
                               case when 
                               a.forecast in (0,NULL) and 
                               a.forecast_tx in (0,NULL) and 
                               b.sales_sub_total in (0,NULL) and 
                               b.trans_count in (0,NULL) 
                               then 0 else 1 end as removezeros
                    FROM       ( 
                                      SELECT Distinct *, 
                                             interval_start_time timequarter 
                                      FROM   database.15min_dollar_table a 
                                      WHERE  location_num = 'store_number' 
                                      AND    business_date = '%s' 
                                      AND    generation_date = 
                                             ( 
                                                    SELECT max(generation_date) 
                                                    FROM   database.15min_dollar_table 
                                                    WHERE  location_num = 'store_number' 
                                                    AND    business_date = '%s' ) ) a
                    LEFT JOIN 
                               ( 
                                      SELECT * 
                                      FROM   ( 
                                                    SELECT Distinct location_num, 
                                                           sales_sub_total, 
                                                           business_date, 
                                                           substring(timequarter, 12, 19) timequarter,
                                                           trans_count, 
                                                           'date_last_year' AS date_last 
                                                    FROM   ml_preprod.final_table_15min 
                                                    WHERE  location_num = 'store_number' ) final 
                                      WHERE  business_date = date_last ) b 
                    ON         a.location_num = 'store_number' -- and cast(a.business_Date as date)=cast(b.business_Date  as date) 
                    AND        cast(a.timequarter AS CHAR(10)) = cast(b.timequarter AS CHAR(10)) ) a where removezeros = 1
union all                                                                
select 
         intervalstart , 
         servicetype   , 
         sales         , 
         transactioncount, 
         lastyearsales,
         lastyeartransactioncount from (select concat( concat( substring(cast('%s' AS CHAR(10)), 1, 10), 'T' ), (substring(a.timequarter, 12, 19) ) ) AS intervalstart,2
servicetype, 0 sales, 0 transactioncount,  coalesce(b.sales_sub_total,0)lastyearsales, 
 coalesce(b.trans_count,0)lastyeartransactioncount,
 case when b.sales_sub_total = 0 and b.trans_count = 0 then 0 else 1 end as removezeros
 from (select * from ml_preprod.inferencedates_15min where business_date =  'date_last_year')a inner join
(select *
from ml_preprod.initial_deffered_salesandtranscount_15min where location_num ='store_number' and business_Date =  'date_last_year' )b
on a.timequarter = b.timequarter ) removezero where removezeros = 1
order by  servicetype,intervalstart asc