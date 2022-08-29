-- this query generates dollars and transcount forecast at daily level. This query runs for a location and date wise.
SELECT 
           CASE 
                      WHEN Length (b.location_num )=1 THEN Concat('0000',b.location_num ) 
                      WHEN Length (b.location_num )=2 THEN Concat('000',b.location_num ) 
                      WHEN Length (b.location_num )=3 THEN Concat('00',b.location_num ) 
                      WHEN Length (b.location_num )=4 THEN Concat('0',b.location_num ) 
                      ELSE b.location_num 
           end AS storenumber, 
                      Concat('%s' ,'T00:00:00') businessday, 
           'true'                                          readonly , 
           'Net Sales'                               ForecastBasis,
           Cast(Coalesce(a.forecast ,0)AS        DECIMAL(18,2))   sales, 
           Cast(Coalesce(a.forecast_tx ,0)AS     DECIMAL(18,2))   transactioncount, 
           Cast(Coalesce(b.sales_sub_total ,0)AS DECIMAL(18,2))   lastyearsales, 
           Cast(Coalesce(b.trans_count ,0)AS     DECIMAL(18,2))   lastyeartransactioncount 
FROM       ( 
                    SELECT   business_date, 
                             Sum(forecast)   forecast, 
                             Sum(forecast_tx)forecast_tx, 
                             location_num 
                    FROM     database.15min_dollar_table a 
                    WHERE    location_num ='store_number' 
                    AND      business_date='%s' 
                    AND      generation_date = 
                             ( 
                                    SELECT max(generation_date) 
                                    FROM   database.15min_dollar_table 
                                    WHERE  location_num ='store_number' 
                                    AND    business_date='%s' ) 
                    GROUP BY business_date, 
                             location_num )a 
RIGHT JOIN 
           ( 
                  SELECT * 
                  FROM   ( 
                                SELECT location_num, 
                                       sales_sub_total, 
                                       business_date, 
                                       trans_count, 
                                      'date_last_year' AS date_last 
                                FROM   ml_preprod.final_table 
                                WHERE  location_num ='store_number' )z 
                  WHERE  business_date = -- date_add('year',-1,current_Date) 
                         date_last ) b 
ON         a.location_num =b.location_num 
UNION 
SELECT 
          CASE 
                    WHEN length (a.location_num )=1 THEN concat('0000',a.location_num ) 
                    WHEN length (a.location_num )=2 THEN concat('000',a.location_num ) 
                    WHEN length (a.location_num )=3 THEN concat('00',a.location_num ) 
                    WHEN length (a.location_num )=4 THEN concat('0',a.location_num ) 
                    ELSE a.location_num 
          end                                            AS storenumber, 
                    concat( a.business_date,'T00:00:00')    businessday , 
          'true'                                            readonly ,
          'Net Sales'                               ForecastBasis,
          cast(coalesce(a.forecast ,0)AS        DECIMAL(18,2))     sales, 
          cast(coalesce(a.forecast_tx ,0)AS     DECIMAL(18,2))     transactioncount, 
          cast(coalesce(b.sales_sub_total ,0)AS DECIMAL(18,2))     lastyearsales, 
          cast(coalesce(b.trans_count ,0)AS     DECIMAL(18,2))     lastyeartransactioncount 
FROM      ( 
                   SELECT   Distinct business_date, 
                            sum(forecast)   forecast, 
                            sum(forecast_tx)forecast_tx , 
                            location_num 
                   FROM     database.15min_dollar_table a 
                   WHERE    location_num ='store_number' 
                   AND      business_date='%s' 
                   AND      generation_date = 
                            ( 
                                   SELECT max(generation_date) 
                                   FROM   database.15min_dollar_table 
                                   WHERE  location_num ='store_number' 
                                   AND    business_date='%s' ) 
                   GROUP BY business_date, 
                            location_num )a 
LEFT JOIN 
          ( 
                 SELECT * 
                 FROM   ( 
                               SELECT Distinct location_num, 
                                      sales_sub_total, 
                                      business_date, 
                                      trans_count, 
                                      'date_last_year' AS date_last 
                               FROM   ml_preprod.final_table 
                               WHERE  location_num ='store_number' )z 
                 WHERE  business_date = -- date_add('year',-1,current_Date) 
                        date_last ) b 
ON        a.location_num =b.location_num;
