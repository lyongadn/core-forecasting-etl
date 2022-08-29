-- this query does quality check on the forecast data and inserts into respective table passed via lambda script
SELECT gen_date_dollars30thday forecast_generation_date, 
       a.business_date,
       location_num, 
       Curdate()               generation_date-- , 
       , 
       CASE 
         WHEN (SELECT Min(business_date) 
                   FROM   __forecast_database__.__forecast_table__ 
                   WHERE  generation_date = (SELECT Max(generation_date) 
                                             FROM 
                          __forecast_database__.__forecast_table__ 
                                             WHERE  location_num = __store_num__) 
                          AND location_num = __store_num__) <> 
                  Date_add((select max(generation_date) from baseline.15min_dollarsales_lstm_14days_ahead where location_num =__store_num__), 
                  INTERVAL __forecast_from__ day) 
               OR (SELECT Max(business_date) 
                   FROM   __forecast_database__.__forecast_table__ 
                   WHERE  generation_date = (SELECT Max(generation_date) 
                                             FROM 
                          __forecast_database__.__forecast_table__ 
                                             WHERE  location_num = __store_num__) 
                          AND location_num = __store_num__) <> 
                  Date_add((select max(generation_date) from baseline.15min_dollarsales_lstm_14days_ahead where location_num =__store_num__), 
                  INTERVAL __forecast_to__ day) 
               OR no_of_rows_updated <> 96 
                  THEN 0 
         ELSE 1 
       end                     AS forecast_qc 
FROM   (SELECT location_num, 
               generation_date             gen_date_dollars30thday,
               Count(1)                    no_of_rows_updated, 
               business_date 
        FROM   __forecast_database__.__forecast_table__ a 
        WHERE  generation_date = (SELECT Max(generation_date) 
                                  FROM 
               __forecast_database__.__forecast_table__ 
                                  WHERE  location_num = __store_num__) 
               AND location_num = __store_num__ 
        GROUP  BY business_date) a 