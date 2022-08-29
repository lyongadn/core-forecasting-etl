-- this query does quality check on the forecast data and inserts into respective table passed via lambda script
SELECT forecast_generation_date, 
       business_date, 
       expected_items, 
       actual_items, 
       location_num, 
       Curdate() generation_date, 
       CASE 
         WHEN actual_items < expected_items 
               OR (SELECT Min(business_date) 
                   FROM   __forecast_database__.__forecast_table__ 
                   WHERE  generation_date = (SELECT Max(generation_date) 
                                             FROM 
                          __forecast_database__.__forecast_table__ 
                                             WHERE  location_num = __store_num__ 
                                            ) 
                          AND location_num = __store_num__) <> 
                  Date_add((select max(generation_date) from baseline.15min_dollarsales_lstm_14days_ahead where location_num =__store_num__), INTERVAL __forecast_from__ day) 
               OR (SELECT Max(business_date) 
                   FROM   __forecast_database__.__forecast_table__ 
                   WHERE  generation_date = (SELECT Max(generation_date) 
                                             FROM 
                          __forecast_database__.__forecast_table__ 
                                             WHERE  location_num = __store_num__ 
                                            ) 
                          AND location_num = __store_num__) <> 
                  Date_add((select max(generation_date) from baseline.15min_dollarsales_lstm_14days_ahead where location_num =__store_num__), INTERVAL __forecast_to__ day) THEN 0 
               when  actual_items * 96  <> countrows THEN 0 
         ELSE 1 
       end       AS forecast_qc 
FROM   (SELECT ( generation_date )                  forecast_generation_date, '__placeholder__' daily15min,
               business_date, 
               (SELECT distinct products
                FROM   baseline_qc.expected_forecasted_products 
                WHERE  location_num = __store_num__ and generation_date=(select max(generation_Date) from baseline_qc.expected_forecasted_products 
                WHERE  location_num = __store_num__))expected_items, 
               Count(DISTINCT item_id)              actual_items, 
               Count(*)                             countrows, 
               location_num 
        FROM   __forecast_database__.__forecast_table__ 
        WHERE  ( generation_date ) = (SELECT Max(generation_date) 
                                      FROM 
               __forecast_database__.__forecast_table__ 
                                      WHERE  location_num = __store_num__) 
               AND location_num = __store_num__ 
        GROUP  BY business_date)final; 