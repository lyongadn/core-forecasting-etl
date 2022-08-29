SELECT CASE 
    WHEN LENGTH(location_num)=1 THEN concat('0000',location_num) 
    WHEN LENGTH(location_num)=2 THEN concat('000',location_num) 
    WHEN LENGTH(location_num)=3 THEN concat('00',location_num) 
    WHEN LENGTH(location_num)=4 THEN concat('0',location_num) 
    ELSE location_num END AS location_num,
    forecast_type
FROM ml_preprod.lstm_baseline_location_list
ORDER BY forecast_type DESC, location_num ASC