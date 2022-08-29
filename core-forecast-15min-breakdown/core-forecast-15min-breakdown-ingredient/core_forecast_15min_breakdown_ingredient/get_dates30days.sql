SELECT DISTINCT business_date
FROM ui_preprod.daily_itemcount_lstm_30days_ahead a
WHERE a.location_num = '__store_number__'
        AND item_id = '160001'
        AND generation_date =
    (SELECT max(generation_date)
    FROM ui_preprod.daily_itemcount_lstm_30days_ahead
    WHERE location_num = '__store_number__'
            AND item_id = '160001' ) 
