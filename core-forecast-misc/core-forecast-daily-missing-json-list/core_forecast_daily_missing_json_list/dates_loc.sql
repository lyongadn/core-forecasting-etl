SELECT 
	CASE 
	WHEN length(location_num)=1 THEN concat('0000',location_num) 
	WHEN length(location_num)=2 THEN concat('000',location_num) 
    WHEN length(location_num)=3 THEN concat('00',location_num) 
	WHEN length(location_num)=4 THEN concat('0',location_num) 
	ELSE location_num END AS store_number
FROM
(	SELECT location_num, COUNT(weekday(business_date)) as CNT
	FROM ml_preprod.initial_dollarsalesandtranscount_daily
	WHERE DATE(business_date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -((7 * 8) + 2) DAY)
	AND weekday(business_date) = weekday('__business_date__')
	GROUP BY location_num, business_date) sums
GROUP BY location_num
HAVING SUM(CNT) >= 3