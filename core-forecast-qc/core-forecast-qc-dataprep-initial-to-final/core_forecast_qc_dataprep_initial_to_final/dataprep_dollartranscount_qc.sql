/*
This query generates the qc flag store wise
*/
SELECT
  a.dataprep_generation_date,
  business_date_max,
  dollardaily_sum_initial,
  transdaily_sum_initial,
  dollardaily_sum_final,
  transdaily_sum_final,
  dollar15min_sum_initial,
  trans15min_sum_initial,
  dollar15min_sum_final,
  trans15min_sum_final,
  __location_num__ location_num,
  CURDATE() generation_date,
  CASE 
    WHEN 
         dollardaily_sum_initial <> dollardaily_sum_final
      OR transdaily_sum_initial <> transdaily_sum_final
      OR dollar15min_sum_initial <> dollar15min_sum_final
      OR trans15min_sum_initial <> trans15min_sum_final 
    THEN 0 
    ELSE 1 END AS dailyqc
FROM
  (
    SELECT
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) dollardaily_sum_initial,
      SUM(trans_count) transdaily_sum_initial,
      generation_date dataprep_generation_date
    FROM
      ml_preprod.initial_dollarsalesandtranscount_daily
    WHERE
      location_num = __location_num__
      AND generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_dollarsalesandtranscount_daily
        WHERE
          location_num = __location_num__
      )
    GROUP BY
      generation_date
  ) a
  CROSS JOIN (
    SELECT 
	  max(business_date)business_date_max,
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) dollardaily_sum_final,
      SUM(trans_count) transdaily_sum_final
    FROM
      ml_preprod.final_dollarsalesandtranscount_daily
    WHERE
      location_num = __location_num__
      AND generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.final_dollarsalesandtranscount_daily
        WHERE
          location_num = __location_num__
      )
    GROUP BY
      generation_date
  ) b
  CROSS JOIN (
    SELECT
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) dollar15min_sum_initial,
      SUM(trans_count) trans15min_sum_initial
    FROM
      ml_preprod.initial_dollarsalesandtranscount_15min
    WHERE
      location_num = __location_num__
      AND generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_dollarsalesandtranscount_daily
        WHERE
          location_num = __location_num__
      )
    GROUP BY
      generation_date
  ) c
  CROSS JOIN (
    SELECT
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) dollar15min_sum_final,
      SUM(trans_count) trans15min_sum_final
    FROM
      ml_preprod.final_dollarsalesandtranscount_15min
    WHERE
      location_num = __location_num__
      AND generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.final_dollarsalesandtranscount_daily
        WHERE
          location_num = __location_num__
      )
    GROUP BY
      generation_date
  ) d
