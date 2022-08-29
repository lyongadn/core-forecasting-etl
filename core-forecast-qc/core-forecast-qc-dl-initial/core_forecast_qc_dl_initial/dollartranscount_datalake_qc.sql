-- this query generates the qc for dollarsales, transcount datalake and inserts the data in the dataprep initial datalake table
SELECT
  initialdollars.generation_date dataprep_generation_date,
  business_date_max,
  sales_total_daily sales_sub_total_daily,
  trans_count_daily,
  sales_total_qc sales_total_daily_qc,
  trans_count_qc trans_count_daily_qc,
  sales_total_daily15min sales_sub_total_15min,
  trans_count_daily15min trans_count_15min,
  sales_total_15minqc sales_total_15min_qc,
  trans_count_15minqc trans_count_15min_qc,
  location_num,
  CURDATE() generation_date,
  CASE 
    WHEN sales_total_daily <> sales_total_qc
      OR trans_count_qc <> trans_count_daily
      OR sales_total_daily15min <> sales_total_15minqc
      OR trans_count_daily15min <> trans_count_15minqc 
    THEN 0 
    ELSE 1 
  END AS dailyqc
FROM
  (
    SELECT
      MAX(business_date) business_date_max,
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) sales_total_daily,
      CAST(SUM(trans_count) AS DECIMAL (18, 2)) trans_count_daily,
      generation_date,
      location_num
    FROM
      ml_preprod.initial_dollarsalesandtranscount_daily
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_dollarsalesandtranscount_daily
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) initialdollars
  CROSS JOIN (
    SELECT
      SUM(CAST(sales_sub_total AS DECIMAL (18, 2))) sales_total_qc,
      SUM(CAST(trans_count AS DECIMAL (18, 2))) trans_count_qc,
      generation_date
    FROM
      store_wise_qc.dataprep_initial_dollarsandtranscount_daily
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          store_wise_qc.dataprep_initial_dollarsandtranscount_daily
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) qcinitialdaily
  CROSS JOIN (
    SELECT
      CAST(SUM(sales_sub_total) AS DECIMAL (18, 2)) sales_total_daily15min,
      CAST(SUM(trans_count) AS DECIMAL (18, 2)) trans_count_daily15min,
      generation_date
    FROM
      ml_preprod.initial_dollarsalesandtranscount_15min
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_dollarsalesandtranscount_15min
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) initialdollars15min
  CROSS JOIN (
    SELECT
      SUM(CAST(sales_sub_total AS DECIMAL (18, 2))) sales_total_15minqc,
      SUM(CAST(trans_count AS DECIMAL (18, 2))) trans_count_15minqc,
      generation_date
    FROM
      store_wise_qc.dataprep_initial_dollarsandtranscount_15min
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          store_wise_qc.dataprep_initial_dollarsandtranscount_15min
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) qcinitial15min
