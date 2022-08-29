SELECT
  initialdollars.generation_date dataprep_generation_date,
  business_date_max,
  sum_daily_quantity_daily,
  sum_daily_quantity_daily_qc,
  sum_daily_quantity_15min,
  sum_daily_quantity_15min_qc,
  location_num,
  CURDATE() generation_date,
  CASE 
    WHEN sum_daily_quantity_daily <> sum_daily_quantity_15min_qc
      OR sum_daily_quantity_15min <> sum_daily_quantity_daily_qc 
    THEN 0 
    ELSE 1 
  END AS dailyqc
FROM
  (
    SELECT
      MAX(business_Date) business_date_max,
      CAST(SUM(sum_daily_quantity) AS DECIMAL (18, 2)) sum_daily_quantity_daily,
      generation_date,
      location_num
    FROM
      ml_preprod.initial_itemlevelcount_daily
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_itemlevelcount_daily
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) initialdollars
  CROSS JOIN (
    SELECT
      CAST(SUM(sum_daily_quantity) AS DECIMAL (18, 2)) sum_daily_quantity_daily_qc,
      generation_date
    FROM
      store_wise_qc.dataprep_initial_itemlevelcount_daily
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          store_wise_qc.dataprep_initial_itemlevelcount_daily
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) qcinitialdaily
  CROSS JOIN (
    SELECT
      CAST(SUM(sum_daily_quantity) AS DECIMAL (18, 2)) sum_daily_quantity_15min,
      generation_date
    FROM
      ml_preprod.initial_itemlevelcount_15min
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          ml_preprod.initial_itemlevelcount_15min
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) initialdollars15min
  CROSS JOIN (
    SELECT
      CAST(SUM(sum_daily_quantity) AS DECIMAL (18, 2)) sum_daily_quantity_15min_qc,
      generation_date
    FROM
      store_wise_qc.dataprep_initial_itemlevelcount_15min
    WHERE
      generation_date = (
        SELECT
          MAX(generation_date)
        FROM
          store_wise_qc.dataprep_initial_itemlevelcount_15min
        WHERE
          location_num = __location_num__
      )
      AND location_num = __location_num__
  ) qcinitial15min
