-- this query generates the qc for itemlevelcount dataprep and inserts the data in the dataprep tables
select
  a.generation_date forecast_generation_date,
  business_date_max,
  sum_quantity_daily_initial,
  sum_quantity_daily_final,
  sum_quantity_15min_initial,
  sum_quantity_15min_final,
  __location_num__ location_num,
  curdate() generation_date,
  CASE 
    WHEN sum_quantity_daily_initial <> sum_quantity_daily_final
      OR sum_quantity_15min_initial <> coalesce(sum_quantity_15min_final, 0)
      OR sum_quantity_15min_initial <> sum_quantity_daily_initial
    THEN 0 
    ELSE 1 
  END as dailyqc
from
  (
    select
      cast(sum(sum_daily_quantity) as decimal(18, 2)) sum_quantity_daily_initial,
      generation_date
    from
      ml_preprod.initial_itemlevelcount_daily
    where
      location_num = __location_num__
      and generation_date =(
        select
          max(generation_date)
        from
          ml_preprod.initial_itemlevelcount_daily
        where
          location_num = __location_num__
      )
    group by
      generation_date
  ) a
  cross join (
    select
      cast(sum(sum_daily_quantity) as decimal(18, 2)) sum_quantity_15min_initial,
      generation_date
    from
      ml_preprod.initial_itemlevelcount_15min
    where
      location_num = __location_num__
      and generation_date =(
        select
          max(generation_date)
        from
          ml_preprod.initial_itemlevelcount_daily
        where
          location_num = __location_num__
      )
    group by
      generation_date
  ) b
  cross join (
    select
      sum(sum_quantity_15min_final) sum_quantity_15min_final
    from(
        select
          cast(sum(sum_daily_quantity) as decimal(18, 2)) sum_quantity_15min_final,
          business_date
        from
          data_forecast_qc.final_itemlevelcount_15min_QC
        where
          location_num = __location_num__
          and generation_date =(
            select
              max(generation_date)
            from
              ml_preprod.final_itemlevelcount_daily
            where
              location_num = __location_num__
          )
        group by
          generation_date
      ) a
  ) c
  cross join (
    select
      cast(sum(sum_daily_quantity) as decimal(18, 2)) sum_quantity_daily_final,
      max(business_date) business_date_max,
      generation_date
    from
      ml_preprod.final_itemlevelcount_daily
    where
      location_num = __location_num__
      and generation_date =(
        select
          max(generation_date)
        from
          ml_preprod.final_itemlevelcount_daily
        where
          location_num = __location_num__
      )
    group by
      generation_date
  ) d;
