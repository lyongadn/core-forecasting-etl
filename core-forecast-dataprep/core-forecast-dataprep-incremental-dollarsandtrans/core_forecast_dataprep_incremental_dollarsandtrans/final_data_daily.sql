-- selecting features required in modelling such aroundthanksgiving, aroundchristmas, sunday, etc on daily level
select
  expended_features.aroundthanksgiving,
  expended_features.aroundchristmas,
  expended_features.onedaypriorchristmas_and_new_year,
  expended_features.federalholiday,
  expended_features.holiday,
  expended_features.blackfridaycheck,
  expended_features.business_date,
  expended_features.dayofweek,
  CASE WHEN expended_features.location_num in (
    select
      location_num
    from
      __database__.saturday_off_stores
  )
  AND expended_features.dayofweek IN (6, 7) THEN 1 ELSE expended_features.sunday END AS Weekends,
  coalesce(actuals.sales_sub_total, 0) sales_sub_total,
  coalesce(actuals.trans_count, 0) trans_count,
  expended_features.location_num,
  expended_features.federalholiday_name,
  curdate() generation_date
from
  (
    SELECT
      *,
      '__store_number__' location_num
    FROM
      __database__.inferencedates_daily -- underlying features table which contains the required features
    WHERE
      -- selecting initial data from max date of last run to data present in initial table
      Business_date >(
        select
          max(business_Date)
        from
          __database__.final_dollarsalesandtranscount_daily
        where
          location_num = '__store_number__'
      )
      and cast(Business_date as date) <= (
        select
          max(business_Date)
        from
          __database__.initial_dollarsalesandtranscount_daily
        where
          location_num = '__store_number__'
      )
  ) expended_features
  left join ( -- joining feature data with actuals table
    select
     business_date,sales_sub_total,location_num,trans_count
    from
      __database__.initial_dollarsalesandtranscount_daily
    where
      location_num = '__store_number__'
      -- selecting initial data from max date of last run
      and Business_date > (
        select
          max(business_Date)
        from
          __database__.final_dollarsalesandtranscount_daily
        where
          location_num = '__store_number__'
      )
  ) actuals on actuals.business_date = expended_features.business_date
  and actuals.location_num = expended_features.location_num
