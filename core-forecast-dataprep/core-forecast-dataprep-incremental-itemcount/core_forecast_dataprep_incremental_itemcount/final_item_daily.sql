-- selecting features required in modelling such aroundthanksgiving, aroundchristmas, sunday, etc on daily level
select
  features_data.aroundthanksgiving,
  features_data.aroundchristmas,
  features_data.onedaypriorchristmas_and_new_year,
  features_data.federalholiday,
  features_data.holiday,
  features_data.blackfridaycheck,
  features_data.business_date,
  features_data.dayofweek,
  features_data.sunday,
  features_data.location_num,
  features_data.pdinitial as product,
  coalesce(actuals.sum_daily_quantity, 0) sum_daily_quantity,
  features_data.federalholiday_name,
  CURDATE() generation_date
from
  (
    select
      *
    from
      (
        -- fetching distinct products present in that location
        select
          distinct product pdinitial,
          location_num
        from
          ml_preprod.initial_itemlevelcount_daily
        where
          location_num = '__store_number__'
      ) initial
      cross join (  -- adding features for each product location combination
        select
          *
        from
          ml_preprod.inferencedates_daily
        where
          -- selecting initial data from max date of last run to data present in initial table
          business_date >(
            select
              max(business_date)
            from
              ml_preprod.final_itemlevelcount_daily
            where
              location_num = '__store_number__'
          )
          and business_Date <=(
            select
              max(business_date)
            from
              ml_preprod.initial_itemlevelcount_daily
            where
              location_num = '__store_number__'
              -- selecting recent data based on max generation date
              and generation_date =(
                select
                  max(generation_date)
                from
                  initial_itemlevelcount_daily
                where
                  location_num = '__store_number__'
              )
          )
      ) features
  ) features_data
  left join ( -- joining feature data with actuals table
    select
      location_num,
      business_date,
      product,
      sum_daily_quantity
    from
      ml_preprod.initial_itemlevelcount_daily
    where
      location_num = '__store_number__'
      -- selecting initial data from max date of last run
      and business_date >(
        select
          max(business_date)
        from
          ml_preprod.final_itemlevelcount_daily
        where
          location_num = '__store_number__'
      )
  ) actuals on features_data.location_num = actuals.location_num
  and features_data.pdinitial = actuals.product
  and features_data.business_Date = actuals.business_Date;
