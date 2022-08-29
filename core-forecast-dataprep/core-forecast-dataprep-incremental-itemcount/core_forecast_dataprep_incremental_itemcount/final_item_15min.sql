-- selecting features required in modelling such aroundthanksgiving, aroundchristmas, sunday, timequarter, etc on 15min level
select
  features.business_date,
  features.location_num,
  features.timequarter,
  features.pdinitial as product,
  coalesce(actuals.sum_daily_quantity, 0) sum_daily_quantity,
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
      cross join ( -- adding features for each product location combination
        select
          timequarter,business_date
        from
          ml_preprod.inferencedates_15min
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
      ) temp
  ) features
  left join ( -- joining feature data with actuals table
    select
      location_num,
      business_date,
      product,
      timequarter,
      sum_daily_quantity
    from
      ml_preprod.initial_itemlevelcount_15min
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
  ) actuals on features.location_num = actuals.location_num
  and features.pdinitial = actuals.product
  and features.business_Date = actuals.business_Date
  and features.timequarter = actuals.timequarter
