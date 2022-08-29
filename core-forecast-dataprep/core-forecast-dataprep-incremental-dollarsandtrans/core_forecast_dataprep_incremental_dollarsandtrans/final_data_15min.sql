select
  expended_features.business_date,
  expended_features.timequarter,
  coalesce(actuals.sales_sub_total, 0) sales_sub_total,
  coalesce(actuals.trans_count, 0) trans_count,
  expended_features.location_num,
  CURDATE() generation_date
from(
    select
      * 
    from
      (
        SELECT
          business_date,timequarter
        FROM
          __database__.inferencedates_15min -- underlying features table which has all the features
        WHERE
          -- selecting initial data from max date of last run to data present in initial table
          Business_date >(
            select
              max(business_Date)
            from
              __database__.final_dollarsalesandtranscount_15min
            where
              location_num = '__store_number__'
          )
          and Business_date <= (
            select
              max(business_Date)
            from
              __database__.initial_dollarsalesandtranscount_15min
            where
              location_num = '__store_number__'
          )
      ) features_table
      cross join ( -- expending features table store wise
        select
          '__store_number__' location_num
      ) loc
  ) expended_features
  left join ( -- joining feature data with actuals table
    select
      business_date,timequarter,sales_sub_total,trans_count,location_num
    from
      __database__.initial_dollarsalesandtranscount_15min
    where
      location_num = '__store_number__'
      -- selecting initial data from max date of last run
      and Business_date > (
        select
          max(business_Date)
        from
          __database__.final_dollarsalesandtranscount_15min
        where
          location_num = '__store_number__'
      )
  ) actuals on actuals.location_num = expended_features.location_num
  and actuals.business_date = expended_features.business_date
  and actuals.timequarter = expended_features.timequarter
  order by location_num,business_date,timequarter
  
  ;
