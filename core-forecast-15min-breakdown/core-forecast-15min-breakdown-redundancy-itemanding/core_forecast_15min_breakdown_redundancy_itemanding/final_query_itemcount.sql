-- This query breaksdown redundancy itemcount forecast to 15min level
select
  business_date,
  product,
  cast(timequarter as char(10)) timequarter,
  min15_forecast,
  curdate() generation_date,
  location_num
from
  (
    select
      business_date,
      product,
      timequarter,
      cast(min15_forecast as decimal(18, 2)) min15_forecast,
      location_num
    from
      (
        SELECT
          ratios.location_num,
          ratios.business_date,
          ratios.product,
          ratios.time as timequarter,
          -- for stores closed on weekends and on sunday make forecast zero
          case WHEN ratios.location_num IN (
            select
              location_num
            from
              ml_preprod.saturday_off_stores
          ) 
          AND dayofweek(cast(ratios.business_Date AS date)) IN (1, 7) THEN 0 
          WHEN dayofweek(cast(ratios.business_Date as date)) in (1) then 0 
          ELSE forecast * ratio end as min15_forecast 
        FROM
          (
            SELECT
              location_num,
              business_date,
              item_id as product,
              forecast
            FROM
              redundancy.daily_itemcount_lstm_redundancy a
            WHERE
              (location_num) = ('__store_number__')
              and business_date = '__date__'
              and generation_date = (
                -- retrieve latest data based on generation date of last run
                select
                  max(generation_date)
                from
                  redundancy.daily_itemcount_lstm_redundancy
                where
                  location_num = '__store_number__'
                  and business_date = '__date__'
              )
          ) forecast_data
          -- joining with ratios table to get ratios for each timequarter
          INNER JOIN (
            SELECT
              distinct *
            FROM
              ui_preprod.15min_itemcount_ratio
            where
              location_num = '__store_number__'
              and business_date = '__date__'
          ) ratios ON forecast_data.location_num = ratios.location_num
          AND forecast_data.business_Date = ratios.business_date
          and forecast_data.product = ratios.product
      ) final
    union
    -- for those dates where ratios are not present, divide the forecast by 65 and union the result
     select fore.business_date,fore.item_id as product,substring(datetimecrea,12,19)timequarter,
   case WHEN fore.location_num IN (
        select
          location_num
        from
          ml_preprod.saturday_off_stores
      )  
      and dayofweek(fore.business_Date) IN (1, 7) THEN 0 
      WHEN dayofweek(fore.business_Date) in (1) then 0 
      WHEN substring(datetimecrea, 12, 19) between '06:00:00' and '22:00:00' then CAST(forecast / 65 AS decimal(18, 2)) else 0 end as forecast,fore.location_num
    -- select *
    from(select distinct loc.location_num,loc.product,loc.business_date from (select  location_num, 
     item_id product,business_date from redundancy.daily_itemcount_lstm_redundancy where location_num='__store_number__' and business_date='__date__'
    and generation_date=(select max(generation_date) from redundancy.daily_itemcount_lstm_redundancy where  location_num='__store_number__' and business_date='__date__' )) loc 
    left join 
    (select * from ui_preprod.15min_itemcount_ratio  where location_num='__store_number__' and business_date='__date__' 
    )ratio on loc.location_num =ratio.location_num
            AND loc.business_Date=ratio.business_date and loc.product=ratio.product
     where ratio.location_num  is NULL
     )LPcomb inner join  (select * from redundancy.daily_itemcount_lstm_redundancy fore where location_num='__store_number__' and business_date ='__date__' 
    and generation_date=(select max(generation_date) from redundancy.daily_itemcount_lstm_redundancy where  location_num='__store_number__' and business_date='__date__') 
     )fore
    on LPcomb.location_num=fore.location_num and LPcomb.product=fore.item_id and LPcomb.business_date=fore.business_date
    cross join (select * from ml_preprod.date_quarters where date_ = '__date__'
    )dates on dates.date_ =LPcomb.business_date)final;
