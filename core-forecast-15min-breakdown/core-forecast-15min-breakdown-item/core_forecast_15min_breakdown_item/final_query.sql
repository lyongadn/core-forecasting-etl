-- breaksdown the daily forecast to 15min level for itemcount. Forecast_tables(lstm10 and lstm30 days),dates and store_number comes from lambda script
select
  business_date,
  product,
  cast(timequarter as char) timequarter,
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
          ratios_table.location_num,
          ratios_table.business_date,
          ratios_table.product,
          ratios_table.time as timequarter,
          -- make forecast 0 for weekends and given list of stores
          case 
              WHEN ratios_table.location_num IN (select location_num from ml_preprod.saturday_off_stores)
              AND dayofweek(cast(ratios_table.business_Date AS date)) IN (1, 7) 
            THEN 0 
              WHEN dayofweek(cast(ratios_table.business_Date as date)) in (1) 
            THEN 0 
            ELSE forecast * ratio 
          END as min15_forecast 
        FROM
          (
            SELECT
              location_num,
              business_date,
              item_id as product,
              forecast
            FROM
              ui_preprod.__forecast_table__ a
            WHERE
              location_num = '__store_number__'
              and business_date = '__date__'
              and generation_date =(
                select
                  max(generation_date)
                from
                  ui_preprod.__forecast_table__
                where
                  location_num = '__store_number__'
                  and business_Date = '__date__'
              )
          ) forecast_data
          INNER JOIN ( -- joining the daily table with 15min ratios table to breakdown the forecast
            SELECT
             distinct *
            FROM
              ui_preprod.15min_itemcount_ratio
            where
              location_num = '__store_number__'
              and business_date = '__date__'
          ) ratios_table ON forecast_data.location_num = ratios_table.location_num
          AND forecast_data.business_Date = ratios_table.business_date
          and forecast_data.product = ratios_table.product
      ) final
    union -- here we are calculating the forecast for location product combinations which dont have ratios present
    select
      forecast_data.business_date,
      forecast_data.item_id as product,
      substring(datetimecrea, 12, 19) timequarter,
      case WHEN forecast_data.location_num IN (652, 375, 517)
      AND dayofweek(forecast_data.business_Date) IN (1, 7) THEN 0 WHEN dayofweek(forecast_data.business_Date) in (1) then 0 when substring(datetimecrea, 12, 19) between '06:00:00'
      and '22:00:00' then CAST(forecast / 65 AS decimal(18, 2)) else 0 end as forecast,
      forecast_data.location_num 
    from(
        select
          distinct forecast_data.location_num,
          forecast_data.product,
          forecast_data.business_date
        from
          (
            select
              location_num,
              item_id product,
              business_date
            from
              ui_preprod.__forecast_table__
            where
              location_num = '__store_number__'
              and business_date = '__date__'
          ) forecast_data
          left join (
            select
              distinct *
            from
              ui_preprod.15min_itemcount_ratio
            where
              location_num = '__store_number__'
              and business_date = '__date__'
          ) ratio on forecast_data.location_num = ratio.location_num
          AND forecast_data.business_Date = ratio.business_date
          and forecast_data.product = ratio.product
        where
          ratio.location_num is NULL
      ) LPcomb
      inner join (
        select
          *
        from
          ui_preprod.__forecast_table__
        where
          location_num = '__store_number__'
          and business_Date = '__date__'
          and generation_date = ( -- extracting data with max generation_date from last run
            select
              max(generation_date)
            from
              ui_preprod.__forecast_table__
            where
              location_num = '__store_number__'
              and business_Date = '__date__'
          )
      ) forecast_data on LPcomb.location_num = forecast_data.location_num
      and LPcomb.product = forecast_data.item_id
      and LPcomb.business_date = forecast_data.business_date
      cross join (
        select
          *
        from
          ml_preprod.date_quarters
        where
          date_ = '__date__'
      ) dates on dates.date_ = LPcomb.business_date
  ) final;
