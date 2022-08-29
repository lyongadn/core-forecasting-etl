/*
This query breaks down the forecast at daily level to 15min level store wise
the forecast_table and ratio table are passed via lambda function script
*/
-- select sum(min15_forecast),sum(min15_forecast_trans) from (
Select distinct 
  business_date,
  timequarter,
  cast(min15_forecast as decimal(18, 2)) min15_forecast,
   cast(min15_forecast_trans as decimal(18, 2)) min15_forecast_trans,
  curdate() generation_date,
  location_num
from
  (
    SELECT
      ratios_data.location_num,
      ratios_data.business_date,
      ratios_data.time as timequarter,
      -- for stores which are closed on weekends and for business_dates in weekend make the forecast 0
      case WHEN ratios_data.location_num IN (select location_num from ml_preprod.saturday_off_stores)
      AND DAYOFWEEK(ratios_data.business_Date) IN (1, 7) THEN 0 WHEN DAYOFWEEK(ratios_data.business_Date) in (1) then 0 else forecast * ratios_data.ratio end as min15_forecast
      ,  case WHEN ratios_data.location_num IN (select location_num from ml_preprod.saturday_off_stores)
      AND DAYOFWEEK(ratios_data.business_Date) IN (1, 7) THEN 0 WHEN DAYOFWEEK(ratios_data.business_Date) in (1) then 0 else forecast_trans * ratios_data_trans.ratio end as min15_forecast_trans
    FROM
      (
        SELECT
          location_num,
          business_date,
          forecast
        FROM
          ui_preprod.__forecast_table__
        WHERE
          location_num = '__store_number__'
          --  extract data based on the generation date of last run 
          and generation_date = (
            select
              max(generation_date)
            from
              ui_preprod.__forecast_table__
            where
              location_num = '__store_number__'
          )
      ) forecast_data
      INNER JOIN (
        -- join the forecast at daily level with the 15min ratios for breaking down the forecast
        SELECT
          *
        FROM
          ui_preprod.__ratio_table__
        where
          location_num = '__store_number__'
          and generation_date=(select max(generation_date) from ui_preprod.__ratio_table__ where location_num = '__store_number__')
          --  and business_date> current_date
      ) ratios_data ON forecast_data.location_num = ratios_data.location_num
      AND forecast_data.business_Date = ratios_data.business_date
inner join

(
        SELECT
          location_num location_num_trans,
          business_date business_Date_trans,
          forecast forecast_trans
        FROM
          ui_preprod.__forecast__trans__ 
        WHERE
          location_num = '__store_number__'
          --  extract data based on the generation date of last run 
          and generation_date = (
            select
              max(generation_date)
            from
              ui_preprod.__forecast__trans__
            where
              location_num = '__store_number__'
          )
      ) forecast_data_trans on  forecast_data.location_num=forecast_data_trans.location_num_trans
      and  forecast_data.business_Date=forecast_data_trans.business_Date_trans
      INNER JOIN (
        -- join the forecast at daily level with the 15min ratios for breaking down the forecast
        SELECT
          *
        FROM
          ui_preprod.__ratio__trans__
        where
          location_num = '__store_number__'
          and generation_date=(select max(generation_date) from ui_preprod.__ratio__trans__ where location_num = '__store_number__')
           --  and business_date> current_date
      ) ratios_data_trans ON ratios_data.location_num = ratios_data_trans.location_num
      AND ratios_data.business_Date = ratios_data_trans.business_date
      and ratios_data.time=ratios_data_trans.time
      
      
  ) Final;

