/*
This query generates 15min level ingredient forecast from itemlevelcount daily database store wise
with the store_number, forecast_table, lookup_table passed via lambda script
*/
select   
    business_Date,ingredient_id,sum(cast(
-- multiplying forecast with quantity and ycf metric
        (forecast * (ingredient_quantity) * coalesce(ycf, 1))as decimal(18,2)  )) sumingredient,
  unit_of_measure,
  cast(interval_start_time as char(10)) interval_start_time,
  curdate() generation_date,
  location_num from (select DISTINCT
  forecast,
     
     
      business_Date,
      location_num,
       item_id,
      interval_start_time
   from
          ui_preprod.__forecast_table__
        where
          location_num = '__store_number__' 
          and business_date = '__date__'
          and (generation_date) =
          (
            -- retrieve data based on generation date of last run
            select
              max(generation_date)
            from
              ui_preprod.__forecast_table__
            where
              location_num = '__store_number__' 
              and business_date = '__date__'
          )
 )final
   inner join  (select   ingredient_quantity,ycf,ingredient_id,unit_of_measure,pin  from (SELECT Distinct '__store_number__' location_num ,pin,ingredient_quantity,ingredient_id,unit_of_measure FROM ml_preprod.__lookup_table__ where generation_date=(select max(generation_date) from ml_preprod.__lookup_table__)) a
   left join (select
          distinct 
          ingredient_id as id,
          coalesce(avg(ycf), 1) ycf,
          '__store_number__' loc_ycf
        from
          ml_preprod.ycf_ingredients_monthly
        where
          location_num = '__store_number__'
          and generation_date =(
            select
              max(generation_date)
            from
              ml_preprod.ycf_ingredients_monthly
            where
              location_num = '__store_number__'
          )
              group by ingredient_id
      ) ycf on a.location_num = ycf.loc_ycf
      and a.ingredient_id = ycf.id)
    b on final.item_id = b.pin
   group by   business_Date,
  location_num,
  ingredient_id,
  unit_of_measure,
  interval_start_time

