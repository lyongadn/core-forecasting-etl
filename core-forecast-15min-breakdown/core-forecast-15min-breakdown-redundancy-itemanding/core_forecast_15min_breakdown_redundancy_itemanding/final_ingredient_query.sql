
-- This query breaksdown the redundancy ingredient forecast to 15min level
select   
    business_Date,ingredient_id,sum(cast(
-- multiplying forecast with quantity and ycf metric
        (forecast * (ingredient_quantity) * coalesce(ycf, 1))as decimal(18,2)  )) sumingredient,
  unit_of_measure,
  cast(interval_start_time as char(10)) interval_start_time,
  curdate() generation_date,
  location_num from (select
  forecast,
     
     
      business_Date,
      location_num,
       item_id,
      interval_start_time
   from
          redundancy.15min_itemcount_lstm_redundancy
        where
          location_num = '__store_number__' 
          and business_date = '__date__'
          and (generation_date) =
          (
            -- retrieve data based on generation date of last run
            select
              max(generation_date)
            from
              redundancy.15min_itemcount_lstm_redundancy
            where
              location_num = '__store_number__' 
              and business_date = '__date__'
          )
 )final
   inner join  (select   ingredient_quantity,ycf,ingredient_id,unit_of_measure,pin  from (SELECT '__store_number__' location_num ,pin,ingredient_quantity,ingredient_id,unit_of_measure FROM ml_preprod.lookup_ingredients) a
   left join (select
          distinct 
          ingredient_id as id,
          coalesce(ycf, 1) ycf,
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
      ) ycf on a.location_num = ycf.loc_ycf
      and a.ingredient_id = ycf.id)
    b on final.item_id = b.pin
   group by   business_Date,
  location_num,
  ingredient_id,
  unit_of_measure,
  interval_start_time
