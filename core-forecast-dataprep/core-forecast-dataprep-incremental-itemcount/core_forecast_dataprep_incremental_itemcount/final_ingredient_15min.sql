-- selecting features required in modelling such unit_of_measure,timequarter,quarteres on ingredient level
select
  ingredient_id,
  unit_of_measure,
  business_Date,
  timequarter,
  quarters,
  SUM(sumingredient) ingredient_quantity,
  location_num
   ,CURDATE() generation_date
from
  (
    select
        sum_daily_quantity * (ingredient_quantity)
       sumingredient, -- multiplying quantity of products by ingredient quantity for particular pin
      ingredient_quantity,
      sum_daily_quantity,
      ingredient_id,
      business_Date,
      location_num,
      pin,
      unit_of_measure,
      timequarter,
      quarters
    from(
        select
          *
        from
          ml_preprod.initial_itemlevelcount_15min
        where
          location_num = '__store_number__'
          -- selecting item data greater than max date present in ingredient table
          and business_date > coalesce(
            (
              select
                max(business_date)
              from
                ml_preprod.initial_ingredient_15min
              where
                location_num = '__store_number__'
            ),
            date '2018-01-01'
          )
      ) actual_data
      inner join ml_preprod.lookup_ingredients ingredient_data on actual_data.product = ingredient_data.pin -- joining lookup ingre table with item initial table to get ingredient quantity
  ) A
Group by
  business_Date,
  location_num,
  ingredient_id,
  unit_of_measure,
  timequarter,
  quarters,
  CURDATE();
