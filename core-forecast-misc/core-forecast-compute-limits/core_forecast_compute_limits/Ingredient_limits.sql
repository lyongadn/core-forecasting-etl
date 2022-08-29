select aa.weekday,aa.Ing_quantity_total from
    (SELECT business_date,weekday(business_date) weekday,sum(ingredient_quantity) Ing_quantity_total
            FROM ml_preprod.initial_ingredient_15min
            where location_num in (__store_num__)  and ingredient_id in (1)
            and business_date not in (select business_date from ml_preprod.inferencedates_daily
            where federalholiday_name != "Regular-day" or  holiday =1)
            group by business_date) aa
    right join      
    (select distinct business_date from ml_preprod.initial_ingredient_15min
     where location_num in (__store_num__)
    order by business_date desc limit __limit__) bb 
    on aa.business_date = bb.business_date
    where aa.business_date is not null 