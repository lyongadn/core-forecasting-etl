select aa.weekday,aa.Item_sales_total from
    (select a.business_date,weekday(a.business_date) weekday,sum(a.sum_daily_quantity) Item_sales_total from

            (
            SELECT location_num,business_date,sum_daily_quantity,generation_date
            FROM ml_preprod.initial_itemlevelcount_daily 
            where weekday(business_date) !=6 
            and location_num in (__store_num__) and product = 160001
            and business_date not in (select business_date from ml_preprod.inferencedates_daily
            where federalholiday_name != "Regular-day" or  holiday =1)
            ) a
            left join

            (SELECT location_num,business_date,max(generation_date) max_gendate
            FROM ml_preprod.initial_itemlevelcount_daily
            where weekday(business_date) !=6 and location_num in (__store_num__) and product = 160001
            group by location_num,business_date
            ) b
            on a.location_num = b.location_num and a.business_date = b.business_date and a.generation_date = b.max_gendate
            where b.business_date is not null 
            group by a.business_date ) aa

    right join      
    (select distinct business_date from ml_preprod.initial_itemlevelcount_daily
     where location_num in (__store_num__)
    order by business_date desc limit __limit__) bb 
    on aa.business_date = bb.business_date
    where aa.business_date is not null