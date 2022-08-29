SELECT * FROM __database__.__final_itemlevelcount_daily__
where location_num = __location_num__ and product in (__product__)
and business_date <= (select max(business_date) from __database__.__final_itemlevelcount_daily__ 
                        where location_num = __location_num__ and product in (__product__)) and 
      business_date > (select max(business_date) - INTERVAL 30 DAY
                         from __database__.__final_itemlevelcount_daily__ where location_num =__location_num__ and product in (__product__))