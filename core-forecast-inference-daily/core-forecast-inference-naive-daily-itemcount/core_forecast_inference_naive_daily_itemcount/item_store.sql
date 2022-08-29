select t1.* from
(SELECT distinct location_num, product,max(business_date) Max_Date FROM __database__.__initial_itemlevelcount_daily__ where location_num=__location_num__
group by location_num, product) t1

left Join

(select * from __database__.__lstm_combinations__) t2

on t1.location_num = t2.location_num and t1.product = t2.product

where (t2.location_num is null and t2.product is null) and cast(t1.location_num as signed) = __location_num__