select 
        aroundthanksgiving,
        aroundchristmas ,
        onedaypriorchristmas_and_new_year   ,
        federalholiday,
        holiday ,
        blackfridaycheck,
        business_date,
        dayofweek   ,
        sunday ,
        location_num,
        productmain product,
         coalesce(sum_daily_quantity,0)sum_daily_quantity,
        federalholiday_name,
        (select max(dataprep_generation_date) from store_wise_qc.dataprep_itemlevelcount)dataprep_generation_date
  -- date_add(curdate(),interval -2 day)maxdategenerated
   from (  select  a.location_num,a.product as productmain,MD,aa.* ,b.*  from(select a.* from (select distinct location_num ,product, max(business_date)MD from 
ml_preprod.final_itemlevelcount_daily
where location_num='store_number'
group by product)a
inner join (select distinct * from ml_preprod.lstm_combinations where location_num='store_number' ) lstmcomb on a.location_num=lstmcomb.location_num
and a.product=lstmcomb.product
)a cross join  (select * from ml_preprod.inferencedates_daily where  business_date > date_add('MaxDate', interval -  'actual_days' day) and  business_date <current_date )aa
left join 
(
select location_num loc,product pd,business_date bd,
        sum_daily_quantity,
  date_add(curdate(),interval -2 day)maxdategenerated
        from ml_preprod.final_itemlevelcount_daily
        where location_num ='store_number'  and business_date  > date_add('MaxDate', interval - 'actual_days' day)
        and business_date  <=('MaxDate')
        )b on a.location_num=b.loc and a.product =b.pd  and  aa.business_date=b.bd)final
where business_date  > date_add(date_add('MaxDate',interval -2 day), interval - 'actual_days' day) and business_date  <=date_add(curdate(),interval -2 day)
  -- >date_add(MD,interval - 'actual_days' day)
       -- group by a.product
--   and productmain='160001'
 
          
          union
        select 
       aroundthanksgiving,aroundchristmas  ,onedaypriorchristmas_and_new_year, federalholiday, holiday,blackfridaycheck,
        business_date,dayofweek,CASE
          WHEN location_num IN (SELECT * FROM ml_preprod.saturday_off_stores)
              AND dayofweek IN (6,7) THEN
          1
          ELSE sunday
          END AS sunday,location_num,Pd product,'0' as sum_daily_quantity,federalholiday_name,(select max(dataprep_generation_date) from store_wise_qc.dataprep_itemlevelcount)dataprep_generation_date
        from( select * from(select * from (  
        select distinct location_num ,product as PD, max(business_date)MD from 
ml_preprod.final_itemlevelcount_daily a
where location_num='store_number'
group by product
        )a inner join(select distinct location_num loc, product as prod from ml_preprod.lstm_combinations where location_num='store_number' ) lstmcomb on a.location_num=lstmcomb.loc and a.PD=lstmcomb.prod) totalproduct cross join ml_preprod.inferencedates_daily)A
        where business_date >date_add(curdate(),interval -2 day) and business_date <=date_add((date_add(curdate(),interval -2 day) ), interval 'numberOfFeatures' day)
        -- and  PD='160001'
        order by product,business_date
  
   ;
