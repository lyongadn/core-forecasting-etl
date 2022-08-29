select coalesce((select max(dollars.business_Date_max)MaxDate from  (select (business_Date_max)business_Date_max,dailyqc,generation_Date,location_num from store_wise_qc.dataprep_dollarsalesandtranscount
where location_num='store_number' and dailyqc=1 and generation_Date>date_add(current_date(), interval -61 day) ) dollars inner join 
(select (business_Date_max)business_Date_max,dailyqc,generation_Date,location_num from store_wise_qc.dataprep_itemlevelcount
where location_num='store_number' and dailyqc=1 and generation_Date>date_add(current_date(), interval -61 day)) item
on item.generation_date=dollars.generation_date and item.location_num=dollars.location_num
where dollars.dailyqc=1 and item.dailyqc=1),
(select
                  max(business_Date)
                from
                  ml_preprod.final_dollarsalesandtranscount_daily
                where
                  location_num = 'store_number'));
