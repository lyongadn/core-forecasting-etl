select case when length(location_num)=1 then concat('0000',location_num) 
 when length(location_num)=2 then concat('000',location_num) 
  when length(location_num)=3 then concat('00',location_num) 
   when length(location_num)=4 then concat('0',location_num) 
   else location_num end as store_number from (select items.location_num from (select distinct location_num from store_wise_qc.redundancy_15min_itemcount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_15min_itemcount)
and forecast_qc=1
union
select distinct location_num from store_wise_qc.redundancy_daily_itemcount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_daily_itemcount)
and forecast_qc=1)items
 inner join (
select distinct location_num from store_wise_qc.redundancy_15min_dollarsandtranscount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_15min_dollarsandtranscount)
and forecast_qc=1
union

select distinct location_num from store_wise_qc.redundancy_daily_dollarsandtranscount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_daily_dollarsandtranscount)
and forecast_qc=1

)dollars on items.location_num =dollars.location_num)final
where location_num not in (select distinct location_num from store_wise_qc.redundancy_daily_itemcount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_daily_itemcount)
and forecast_qc=0
union
select distinct location_num from store_wise_qc.redundancy_15min_itemcount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_15min_itemcount)
and forecast_qc=0
union
select distinct location_num from store_wise_qc.redundancy_15min_dollarsandtranscount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_15min_dollarsandtranscount)
and forecast_qc=0
union

select distinct location_num from store_wise_qc.redundancy_daily_dollarsandtranscount where 
generation_date = (select max(generation_date) from store_wise_qc.redundancy_daily_dollarsandtranscount)
and forecast_qc=0
);
