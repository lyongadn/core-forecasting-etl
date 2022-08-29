select distinct location_num from store_wise_qc.redundancy_daily_itemcount where 
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
and forecast_qc=0;
