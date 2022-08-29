select case when length(location_num)=1 then concat('0000',location_num) 
 when length(location_num)=2 then concat('000',location_num) 
  when length(location_num)=3 then concat('00',location_num) 
   when length(location_num)=4 then concat('0',location_num) 
   else location_num end as store_number from (select items.location_num from (select distinct location_num from store_wise_qc.forecast_15min_item_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_item_14thday_ahead)
and forecast_qc=1
union
select distinct location_num from store_wise_qc.forecast_15min_item_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_item_30thday_ahead)
and forecast_qc=1
union
select distinct location_num from store_wise_qc.forecast_daily_item_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_item_14thday_ahead)
and forecast_qc=1
union

select distinct location_num from store_wise_qc.forecast_daily_item_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_item_30thday_ahead)
and forecast_qc=1)items
 inner join (

select distinct location_num from store_wise_qc.forecast_15min_dollarsandtrans_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_dollarsandtrans_14thday_ahead)
and forecast_qc=1
union
select distinct location_num from store_wise_qc.forecast_15min_dollarsandtrans_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_dollarsandtrans_30thday_ahead)
and forecast_qc=1
union
select distinct location_num from store_wise_qc.forecast_daily_dollarsandtrans_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_dollarsandtrans_14thday_ahead)
and forecast_qc=1
union

select distinct location_num from store_wise_qc.forecast_daily_dollarsandtrans_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_dollarsandtrans_30thday_ahead)
and forecast_qc=1

)dollars on items.location_num =dollars.location_num)final
where location_num not in (select distinct location_num from store_wise_qc.forecast_15min_item_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_item_14thday_ahead)
and forecast_qc=0
union
select distinct location_num from store_wise_qc.forecast_15min_item_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_item_30thday_ahead)
and forecast_qc=0
union
select distinct location_num from store_wise_qc.forecast_daily_item_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_item_14thday_ahead)
and forecast_qc=0
union

select distinct location_num from store_wise_qc.forecast_daily_item_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_item_30thday_ahead)
and forecast_qc=0

union
select distinct  coalesce(location_num,0)location_num from store_wise_qc.forecast_15min_dollarsandtrans_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_dollarsandtrans_14thday_ahead)
and forecast_qc=0
union
select distinct  coalesce(location_num,0)location_num from store_wise_qc.forecast_15min_dollarsandtrans_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_15min_dollarsandtrans_30thday_ahead)
and forecast_qc=0
union
select distinct  coalesce(location_num,0)location_num from store_wise_qc.forecast_daily_dollarsandtrans_14thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_dollarsandtrans_14thday_ahead)
and forecast_qc=0
union

select distinct  coalesce(location_num,0)location_num from store_wise_qc.forecast_daily_dollarsandtrans_30thday_ahead where 
generation_date = (select max(generation_date) from store_wise_qc.forecast_daily_dollarsandtrans_30thday_ahead)
and forecast_qc=0
);
