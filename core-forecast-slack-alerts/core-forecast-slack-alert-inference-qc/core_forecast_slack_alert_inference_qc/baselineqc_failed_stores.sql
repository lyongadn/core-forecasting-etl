select distinct location_num from baseline_qc.forecast_15min_item_14thday_ahead where 
generation_date = (select max(generation_date) from baseline_qc.forecast_15min_item_14thday_ahead)
and forecast_qc=0
union
select distinct location_num from baseline_qc.forecast_15min_item_30thday_ahead where 
generation_date = (select max(generation_date) from baseline_qc.forecast_15min_item_30thday_ahead)
and forecast_qc=0
union
select distinct  coalesce(location_num,0)location_num from baseline_qc.forecast_15min_dollarsandtrans_14thday_ahead where 
generation_date = (select max(generation_date) from baseline_qc.forecast_15min_dollarsandtrans_14thday_ahead)
and forecast_qc=0
union
select distinct  coalesce(location_num,0)location_num from baseline_qc.forecast_15min_dollarsandtrans_30thday_ahead where 
generation_date = (select max(generation_date) from baseline_qc.forecast_15min_dollarsandtrans_30thday_ahead)
and forecast_qc=0