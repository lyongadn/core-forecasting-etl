select distinct location_num,dailyqc from store_wise_qc.dataprep_dollarsalesandtranscount where generation_date =
(select max(generation_date) from store_wise_qc.dataprep_dollarsalesandtranscount) and dataprep_generation_date=current_date-1;
