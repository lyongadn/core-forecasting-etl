select distinct location_num,dailyqc from store_wise_qc.dataprep_itemlevelcount where generation_date = 
(select max(generation_date) from store_wise_qc.dataprep_itemlevelcount) and dataprep_generation_date=current_date-1;
