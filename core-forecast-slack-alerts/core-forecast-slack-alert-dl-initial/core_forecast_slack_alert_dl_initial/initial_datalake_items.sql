select distinct qcdata.* from (select location_num,dailyqc from store_wise_qc.dataprep_initial_itemlevelcount_datalake 
where generation_date = (select max(generation_date) from store_wise_qc.dataprep_initial_itemlevelcount_datalake)
) qcdata
  inner join ml_preprod.crawler_stores_fixed totalstores on qcdata.location_num = totalstores.location_num
inner join (select distinct location_num from store_wise_qc.dataprep_initial_itemlevelcount_daily
where generation_date= (select max(generation_date) from store_wise_qc.dataprep_initial_itemlevelcount_daily)
)datalakeupdation
on datalakeupdation.location_num = qcdata.location_Num
where qcdata.location_num not in (select  
 last_gen_date.location_num from (select max(generation_date)lastmax_date,location_num from store_wise_qc.dataprep_initial_itemlevelcount_daily
where generation_date<(select max(generation_date) from store_wise_qc.dataprep_initial_itemlevelcount_daily
)group by location_num)last_gen_date
inner join
(select max(generation_date)curmax_date,location_num from store_wise_qc.dataprep_initial_itemlevelcount_daily
where generation_date=(select max(generation_date) from store_wise_qc.dataprep_initial_itemlevelcount_daily
)group by location_num 
)cur_gen_date on last_gen_date.location_num=cur_gen_date.location_num
where datediff(curmax_date,lastmax_date)>15)