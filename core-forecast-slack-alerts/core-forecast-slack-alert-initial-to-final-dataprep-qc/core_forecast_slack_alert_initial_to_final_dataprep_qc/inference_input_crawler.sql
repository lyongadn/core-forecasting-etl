select  case when length(location_num)=1 then concat('0000',location_num) 
 when  length(location_num)=2 then concat('000',location_num) 
  when length(location_num)=3 then concat('00',location_num)
  when length(location_num)=4 then concat('0',location_num) 
  else location_num
 end  store_number,status ,case when status=0 then (case when remodelclosedate='1901-01-01' then  remodelprojectedclosedate else 
remodelclosedate end)  else '1901-01-01' end as period from (
select location_num,
case when remodelclosedate='1901-01-01' and remodelprojectedclosedate='1901-01-01'  or 
 (case when remodelclosedate='1901-01-01' then  remodelprojectedclosedate else 
remodelclosedate end)>current_date() or (case when remodelreopendate='1901-01-01' then  remodelprojectedreopendate else 
remodelreopendate end)<date_add(current_date(), interval -25 day)
or remodelclosedate and remodelprojectedclosedate ='1901-01-01' 
  then 1
when  (case when remodelclosedate='1901-01-01' then  remodelprojectedclosedate else 
remodelclosedate end)<=current_date() and 
current_date()<date_add((case when remodelreopendate='1901-01-01' then  remodelprojectedreopendate else 
remodelreopendate end),interval -20 day)
 then 99
when (case when remodelreopendate='1901-01-01' then  remodelprojectedreopendate else 
remodelreopendate end)>= date_add(current_date(), interval -45 day) then 0
else 9999
 end as status
,remodelclosedate,remodelprojectedclosedate
,remodelprojectedreopendate, remodelreopendate
 from ml_preprod.locations_pq )a  inner join (select distinct location_Num as loc from ml_preprod.crawler_stores_fixed
 )b on a.location_num=b.loc
 order by location_num;
