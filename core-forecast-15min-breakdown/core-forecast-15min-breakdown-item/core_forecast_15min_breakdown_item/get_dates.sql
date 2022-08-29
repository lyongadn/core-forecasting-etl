-- this query returns unique list of business_date for individuals stores
select
  distinct business_date
from
  ui_preprod.__table__ a
where
  a.location_num = '__store_number__'
  --  extract data based on the generation date of last run
  and generation_date =(
    select
      max(generation_date)
    from
      ui_preprod.__table__
    where
      location_num = '__store_number__'
  )
