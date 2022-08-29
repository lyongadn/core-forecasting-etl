select distinct business_date from database.table where location_num='store_number' and generation_date=
        (select max(generation_date) from database.table where location_num='store_number')
