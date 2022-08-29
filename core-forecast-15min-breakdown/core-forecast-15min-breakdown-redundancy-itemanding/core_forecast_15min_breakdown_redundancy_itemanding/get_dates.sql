select
  distinct business_date
from
  redundancy.daily_itemcount_lstm_redundancy a
where
  a.location_num = '__store_number__'
  and generation_date =(
    select
      max(generation_date)
    from
      redundancy.daily_itemcount_lstm_redundancy
    where
      location_num = '__store_number__'
  )
