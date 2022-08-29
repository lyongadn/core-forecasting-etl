insert into
  ml_preprod.expected_forecasted_products
select location_num,count(distinct product) as products from ml_preprod.initial_itemlevelcount_daily where location_num=__location_num__
