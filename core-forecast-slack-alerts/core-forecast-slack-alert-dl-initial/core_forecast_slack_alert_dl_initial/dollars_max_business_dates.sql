select max(business_date)business_date, max(generation_date)generation_date from ml_preprod.initial_dollarsalesandtranscount_daily
where generation_date=(select max(generation_date) from ml_preprod.initial_dollarsalesandtranscount_daily);