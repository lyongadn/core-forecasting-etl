insert into ml_preprod.lookup_ingredients 
select Distinct * from ml_preprod.lookup_ingredients_history
where generation_date = (select max(generation_date) from ml_preprod.lookup_ingredients_history)