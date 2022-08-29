SELECT distinct business_date
FROM __database__.__inferencedates_daily__
where holiday =1 and year(business_date) = year(current_date)
