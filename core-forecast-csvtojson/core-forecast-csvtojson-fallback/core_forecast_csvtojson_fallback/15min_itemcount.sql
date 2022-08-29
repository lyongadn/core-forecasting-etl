-- this query generates forecast for each salesItemcode at 15mins level for one location and date.
select Distinct salesitemcode, 
        intervalstart, 
        servicetype,
        transactioncount,
        lastyearcount
        from (
          SELECT    salesitemcode salesitemcode, 
          Substring( Concat( Concat('%s', 'T' ), Substring(Cast(timequarter AS CHAR(55)), 12, 19) ), 1, 19 ) intervalstart -- ,timequarter -- ,timequarter, 
          , 
          '3'                                             AS servicetype, 
          Cast(Coalesce(a.forecast, 0) AS DECIMAL(18, 2))    transactioncount, 
          Cast(Coalesce(c.amount, 0) AS   DECIMAL(18, 2))    lastyearcount ,
          case when a.forecast = 0 and c.amount =0 then 0 else 1 end as removezeros
FROM      ( 
                    SELECT   distinct a.location_num, 
                              (datetimecrea)                  timequarter, 
                              Substring(datetimecrea, 12, 19) timequarter1, 
                              a.product                       salesitemcode, 
                              cast(Coalesce(amount, 0) AS DECIMAL(18, 2)) amount 
                    FROM      ( 
                                         SELECT     * 
                                         FROM       ( 
                                                           SELECT * 
                                                           FROM   ( 
                                                                         SELECT *, 
                                                                                'date_last_year' AS date_last 
                                                                         FROM   ml_preprod.date_quarters ) lastyeardates
                                                           WHERE  ( 
                                                                         date_) =(date_last) ) lastyeardates1 
                                         CROSS JOIN 
                                                    ( 
                                                                    SELECT DISTINCT location_num, 
                                                                                    item_id product
                                                                    FROM            database.15min_itemcount_table
                                                                    WHERE           ( 
                                                                                                    location_num) = 'store_number'
                                                                    AND             business_date = ('%s') ) lastyeardatesfinal ) a 
                    LEFT JOIN 
                              ( 
                                     SELECT * 
                                     FROM   ( 
                                                   SELECT location_num, 
                                                          business_date, 
                                                          sum_daily_quantity amount, 
                                                          timequarter, 
                                                          product, 
                                                         'date_last_year' AS date_last 
                                                   FROM   ml_preprod.initial_itemlevelcount_15min b
                                                   WHERE  location_num = 'store_number' 
                                                   AND    business_date > date_add('%s', INTERVAL -1 year) 
                                                   AND    business_date <= date_add(date_add('%s', INTERVAL -1 year), INTERVAL 2 day) ) thisyearsales 
                                     WHERE  ( 
                                                   business_date) = date_last ) b 
                    ON -- cast(cast(a.datetimecrea as timestamp )as date)= cast(cast(b.timequarter as timestamp )as date) and
                              a.location_num = b.location_num 
                    AND       a.date_ = b.business_date 
                    AND       a.product = b.product 
                    AND       a.datetimecrea = b.timequarter ) c 
LEFT JOIN 
          ( 
                 SELECT distinct *, 
                        cast(interval_start_time AS CHAR) inte 
                 FROM   database.15min_itemcount_table 
                 WHERE  location_num = 'store_number' 
                 AND    business_date = '%s' 
                 AND    generation_date = 
                        ( 
                               SELECT max(generation_date) 
                               FROM   database.15min_itemcount_table 
                               WHERE  ( 
                                             location_num) = 'store_number' 
                               AND    business_date = '%s' ) ) a 
ON        c.location_num = a.location_num 
AND       c.salesitemcode = a.item_id 
AND       cast(c.timequarter1 AS time) = cast(a.interval_start_time AS time)
  )removezero 
            where removezeros =1
   -- where SalesItemCode in ('300104','20082') 

union all 

select product as salesitemcode, concat( concat( substring(cast('%s' AS CHAR(10)), 1, 10), 'T' ), (substring(timequarter, 12, 19) ) ) AS intervalstart,2
servicetype, 0 transactioncount,
 coalesce(sum_daily_quantity,0)lastyeartransactioncount
 from 
 ml_preprod.initial_deffered_itemlevelcount_15min where location_num ='store_number' and business_Date =  'date_last_year' 
 and sum_daily_quantity <> 0
order by servicetype,salesitemcode,intervalstart