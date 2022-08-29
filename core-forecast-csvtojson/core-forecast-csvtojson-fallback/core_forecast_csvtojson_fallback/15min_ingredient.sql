-- this query generates ingredient level forecast at 15min level for one location and date. 
select  Distinct itemcode, 
        intervalstart, 
        servicetype,
        transactioncount,
        lastyearcount
        from 
(
SELECT    salesitemcode itemcode, 
          --  date_add('year',1,cast(cast(timequarter as timestamp) as date))timequarter, 
          Substring( Concat( Concat('%s', 'T' ), Substring(Cast(timequarter AS CHAR(55)), 12, 19) ), 1, 19 ) intervalstart -- ,timequarter -- ,timequarter, 
          , 
          '3'                                             AS servicetype, 
          Cast(Coalesce(a.forecast, 0) AS DECIMAL(18, 2))    transactioncount, 
          Cast(Coalesce(c.amount, 0) AS   DECIMAL(18, 2))    lastyearcount,
          case when a.forecast = 0 and c.amount =0 then 0 else 1 end as removezeros
FROM      ( 
                    SELECT   Distinct  a.location_num, 
                              datetimecrea                    timequarter, 
                              Substring(datetimecrea, 12, 19) timequarter1, 
                              a.ingredient                    salesitemcode, 
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
                                                                                    ingredient_id ingredient
                                                                    FROM            database.15min_ingredient_table 
                                                                    WHERE           ( 
                                                                                                    location_num) = 'store_number'
                                                                    AND             business_date = ('%s') ) lastyeardatesfinal ) a 
                    LEFT JOIN 
                              ( 
                                     SELECT * 
                                     FROM   ( 
                                                   SELECT Distinct location_num, 
                                                          business_date, 
                                                          ingredient_quantity amount, 
                                                          timequarter, 
                                                          ingredient_id ingredient, 
                                                          'date_last_year' AS date_last 
                                                   FROM   ml_preprod.initial_ingredient_15min b 
                                                   WHERE  location_num = 'store_number' 
                                                   AND    business_date > date_add('%s', INTERVAL -1 year) 
                                                   AND    business_date <= date_add(date_add('%s', INTERVAL -1 year), INTERVAL 2 day) ) thisyearsales 
                                     WHERE  ( 
                                                   business_date) = date_last ) b 
                    ON -- cast(cast(a.datetimecrea as timestamp )as date)= cast(cast(b.timequarter as timestamp )as date) and
                              a.location_num = b.location_num 
                    AND       a.date_ = b.business_date 
                    AND       a.ingredient = b.ingredient 
                    AND       a.datetimecrea = b.timequarter ) c 
LEFT JOIN 
          ( 
                 SELECT Distinct * 
                 FROM   database.15min_ingredient_table  
                 WHERE  location_num = 'store_number' 
                 AND    business_date = '%s' 
                 AND    generation_date = 
                        ( 
                               SELECT max(generation_date) 
                               FROM   database.15min_ingredient_table  
                               WHERE  ( 
                                             location_num) = 'store_number' 
                               AND    business_date = '%s' ) ) a 
ON        c.location_num = a.location_num 
AND       c.salesitemcode = a.ingredient_id 
AND       cast(c.timequarter1 AS CHAR(8)) = cast(a.interval_start_time AS CHAR(8))
)removezero 
where removezeros = 1 -- limit 10 
          -- where SalesItemCode in ('300104','20082') 
ORDER BY  itemcode, 
          intervalstart;