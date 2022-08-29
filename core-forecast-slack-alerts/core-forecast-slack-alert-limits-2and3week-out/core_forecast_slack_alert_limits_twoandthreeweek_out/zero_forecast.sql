SELECT distinct "Business Date",
dollars, trans, item, ingre
FROM
        (SELECT business.business_date AS "Business Date",
                COALESCE(dollarsales.dollars,0) as dollars,
                COALESCE(transcount.trans,0) as trans,
                COALESCE(itemcount.item,0) as item,
                COALESCE(ingredient.ingre,0) as ingre
        FROM 
        (SELECT distinct business_date 
          FROM "json_alerts"."__table_14days__"
          WHERE generation_date = '__gen_date__') business
        LEFT JOIN
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollars,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND dollarsales_forecast_generated_successfully = 0
        GROUP BY  business_date) dollarsales
        ON business.business_date = dollarsales.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND transcount_forecast_generated_successfully = 0
        GROUP BY  business_date) transcount
        ON business.business_date = transcount.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND itemcount_forecast_generated_successfully = 0
        GROUP BY  business_date) itemcount
        ON business.business_date = itemcount.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND ingredient_forecast_generated_successfully = 0
        GROUP BY  business_date) ingredient
        ON business.business_date = ingredient.business_date)
        UNION
        (
        SELECT business.business_date AS "Business Date",
                COALESCE(dollarsales.dollars,0) as dollars,
                COALESCE(transcount.trans,0) as trans,
                COALESCE(itemcount.item,0) as item,
                COALESCE(ingredient.ingre,0) as ingre
        FROM 
        (SELECT distinct business_date 
          FROM "json_alerts"."__table_30days__"
          WHERE generation_date = '__gen_date__') business
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollars,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND dollarsales_forecast_generated_successfully = 0
        GROUP BY  business_date) dollarsales
        ON business.business_date = dollarsales.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND transcount_forecast_generated_successfully = 0
        GROUP BY  business_date) transcount
        ON business.business_date = transcount.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND itemcount_forecast_generated_successfully = 0
        GROUP BY  business_date) itemcount
        ON business.business_date = itemcount.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND ingredient_forecast_generated_successfully = 0
        GROUP BY  business_date) ingredient
        ON business.business_date = ingredient.business_date)
ORDER BY 1