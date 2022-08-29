SELECT distinct "Business Date",  
"Item > 1.5", "Item < 0.9", "Ingre > 1.5", "Ingre < 0.9"
FROM
        (SELECT business.business_date AS "Business Date",
                COALESCE(itemcount_gt.item_gt,0) AS "Item > 1.5",
                COALESCE(itemcount_ls.item_ls,0) AS "Item < 0.9",
                COALESCE(ingredient_gt.ingre_gt,0) AS "Ingre > 1.5",
                COALESCE(ingredient_ls.ingre_ls,0) AS "Ingre < 0.9"
        FROM 
        (SELECT distinct business_date 
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__') business
        LEFT JOIN
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item_gt,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "itemcount_greater_than_1.5_max" = 1
        GROUP BY  business_date) itemcount_gt
        ON business.business_date = itemcount_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item_ls,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "itemcount_less_than_90%_min" = 1
        GROUP BY  business_date) itemcount_ls
        ON business.business_date = itemcount_ls.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre_gt,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "ingredient_greater_than_1.5_max" = 1
        GROUP BY  business_date) ingredient_gt
        ON business.business_date = ingredient_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre_ls,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "ingredient_less_than_90%_min" = 1
        GROUP BY  business_date) ingredient_ls
        ON business.business_date = ingredient_ls.business_date)
        UNION
        (SELECT business.business_date AS "Business Date",
                COALESCE(itemcount_gt.item_gt,0) AS "Item > 1.5",
                COALESCE(itemcount_ls.item_ls,0) AS "Item < 0.9",
                COALESCE(ingredient_gt.ingre_gt,0) AS "Ingre > 1.5",
                COALESCE(ingredient_ls.ingre_ls,0) AS "Ingre < 0.9"
        FROM
        (SELECT distinct business_date 
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__') business
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item_gt,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "itemcount_greater_than_1.5_max" = 1
        GROUP BY  business_date) itemcount_gt
        ON business.business_date = itemcount_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS item_ls,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "itemcount_less_than_90%_min" = 1
        GROUP BY  business_date) itemcount_ls
        ON business.business_date = itemcount_ls.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre_gt,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "ingredient_greater_than_1.5_max" = 1
        GROUP BY  business_date) ingredient_gt
        ON business.business_date = ingredient_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS ingre_ls,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "ingredient_less_than_90%_min" = 1
        GROUP BY  business_date) ingredient_ls
        ON business.business_date = ingredient_ls.business_date)
ORDER BY 1