SELECT distinct "Business Date",
"Dollar > 1.5", "Dollar < 0.9", "Trans > 1.5", "Trans < 0.9"
FROM
        (SELECT business.business_date AS "Business Date",
                COALESCE(dollarsales_gt.dollar_gt,0) AS "Dollar > 1.5",
                COALESCE(dollarsales_ls.dollar_ls,0) AS "Dollar < 0.9",
                COALESCE(transcount_gt.trans_gt,0) AS "Trans > 1.5",
                COALESCE(transcount_ls.trans_ls,0) AS "Trans < 0.9"
        FROM 
        (SELECT distinct business_date 
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__') business
        LEFT JOIN
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollar_gt,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "dollarsales_greater_than_1.5_max" = 1
        GROUP BY  business_date) dollarsales_gt
        ON business.business_date = dollarsales_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollar_ls,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "dollarsales_less_than_90%_min" = 1
        GROUP BY  business_date) dollarsales_ls
        ON business.business_date = dollarsales_ls.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans_gt,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "transcount_greater_than_1.5_max" = 1
        GROUP BY  business_date) transcount_gt
        ON business.business_date = transcount_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans_ls,
                business_date
        FROM "json_alerts"."__table_14days__"
        WHERE generation_date = '__gen_date__'
                AND "transcount_less_than_90%_min" = 1
        GROUP BY  business_date) transcount_ls
        ON business.business_date = transcount_ls.business_date)
        UNION
        (SELECT business.business_date AS "Business Date",
                COALESCE(dollarsales_gt.dollar_gt,0) AS "Dollar > 1.5",
                COALESCE(dollarsales_ls.dollar_ls,0) AS "Dollar < 0.9",
                COALESCE(transcount_gt.trans_gt,0) AS "Trans > 1.5",
                COALESCE(transcount_ls.trans_ls,0) AS "Trans < 0.9"
        FROM
        (SELECT distinct business_date 
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__') business
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollar_gt,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "dollarsales_greater_than_1.5_max" = 1
        GROUP BY  business_date) dollarsales_gt
        ON business.business_date = dollarsales_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS dollar_ls,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "dollarsales_less_than_90%_min" = 1
        GROUP BY  business_date) dollarsales_ls
        ON business.business_date = dollarsales_ls.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans_gt,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "transcount_greater_than_1.5_max" = 1
        GROUP BY  business_date) transcount_gt
        ON business.business_date = transcount_gt.business_date
        LEFT JOIN 
        (SELECT COALESCE(COUNT(distinct location_num),0) AS trans_ls,
                business_date
        FROM "json_alerts"."__table_30days__"
        WHERE generation_date = '__gen_date__'
                AND "transcount_less_than_90%_min" = 1
        GROUP BY  business_date) transcount_ls
        ON business.business_date = transcount_ls.business_date)
ORDER BY 1