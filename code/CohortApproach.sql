WITH 
returned_products AS 
(
SELECT t2.return_date
    , t2.product_id 
    , t2.return_id 
    , t2.state 
    , t1.category
    , t1.subcategory sub_category
    , t3.completion_date
    , t3.status
FROM STATISTICALDATA.PUBLIC.PRODUCTS t1
LEFT JOIN STATISTICALDATA.PUBLIC.RETURNS t2
    ON t1.productid = t2.product_id
LEFT JOIN STATISTICALDATA.PUBLIC.RETURNSTATUS t3
    ON t2.return_id = t3.return_id
)

, data_model AS 
(
SELECT t3.cal_day
    , t1.return_date
    , t1.completion_date
    -- , COALESCE(t1.completion_date, '2024-9-30') completion_date
    , t3.is_holiday
    , t3.is_weekend
    -- , CASE 
    --     WHEN t1.completion_date IS NOT NULL THEN DATEDIFF(DAY, t1.return_date, t1.completion_date)
    --     ELSE DATEDIFF(DAY, t1.return_date, t3.cal_day)
    --   END aging_days
    
    , DATEDIFF(DAY, t1.return_date, t3.cal_day) aging_days
    
    -- , CASE 
    --     WHEN t1.sold_date IS NOT NULL 
    --         THEN DATEDIFF(DAY, TO_DATE(t1.return_date, 'yyyy-MM-dd'), t1.completion_date) - (SUM(t3.is_holiday) + SUM(t3.is_weekend))
    --     ELSE DATEDIFF(DAY, TO_DATE(t1.return_date, 'yyyy-MM-dd'), t3.cal_day) - (SUM(t3.is_holiday) + SUM(t3.is_weekend))
    --   END aging_days_exclude_holidays
    
    , SUM(CASE WHEN t3.is_holiday = 0 AND t3.is_weekend = 0 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY t1.product_id ORDER BY t3.cal_day) - 
         (CASE WHEN t3.is_holiday = 1 OR t3.is_weekend = 1 THEN 0 ELSE 1 END) AS aging_days_exclude_holidays
         
    , t1.product_id
    , t1.return_id
    , t1.state
    , t1.category
    , t1.sub_category
    , CASE 
        WHEN t1.Status = 'Open' THEN 'inshelf'
        WHEN t1.Status = 'Closed' THEN 'sold'
      END Status

FROM returned_products t1
INNER JOIN (
                SELECT DISTINCT product_id
                FROM returned_products
                GROUP BY ALL 
                HAVING COUNT(product_id) = 1  
           ) t2
           ON t1.product_id = t2.product_id

 INNER JOIN (
                SELECT cal_day
                 , is_holiday
                 , is_weekend
                FROM STATISTICALDATA.PUBLIC.CALENDAR
                WHERE 1 = 1
            ) t3
            ON t3.cal_day BETWEEN t1.return_date AND COALESCE(t1.completion_date, '2024-9-30') --COALESCE(t1.sold_date, CURRENT_DATE())
            
 WHERE 1 = 1
)


, sold_items AS 
(
SELECT DISTINCT product_id
    , status
    , MAX(aging_days) aging_days
FROM data_model
WHERE 1 = 1
    AND LOWER(TRIM(status)) = 'sold'
    AND product_id NOT IN (SELECT DISTINCT product_id FROM data_model WHERE LOWER(TRIM(status)) = 'inshelf')
GROUP BY ALL 
)

, sold_items_count AS 
(
SELECT CASE 
        WHEN aging_days BETWEEN 0 AND 14 THEN '0-14'
        WHEN aging_days BETWEEN 15 AND 29 THEN '15-29'
        WHEN aging_days BETWEEN 30 AND 44 THEN '30-44'
        WHEN aging_days BETWEEN 45 AND 59 THEN '45-59'
        WHEN aging_days BETWEEN 60 AND 74 THEN '60-74'
        WHEN aging_days BETWEEN 75 AND 89 THEN '75-89'
        WHEN aging_days BETWEEN 90 AND 104 THEN '90-105'
        WHEN aging_days BETWEEN 105 AND 119 THEN '105-119'
        WHEN aging_days BETWEEN 120 AND 1000 THEN '120-1000'
       END interval_in_days   
    , COUNT(DISTINCT product_id) sold_sku_count
FROM sold_items
GROUP BY ALL 
)

, censored_items AS 
(
SELECT product_id
    , MAX(aging_days) aging_days
    , status
FROM data_model
WHERE 1 = 1
    AND LOWER(TRIM(status)) = 'inshelf'
GROUP BY ALL 
)

, censored_items_count AS 
(
SELECT CASE 
        WHEN aging_days BETWEEN 0 AND 14 THEN '0-14'
        WHEN aging_days BETWEEN 15 AND 29 THEN '15-29'
        WHEN aging_days BETWEEN 30 AND 44 THEN '30-44'
        WHEN aging_days BETWEEN 45 AND 59 THEN '45-59'
        WHEN aging_days BETWEEN 60 AND 74 THEN '60-74'
        WHEN aging_days BETWEEN 75 AND 89 THEN '75-89'
        WHEN aging_days BETWEEN 90 AND 104 THEN '90-105'
        WHEN aging_days BETWEEN 105 AND 119 THEN '105-119'
        WHEN aging_days BETWEEN 120 AND 1000 THEN '120-1000'
       END interval_in_days   
    , COUNT(DISTINCT product_id) censored_sku_count
FROM censored_items
GROUP BY ALL 
)

, alive_at_beginning AS 
(
SELECT COUNT(DISTINCT product_id) alive_at_beginning_of_period
FROM data_model
)


, interval AS
(
SELECT CASE 
        WHEN aging_days BETWEEN 0 AND 14 THEN '0-14'
        WHEN aging_days BETWEEN 15 AND 29 THEN '15-29'
        WHEN aging_days BETWEEN 30 AND 44 THEN '30-44'
        WHEN aging_days BETWEEN 45 AND 59 THEN '45-59'
        WHEN aging_days BETWEEN 60 AND 74 THEN '60-74'
        WHEN aging_days BETWEEN 75 AND 89 THEN '75-89'
        WHEN aging_days BETWEEN 90 AND 104 THEN '90-105'
        WHEN aging_days BETWEEN 105 AND 119 THEN '105-119'
        WHEN aging_days BETWEEN 120 AND 1000 THEN '120-1000'
       END interval_in_days        
FROM data_model
GROUP BY ALL
)

, observations AS 
(
SELECT DISTINCT t1.interval_in_days
, COALESCE(t2.sold_sku_count, 0) sold_skus
, COALESCE(t3.censored_sku_count, 0) censored_skus
, t4.alive_at_beginning_of_period

FROM interval t1
LEFT JOIN sold_items_count t2
    ON t1.interval_in_days = t2.interval_in_days
LEFT JOIN censored_items_count t3
    ON t1.interval_in_days = t3.interval_in_days
CROSS JOIN alive_at_beginning t4 
)

, indexing AS 
(
SELECT *
    , ROW_NUMBER() OVER (ORDER BY CAST(SPLIT_PART(interval_in_days, '-', 1) AS INT) ASC) row_number
FROM observations
)

-- select * from indexing;

, ordered_intervals AS -- Recursive CTE
(
 SELECT row_number
    , interval_in_days
    , sold_skus Number_of_Deaths_Dt
    , censored_skus Number_Censored_Ct
    , alive_at_beginning_of_period Number_At_Risk_Nt
 FROM indexing 
 WHERE 1 = 1
     AND row_number = 1
     
UNION ALL 

SELECT t1.row_number
    , t1.interval_in_days
    , t1.sold_skus Number_of_Deaths_Dt
    , t1.censored_skus Number_Censored_Ct
    , (t2.Number_At_Risk_Nt - (t2.Number_of_Deaths_Dt + t2.Number_Censored_Ct)) Number_At_Risk_Nt
FROM indexing t1
INNER JOIN ordered_intervals t2
    ON t1.row_number = t2.row_number + 1
)

-- select * from ordered_intervals;
--  Life Table (Actuarial Table)

, life_table AS 
(
SELECT interval_in_days "IntervalinDays"
    , row_number "RowNumber"
    , Number_At_Risk_Nt "NumberAliveatBeginningofInterval"
    , COALESCE(Number_of_Deaths_Dt, 0) "NumberofCompletionDuringInterval"
    , COALESCE(Number_Censored_Ct, 0) "NumberCensored"
FROM ordered_intervals
)


, follow_up_life_table AS 
(
SELECT "RowNumber"
    , "IntervalinDays"
    , "NumberAliveatBeginningofInterval" "NumberAtRiskDuringInterval_Nt"
    , "NumberofCompletionDuringInterval" "NumberofDeathsDuringInterval_Dt"
    , "NumberCensored" "LostToFollowUp_Ct"
    , GREATEST(0, "NumberAliveatBeginningofInterval" - DIV0("NumberCensored", 2))  "AverageNumberatRiskDuringInterval_Nt*" -- Adjusted for censoring
    
    ,  GREATEST(0, DIV0("NumberofCompletionDuringInterval",  ("NumberAliveatBeginningofInterval" - DIV0("NumberCensored", 2))))  "ProportionDyingDuringInterval_qt"
    
    , 1 - DIV0("NumberofCompletionDuringInterval",  ("NumberAliveatBeginningofInterval" - DIV0("NumberCensored", 2))) "AmongThoseatRisk_ProportionSurvivingInterval_pt"
    
FROM life_table
ORDER BY "RowNumber" ASC
)


, survival_probability AS 
(
SELECT "RowNumber"
    , "IntervalinDays"
    , "NumberAtRiskDuringInterval_Nt"
    , "NumberofDeathsDuringInterval_Dt"
    , "LostToFollowUp_Ct"
    , "AverageNumberatRiskDuringInterval_Nt*" 
    ,  "ProportionDyingDuringInterval_qt"
    , "AmongThoseatRisk_ProportionSurvivingInterval_pt"
    , CASE
        WHEN "RowNumber" = 1 THEN "AmongThoseatRisk_ProportionSurvivingInterval_pt"
        ELSE "AmongThoseatRisk_ProportionSurvivingInterval_pt" * EXP(SUM(LN("AmongThoseatRisk_ProportionSurvivingInterval_pt")) OVER (ORDER BY "RowNumber" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING))
    END "SurvivalProbability_St"

    -- , SQRT(SUM(DIV0("NumberofDeathsDuringInterval_Dt", ("NumberAtRiskDuringInterval_Nt"*("NumberAtRiskDuringInterval_Nt"-"NumberofDeathsDuringInterval_Dt")))) OVER (ORDER BY "RowNumber" ASC)) "CumulativeQuantity"
FROM follow_up_life_table
ORDER BY "RowNumber" ASC
)

, final_table AS 
(
SELECT "RowNumber"
    , "IntervalinDays"
    , "NumberAtRiskDuringInterval_Nt"
    , "NumberofDeathsDuringInterval_Dt"
    , "LostToFollowUp_Ct"
    , "AverageNumberatRiskDuringInterval_Nt*" 
    ,  "ProportionDyingDuringInterval_qt"
    , "AmongThoseatRisk_ProportionSurvivingInterval_pt"
    , "SurvivalProbability_St"

    -- , ("SurvivalProbability_St"*"SurvivalProbability_St") *(SUM(DIV0("ProportionDyingDuringInterval_qt", ("AmongThoseatRisk_ProportionSurvivingInterval_pt"*"AverageNumberatRiskDuringInterval_Nt*"))) OVER (ORDER BY "RowNumber" ASC)) 

    , ("SurvivalProbability_St"*"SurvivalProbability_St") * (SUM(DIV0("ProportionDyingDuringInterval_qt", ("AverageNumberatRiskDuringInterval_Nt*"*("AverageNumberatRiskDuringInterval_Nt*" - "ProportionDyingDuringInterval_qt")))) OVER (ORDER BY "RowNumber" ASC)) "VarianceEstimate"

    , (SQRT(("SurvivalProbability_St"*"SurvivalProbability_St") * (SUM(DIV0("ProportionDyingDuringInterval_qt", ("AverageNumberatRiskDuringInterval_Nt*"*("AverageNumberatRiskDuringInterval_Nt*" -"ProportionDyingDuringInterval_qt")))) OVER (ORDER BY "RowNumber" ASC)))) "StandardErrorLinear"
    
    , (1.96*(SQRT(("SurvivalProbability_St"*"SurvivalProbability_St") * (SUM(DIV0("ProportionDyingDuringInterval_qt", ("AverageNumberatRiskDuringInterval_Nt*"*("AverageNumberatRiskDuringInterval_Nt*" - "ProportionDyingDuringInterval_qt")))) OVER (ORDER BY "RowNumber" ASC))))) "MarginofErrorLinear"
    
    , LEAST(1, "SurvivalProbability_St" + (1.96*(SQRT(("SurvivalProbability_St"*"SurvivalProbability_St") * (SUM(DIV0("ProportionDyingDuringInterval_qt", ("AverageNumberatRiskDuringInterval_Nt*"*("AverageNumberatRiskDuringInterval_Nt*" - "ProportionDyingDuringInterval_qt")))) OVER (ORDER BY "RowNumber" ASC)))))) "UpperLimitLinear"  -- Linear Confidence Interval
    , GREATEST(0, "SurvivalProbability_St" - (1.96*(SQRT(("SurvivalProbability_St"*"SurvivalProbability_St") * (SUM(DIV0("ProportionDyingDuringInterval_qt", ("AverageNumberatRiskDuringInterval_Nt*"*("AverageNumberatRiskDuringInterval_Nt*" - "ProportionDyingDuringInterval_qt")))) OVER (ORDER BY "RowNumber" ASC)))))) "LowerLimitLinear"  -- Linear Confidence Interval
    

    , LN(-LN("SurvivalProbability_St")+0.0000000000001) AS "LogLogSurvival"
    , DIV0(SQRT("VarianceEstimate"), (LN("SurvivalProbability_St"))) AS "SE_LogLogSurvival"
    , LN(-LN("SurvivalProbability_St")+0.0000000000001) + 1.96 * (DIV0(SQRT("VarianceEstimate"), (LN("SurvivalProbability_St")))) AS "Upper_LogLog"
    , LN(-LN("SurvivalProbability_St")+0.0000000000001) - 1.96 * (DIV0(SQRT("VarianceEstimate"), (LN("SurvivalProbability_St")))) AS "Lower_LogLog"
    , EXP(-EXP("Upper_LogLog")) AS "UpperLimit"   -- LOG -LOG Transformed Confidence Interval
    , EXP(-EXP("Lower_LogLog")) AS "LowerLimit" -- LOG -LOG Transformed Confidence Interval
    , (EXP(-EXP("Upper_LogLog")) - EXP(-EXP("Lower_LogLog"))) / 2 AS "MarginofError"

FROM survival_probability
)

SELECT "RowNumber"
    , "IntervalinDays"
    , "NumberAtRiskDuringInterval_Nt"
    , "NumberofDeathsDuringInterval_Dt"
    , "LostToFollowUp_Ct"
    , "AverageNumberatRiskDuringInterval_Nt*" 
    ,  "ProportionDyingDuringInterval_qt"
    , "AmongThoseatRisk_ProportionSurvivingInterval_pt"
    , "SurvivalProbability_St"
    , "VarianceEstimate"
    , "StandardErrorLinear"
    , "MarginofErrorLinear"
    , "UpperLimitLinear"
    , "LowerLimitLinear"
    , "MarginofError"
    , "UpperLimit" -- Pointwise Confidence Interval Log Transformed
    , "LowerLimit" -- Pointwise Confidence Interval
FROM final_table
