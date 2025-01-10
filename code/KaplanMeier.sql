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
    -- AND t1.sku = '817R2806A20437CF'
    -- AND t1.sold_date IS NOT NULL
    -- AND t1.sku = '503H2465L81864YF'
    -- AND t1.sku = '399E7172S63532UZ'
    -- ORDER BY t3.cal_day DESC
)

select * from data_model;

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
SELECT aging_days
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
SELECT aging_days
    , COUNT(DISTINCT product_id) censored_sku_count
FROM censored_items
GROUP BY ALL 
)

, alive_at_beginning AS 
(
SELECT COUNT(DISTINCT product_id) alive_at_beginning_of_period
FROM data_model
)

, observations AS 
(
SELECT DISTINCT t1.aging_days
, COALESCE(t2.sold_sku_count, 0) sold_skus
, COALESCE(t3.censored_sku_count, 0) censored_skus
, t4.alive_at_beginning_of_period

FROM data_model t1
LEFT JOIN sold_items_count t2
    ON t1.aging_days = t2.aging_days
LEFT JOIN censored_items_count t3
    ON t1.aging_days = t3.aging_days
-- CROSS JOIN alive_at_beginning t4 
LEFT JOIN alive_at_beginning t4
    ON t1.aging_days = 0
ORDER BY aging_days
)

, indexing AS 
(
SELECT *
    , ROW_NUMBER() OVER (ORDER BY aging_days ASC) row_number
FROM observations
)

, ordered_intervals AS -- Recursive CTE
(
 SELECT row_number
    , aging_days
    , sold_skus Number_of_Deaths_Dt
    , censored_skus Number_Censored_Ct
    , alive_at_beginning_of_period Number_At_Risk_Nt
 FROM indexing 
 WHERE 1 = 1
     AND row_number = 1
     
UNION ALL 

SELECT t1.row_number
    , t1.aging_days
    , t1.sold_skus Number_of_Deaths_Dt
    , t1.censored_skus Number_Censored_Ct
    , (t2.Number_At_Risk_Nt - (t2.Number_of_Deaths_Dt + t2.Number_Censored_Ct)) Number_At_Risk_Nt
FROM indexing t1
INNER JOIN ordered_intervals t2
    ON t1.row_number = t2.row_number + 1
)


, survival_probabilities AS -- recursive CTE
(
SELECT aging_days
    , Number_At_Risk_Nt
    , Number_of_Deaths_Dt
    , Number_Censored_Ct
    , 1.0 survival_probability_St
    , row_number
FROM ordered_intervals
WHERE 1 = 1
    AND row_number = 1

UNION ALL

SELECT oi.aging_days
    , oi.Number_At_Risk_Nt
    , oi.Number_of_Deaths_Dt
    , oi.Number_Censored_Ct
    , rcte.survival_probability_St * ((oi.Number_At_Risk_Nt - oi.Number_of_Deaths_Dt) / (oi.Number_At_Risk_Nt + 1E-10)) survival_probability_St -- Added a small number to prevent division by zero
    , oi.row_number
FROM ordered_intervals oi
INNER JOIN survival_probabilities rcte
    ON oi.row_number = rcte.row_number + 1
)

SELECT row_number "RowNumber"
    , aging_days "AgingDays"
    , Number_At_Risk_Nt "Number_At_Risk_Nt"
    , Number_of_Deaths_Dt "Number_of_Deaths_Dt"
    , Number_Censored_Ct "Number_Censored_Ct"
    , survival_probability_St "Survival_Probability_St" -- Survival Function Estimate Kaplan Meier
    , (survival_probability_St*survival_probability_St) * (SUM(DIV0(Number_of_Deaths_Dt, (Number_At_Risk_Nt*(Number_At_Risk_Nt - Number_of_Deaths_Dt)))) OVER (ORDER BY row_number ASC)) "GreenwoodVarianceEstimationforKaplanMeier"
    
    
    , (SQRT((survival_probability_St*survival_probability_St) * (SUM(DIV0(Number_of_Deaths_Dt, (Number_At_Risk_Nt*(Number_At_Risk_Nt - Number_of_Deaths_Dt)))) OVER (ORDER BY row_number ASC)))) "StandardErrorLinear"
    
    , (1.96*(SQRT((survival_probability_St*survival_probability_St) * (SUM(DIV0(Number_of_Deaths_Dt, (Number_At_Risk_Nt*(Number_At_Risk_Nt - Number_of_Deaths_Dt)))) OVER (ORDER BY row_number ASC))))) "MarginofErrorLinear"
    
    , LEAST(1, survival_probability_St + (1.96*(SQRT((survival_probability_St*survival_probability_St) * (SUM(DIV0(Number_of_Deaths_Dt, (Number_At_Risk_Nt*(Number_At_Risk_Nt - Number_of_Deaths_Dt)))) OVER (ORDER BY row_number ASC)))))) "UpperLimitLinear"  -- Linear Confidence Interval
    , GREATEST(0, survival_probability_St - (1.96*(SQRT((survival_probability_St*survival_probability_St) * (SUM(DIV0(Number_of_Deaths_Dt, (Number_At_Risk_Nt*(Number_At_Risk_Nt - Number_of_Deaths_Dt)))) OVER (ORDER BY row_number ASC)))))) "LowerLimitLinear"  -- Linear Confidence Interval
    

    , LN(-LN(survival_probability_St+0.00000001)+0.00000001) "LogLogSurvival"
    , DIV0(SQRT("GreenwoodVarianceEstimationforKaplanMeier"), (LN(survival_probability_St+0.00000001))) "SE_LogLogSurvival"
    , LN(-LN(survival_probability_St+0.00000001)+0.00000001) + 1.96 * (DIV0(SQRT("GreenwoodVarianceEstimationforKaplanMeier"), (LN(survival_probability_St+0.00000001)))) "Upper_LogLog"
    , LN(-LN(survival_probability_St+0.00000001)+0.00000001) - 1.96 * (DIV0(SQRT("GreenwoodVarianceEstimationforKaplanMeier"), (LN(survival_probability_St+0.00000001)))) "Lower_LogLog"
    , EXP(-EXP("Upper_LogLog")) "UpperLimit"   -- LOG -LOG Transformed Confidence Interval
    , EXP(-EXP("Lower_LogLog")) "LowerLimit" -- LOG -LOG Transformed Confidence Interval
    , CASE 
        WHEN aging_days = 0 THEN 0 
        ELSE (EXP(-EXP("Upper_LogLog")) - EXP(-EXP("Lower_LogLog"))) / 2 
      END "MarginofError"

    , SUM(DIV0(Number_of_Deaths_Dt, Number_At_Risk_Nt)) OVER (ORDER BY row_number ASC) "CumulativeHazard"  -- Nelson Aalen Estimator for Cumulative Hazard Rate Function

    , SUM(DIV0(((Number_At_Risk_Nt - Number_of_Deaths_Dt)*Number_of_Deaths_Dt), ((Number_At_Risk_Nt - 1) *(Number_At_Risk_Nt*Number_At_Risk_Nt)))) OVER (ORDER BY row_number ASC) "VarianceofNelsonAalenEstimator"

    , "CumulativeHazard" + 1.96*SQRT("VarianceofNelsonAalenEstimator") "UpperLimitLargeSample" -- For large sample size
    , "CumulativeHazard" - 1.96*SQRT("VarianceofNelsonAalenEstimator") "LowerLimitLargeSample" -- For large sample size
    
    , "CumulativeHazard" * EXP((1.96*DIV0(SQRT("VarianceofNelsonAalenEstimator"), "CumulativeHazard"))) "LogtransformedUpperLimit"  -- For Nelson Aalen Hazard Function
    , "CumulativeHazard" * EXP((-1.96*DIV0(SQRT("VarianceofNelsonAalenEstimator"), "CumulativeHazard"))) "LogtransformedLowerLimit"  -- For Nelson Aalen Hazard Function
    
    , EXP(-(SUM(DIV0(Number_of_Deaths_Dt, Number_At_Risk_Nt)) OVER (ORDER BY row_number ASC))) "SurvivalFunctionEstimateNelsonAalen"
FROM survival_probabilities
;


