df_status = spark2.read.jdbc(
    url=jdbc_url,
    table="""
    (
       WITH base AS (
        SELECT
            inn,
            cohort_date,
            report_date,
            m_diff,
            employees,
            LAG(employees) OVER (PARTITION BY inn, cohort_date ORDER BY report_date) AS emp_lag1,
            employees - LAG(employees) OVER (PARTITION BY inn, cohort_date ORDER BY report_date) AS delta
        FROM employee_cohort_nums
        WHERE m_diff BETWEEN 0 AND 36
        LIMIT 1000
    ),
    
    agg AS (
    SELECT
        inn,
        cohort_date,
    
        -- ===================== POINTS =====================
        MAX(CASE WHEN m_diff = 0 THEN employees END) AS stc_emp_cnt_last_m1,
        MAX(CASE WHEN m_diff = 12 THEN employees END) AS stc_emp_cnt_pt_m12,
        MAX(CASE WHEN m_diff = 6 THEN employees END) AS stc_emp_cnt_pt_m6,
        MAX(CASE WHEN m_diff = 18 THEN employees END) AS stc_emp_cnt_pt_m18,
        MAX(CASE WHEN m_diff = 24 THEN employees END) AS stc_emp_cnt_pt_m24,
        MAX(CASE WHEN m_diff = 36 THEN employees END) AS stc_emp_cnt_pt_m36,
    
        -- ===================== WINDOW STATS =====================
    
        -- M3
        AVG(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) AS stc_emp_cnt_avg_m3,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) AS stc_emp_cnt_min_m3,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) AS stc_emp_cnt_max_m3,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) AS stc_emp_cnt_std_m3,
    
        -- M6
        AVG(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) AS stc_emp_cnt_avg_m6,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) AS stc_emp_cnt_min_m6,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) AS stc_emp_cnt_max_m6,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) AS stc_emp_cnt_std_m6,
    
        -- M9
        AVG(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) AS stc_emp_cnt_avg_m9,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) AS stc_emp_cnt_min_m9,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) AS stc_emp_cnt_max_m9,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) AS stc_emp_cnt_std_m9,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END)
        - MIN(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) AS stc_emp_cnt_range_m9,
    
        -- M12
        AVG(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) AS stc_emp_cnt_avg_m12,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) AS stc_emp_cnt_min_m12,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) AS stc_emp_cnt_max_m12,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) AS stc_emp_cnt_std_m12,
        
    
        -- M18
        AVG(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) AS stc_emp_cnt_avg_m18,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) AS stc_emp_cnt_min_m18,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) AS stc_emp_cnt_max_m18,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) AS stc_emp_cnt_std_m18,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END)
        - MIN(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) AS stc_emp_cnt_range_m18,
    
        -- M24
        AVG(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) AS stc_emp_cnt_avg_m24,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) AS stc_emp_cnt_min_m24,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) AS stc_emp_cnt_max_m24,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) AS stc_emp_cnt_std_m24,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END)
- MIN(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) AS stc_emp_cnt_range_m24,
    
        -- M36
        AVG(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) AS stc_emp_cnt_avg_m36,
        MIN(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) AS stc_emp_cnt_min_m36,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) AS stc_emp_cnt_max_m36,
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) AS stc_emp_cnt_std_m36,
        (1.0/36) * SUM(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m36,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 35 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m36,
        MAX(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END)
- MIN(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) AS stc_emp_cnt_range_m36,

        (MAX(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) -
         MIN(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END)) AS stc_emp_cnt_range_m3,

        (MAX(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) -
         MIN(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END)) AS stc_emp_cnt_range_m6,
        
        CASE WHEN AVG(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) = 0
             THEN NULL
             ELSE STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) /
                  GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END), 1) END AS stc_emp_cnt_cv_m12,

        MAX(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END)
      - MIN(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) AS stc_emp_cnt_range_m12,

        (1.0/12) * SUM(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) /
            GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END), 1e-9)
            AS stc_emp_cnt_vol_adj_m12,

        AVG(employees) AS stc_emp_cnt_avg_ever,
        MIN(employees) AS stc_emp_cnt_min_ever,
        MAX(employees) AS stc_emp_cnt_max_ever,
        STDDEV_POP(employees) AS stc_emp_cnt_std_ever,
        -- M3
        SUM(CASE WHEN m_diff BETWEEN 0 AND 2 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m3,
        
        -- M6
        SUM(CASE WHEN m_diff BETWEEN 0 AND 5 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m6,
        
        -- M9
        SUM(CASE WHEN m_diff BETWEEN 0 AND 8 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m9,
        
        -- M12
        SUM(CASE WHEN m_diff BETWEEN 0 AND 11 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m12,
        
        -- M18
        SUM(CASE WHEN m_diff BETWEEN 0 AND 17 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m18,
        
        -- M24
        SUM(CASE WHEN m_diff BETWEEN 0 AND 23 AND delta = 0 THEN 1 ELSE 0 END) AS stc_emp_zero_growth_cnt_m24,
        


        -- M3
        (1.0/3) * SUM(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m3,
        
        -- M6
        (1.0/6) * SUM(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m6,
        
        -- M9
        (1.0/9) * SUM(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m9,
        
        -- M18
        (1.0/18) * SUM(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m18,
        
        -- M24
        (1.0/24) * SUM(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) /
        GREATEST(STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END), 1e-9) AS stc_emp_cnt_vol_adj_m24,
        
        -- M3
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END), 1) AS stc_emp_cnt_cv_m3,
        
        -- M6
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END), 1) AS stc_emp_cnt_cv_m6,
        
        -- M9
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END), 1) AS stc_emp_cnt_cv_m9,
        
        -- M18
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END), 1) AS stc_emp_cnt_cv_m18,
        
        -- M24
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END), 1) AS stc_emp_cnt_cv_m24,
        
        -- M36
        STDDEV_POP(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) /
        GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END), 1) AS stc_emp_cnt_cv_m36,
        
        -- EVER
        STDDEV_POP(employees) / GREATEST(AVG(employees), 1) AS stc_emp_cnt_cv_ever,

        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END, 0.5) AS stc_emp_cnt_median_m3,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END, 0.5) AS stc_emp_cnt_median_m6,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 8 THEN employees END, 0.5) AS stc_emp_cnt_median_m9,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END, 0.5) AS stc_emp_cnt_median_m12,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END, 0.5) AS stc_emp_cnt_median_m18,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END, 0.5) AS stc_emp_cnt_median_m24,
        percentile_approx(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END, 0.5) AS stc_emp_cnt_median_m36,
        percentile_approx(employees, 0.5) AS stc_emp_cnt_median_ever,

        COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 11 THEN 1 END) AS stc_emp_reported_months_cnt_m12,
        12 - COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 11 THEN 1 END) AS stc_emp_missing_months_cnt_m12,
        
        COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 23 THEN 1 END) AS stc_emp_reported_months_cnt_m24,
        24 - COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 23 THEN 1 END) AS stc_emp_missing_months_cnt_m24,
        
        COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 35 THEN 1 END) AS stc_emp_reported_months_cnt_m36,
        36 - COUNT(CASE WHEN employees IS NOT NULL AND m_diff BETWEEN 0 AND 35 THEN 1 END) AS stc_emp_missing_months_cnt_m36,

        COUNT(employees) AS stc_emp_reported_months_cnt_ever,
        (MAX(cohort_date) - MIN(cohort_date))/30 AS stc_emp_months_span_ever,
        MAX(cohort_date) - MIN(cohort_date) - COUNT(employees) AS stc_emp_max_gap_between_reports,
        DATEDIFF(MAX(cohort_date), cohort_date) AS stc_emp_months_since_last_report,
        SUM(CASE WHEN employees <= 0 THEN 1 ELSE 0 END) AS stc_emp_nonpositive_cnt_ever,
        
        SUM(CASE WHEN employees = 0 AND m_diff BETWEEN 0 AND 11 THEN 1 ELSE 0 END) AS stc_emp_zero_cnt_m12,
        SUM(CASE WHEN employees = 0 AND m_diff BETWEEN 0 AND 23 THEN 1 ELSE 0 END) AS stc_emp_zero_cnt_m24,
        SUM(CASE WHEN employees = 0 AND m_diff BETWEEN 0 AND 35 THEN 1 ELSE 0 END) AS stc_emp_zero_cnt_m36,
        SUM(CASE WHEN delta < 0 THEN 1 ELSE 0 END) AS stc_emp_max_consec_neg_growth_m12,
        SUM(CASE WHEN delta = 0 THEN 1 ELSE 0 END) AS stc_emp_max_consec_zero_growth_m12,
    
        STDDEV_POP(
            CASE WHEN emp_lag1 > 0 THEN delta / emp_lag1 ELSE NULL END
        ) AS stc_emp_mm_pct_std_m12,
    
        MAX(
            CASE WHEN delta < 0 THEN delta / GREATEST(emp_lag1, 1) ELSE 0 END
        ) AS stc_emp_max_mm_decrease_pct_m12,
    
        MAX(
            CASE WHEN delta > 0 THEN delta / GREATEST(emp_lag1, 1) ELSE 0 END
        ) AS stc_emp_max_mm_increase_pct_m12,

        (MAX(employees) - MIN(employees)) AS stc_emp_cnt_range_ever,

        (MAX(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END)) AS stc_emp_cnt_trend_m6,
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END)) / GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 5 THEN employees END),1) AS stc_emp_cnt_trend_norm_m6,
        
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END)) AS stc_emp_cnt_trend_m12,
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END)) / GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 11 THEN employees END),1) AS stc_emp_cnt_trend_norm_m12,
        
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END)) AS stc_emp_cnt_trend_m18,
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END)) / GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 17 THEN employees END),1) AS stc_emp_cnt_trend_norm_m18,
        
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END)) AS stc_emp_cnt_trend_m24,
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END)) / GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 23 THEN employees END),1) AS stc_emp_cnt_trend_norm_m24,
        
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END)) AS stc_emp_cnt_trend_m36,
        (MAX(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END) - MIN(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END)) / GREATEST(AVG(CASE WHEN m_diff BETWEEN 0 AND 35 THEN employees END),1) AS stc_emp_cnt_trend_norm_m36,
        
        (MAX(employees) - MIN(employees)) AS stc_emp_cnt_trend_ever,
        
        (AVG(CASE WHEN m_diff BETWEEN 0 AND 2 THEN employees END) + AVG(CASE WHEN m_diff BETWEEN 3 AND 5 THEN employees END)) / 2 AS stc_emp_cnt_avg_m3_6,
        (AVG(CASE WHEN m_diff BETWEEN 3 AND 5 THEN employees END) + AVG(CASE WHEN m_diff BETWEEN 6 AND 11 THEN employees END)) / 2 AS stc_emp_cnt_avg_m6_12,
        (AVG(CASE WHEN m_diff BETWEEN 6 AND 11 THEN employees END) + AVG(CASE WHEN m_diff BETWEEN 12 AND 23 THEN employees END)) / 2 AS stc_emp_cnt_avg_m12_24,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 2 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m3,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 2 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m3,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 5 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m6,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 5 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m6,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 8 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m9,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 8 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m9,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 11 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m12,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 11 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m12,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 17 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m18,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 17 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m18,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 23 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m24,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 23 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m24,
        
        SUM(CASE WHEN m_diff BETWEEN 0 AND 35 AND delta > 0 THEN 1 ELSE 0 END) AS stc_emp_pos_growth_cnt_m36,
        SUM(CASE WHEN m_diff BETWEEN 0 AND 35 AND delta < 0 THEN 1 ELSE 0 END) AS stc_emp_neg_growth_cnt_m36


    
    FROM base
    GROUP BY inn, cohort_date
    )
    
    SELECT
        *,
        ROUND(stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_pt_m12,1),0) AS stc_emp_cnt_yoy_ratio,
        ROUND((stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m12) / GREATEST(stc_emp_cnt_pt_m12,1),0) AS stc_emp_cnt_yoy_pct,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_avg_m12,1) AS stc_emp_last_to_avg_ratio_m12,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_max_m12,1) AS stc_emp_last_to_max_ratio_m12,
        stc_emp_cnt_min_m12 / GREATEST(stc_emp_cnt_avg_m12,1) AS stc_emp_min_to_avg_ratio_m12,
        stc_emp_cnt_max_m12 / GREATEST(stc_emp_cnt_avg_m12,1) AS stc_emp_max_to_avg_ratio_m12,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_avg_m6,1) AS stc_emp_last_to_avg_ratio_m6,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_max_m6,1) AS stc_emp_last_to_max_ratio_m6,
        stc_emp_cnt_min_m6 / GREATEST(stc_emp_cnt_avg_m6,1) AS stc_emp_min_to_avg_ratio_m6,
        stc_emp_cnt_max_m6 / GREATEST(stc_emp_cnt_avg_m6,1) AS stc_emp_max_to_avg_ratio_m6,
        
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_avg_m24,1) AS stc_emp_last_to_avg_ratio_m24,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_max_m24,1) AS stc_emp_last_to_max_ratio_m24,
        stc_emp_cnt_min_m24 / GREATEST(stc_emp_cnt_avg_m24,1) AS stc_emp_min_to_avg_ratio_m24,
        stc_emp_cnt_max_m24 / GREATEST(stc_emp_cnt_avg_m24,1) AS stc_emp_max_to_avg_ratio_m24,
        
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_avg_m36,1) AS stc_emp_last_to_avg_ratio_m36,
        stc_emp_cnt_last_m1 / GREATEST(stc_emp_cnt_max_m36,1) AS stc_emp_last_to_max_ratio_m36,
        stc_emp_cnt_min_m36 / GREATEST(stc_emp_cnt_avg_m36,1) AS stc_emp_min_to_avg_ratio_m36,
        stc_emp_cnt_max_m36 / GREATEST(stc_emp_cnt_avg_m36,1) AS stc_emp_max_to_avg_ratio_m36,

        CASE WHEN stc_emp_cnt_last_m1 <= 5 THEN 1 ELSE 0 END AS stc_emp_size_micro_flag_last,
        CASE WHEN stc_emp_cnt_last_m1 BETWEEN 6 AND 15 THEN 1 ELSE 0 END AS stc_emp_size_small_flag_last,
        CASE WHEN stc_emp_cnt_last_m1 BETWEEN 16 AND 50 THEN 1 ELSE 0 END AS stc_emp_size_medium_flag_last,
        CASE WHEN stc_emp_cnt_last_m1 > 50 THEN 1 ELSE 0 END AS stc_emp_size_large_flag_last,
        
        CASE WHEN stc_emp_cnt_avg_m12 <= 5 THEN 1 ELSE 0 END AS stc_emp_size_micro_flag_avg_m12,
        CASE WHEN stc_emp_cnt_avg_m12 BETWEEN 6 AND 15 THEN 1 ELSE 0 END AS stc_emp_size_small_flag_avg_m12,
        CASE WHEN stc_emp_cnt_avg_m12 BETWEEN 16 AND 50 THEN 1 ELSE 0 END AS stc_emp_size_medium_flag_avg_m12,
        CASE WHEN stc_emp_cnt_avg_m12 > 50 THEN 1 ELSE 0 END AS stc_emp_size_large_flag_avg_m12,
        (stc_emp_cnt_last_m1 - LAG(stc_emp_cnt_last_m1) OVER (PARTITION BY inn, cohort_date)) AS stc_emp_cnt_mm_change,
        (stc_emp_cnt_last_m1 - LAG(stc_emp_cnt_last_m1) OVER (PARTITION BY inn, cohort_date )) / GREATEST(LAG(stc_emp_cnt_last_m1) OVER (PARTITION BY inn, cohort_date),1) AS stc_emp_cnt_mm_pct,
        
        (stc_emp_cnt_last_m1 - LAG(stc_emp_cnt_last_m1,3) OVER (PARTITION BY inn, cohort_date)) AS stc_emp_cnt_qoq_change,
        (stc_emp_cnt_last_m1 - LAG(stc_emp_cnt_last_m1,3) OVER (PARTITION BY inn, cohort_date)) / GREATEST(LAG(stc_emp_cnt_last_m1,3) OVER (PARTITION BY inn, cohort_date ),1) AS stc_emp_cnt_qoq_pct,
        
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m12) AS stc_emp_cnt_yoy_change,

        
        stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m12 AS stc_emp_cnt_change_12m,
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m12) / GREATEST(stc_emp_cnt_pt_m12,1) AS stc_emp_cnt_pct_change_12m,
        stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m36 AS stc_emp_cnt_change_36m,
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m36) / GREATEST(stc_emp_cnt_pt_m36,1) AS stc_emp_cnt_pct_change_36m,
        
        stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m24 AS stc_emp_cnt_change_24m,
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m24) / GREATEST(stc_emp_cnt_pt_m24,1) AS stc_emp_cnt_pct_change_24m,
        
        stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m18 AS stc_emp_cnt_change_18m,
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m18) / GREATEST(stc_emp_cnt_pt_m18,1) AS stc_emp_cnt_pct_change_18m,
        
        stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m6 AS stc_emp_cnt_change_6m,
        (stc_emp_cnt_last_m1 - stc_emp_cnt_pt_m6) / GREATEST(stc_emp_cnt_pt_m6,1) AS stc_emp_cnt_pct_change_6m


    FROM agg

    ) AS t
    """,
    properties=properties
)
