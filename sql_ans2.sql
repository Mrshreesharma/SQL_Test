WITH rule_performance AS (
    -- Aggregate data per rule
    SELECT
        RULE,  -- Replace with the correct rule column name
        COUNT(*) AS TSFER_RULE,  -- Count of Transfers per Rule
        SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) AS TSFERS_FRAUD_RULE,  -- Count of fraud events per Rule
        COUNT(*) / SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) AS RULE_FR_RATE  -- Fraud detection rate
    FROM transactions
    GROUP BY RULE
),
total_fraud AS (
    -- Get total fraud events per score group (SCORE = 0 for fraud events)
    SELECT
        SCORE,
        SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) AS total_fraud
    FROM transactions
    GROUP BY SCORE
),
rule_with_pct AS (
    -- Add percentage of the fraud events per rule relative to total fraud events per score
    SELECT
        rp.RULE,
        rp.TSFER_RULE,
        rp.TSFERS_FRAUD_RULE,
        rp.RULE_FR_RATE,
        rp.TSFERS_FRAUD_RULE / NULLIF(tf.total_fraud, 0) * 100 AS RULE_AS_PCT_IN_SCORE,
        rp.TSFERS_FRAUD_RULE AS fraud_sum_for_rule,  -- Temporary column to compute next aggregation
        tf.total_fraud
    FROM rule_performance rp
    JOIN total_fraud tf ON tf.SCORE = 0  -- Joining to get total fraud count for SCORE = 0
),
rule_ranking AS (
    -- Rank rules by detection rate (highest to lowest)
    SELECT
        RULE,
        TSFER_RULE,
        TSFERS_FRAUD_RULE,
        RULE_FR_RATE,
        RULE_AS_PCT_IN_SCORE,
        fraud_sum_for_rule,
        total_fraud,
        RANK() OVER (ORDER BY RULE_FR_RATE DESC) AS rule_order_by_rate,
        RANK() OVER (ORDER BY fraud_sum_for_rule DESC) AS rule_order_by_fraud_volume
    FROM rule_with_pct
)
-- Final query to output all the requested columns in one table
SELECT
    RULE,
    TSFER_RULE,
    TSFERS_FRAUD_RULE,
    RULE_FR_RATE,
    RULE_AS_PCT_IN_SCORE,
    rule_order_by_rate,
    rule_order_by_fraud_volume,
    fraud_sum_for_rule / NULLIF(total_fraud, 0) * 100 AS RULE_AS_PCT_OF_TOTAL_FRAUD -- Percentage of fraud events in the rule as a % of total fraud
FROM rule_ranking
ORDER BY rule_order_by_rate, rule_order_by_fraud_volume;
