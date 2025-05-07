DELIMITER $$

CREATE PROCEDURE IF NOT EXISTS get_top_fraud_rules_by_score(IN score INT, IN N INT)
BEGIN
    -- Declare a temporary table to hold the fraud detection data per rule
    CREATE TEMPORARY TABLE temp_rule_fraud_data AS
    SELECT
        TSFER_RULE,  -- Rule ID or Name
        COUNT(*) AS total_transfers,  -- Count of transfers per rule
        SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) AS fraud_events,  -- Fraud events per rule (assuming SCORE = 0 indicates fraud)
        CASE WHEN COUNT(*) > 0 THEN 
            (SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100 
        ELSE 0 END AS fraud_rate_percentage  -- Fraud rate (percentage)
    FROM transactions
    WHERE SCORE = score  -- Filter by the provided score
    GROUP BY TSFER_RULE;  -- Group by rule

    -- Select the top N records based on the highest fraud_rate_percentage
    SELECT 
        TSFER_RULE,
        total_transfers,
        fraud_events,
        fraud_rate_percentage
    FROM temp_rule_fraud_data
    ORDER BY fraud_rate_percentage DESC  -- Highest fraud rate first
    LIMIT N;  -- Limit the results to N records

    -- Clean up the temporary table
    DROP TEMPORARY TABLE IF EXISTS temp_rule_fraud_data;
END $$

DELIMITER ;
