WITH weekly_data AS (
    SELECT
        DATE_FORMAT(STR_TO_DATE(TRANSACTION_DATE, '%m/%d/%Y'), '%Y-%u') AS week,
        COUNT(*) AS total_transfers,
        SUM(CASE WHEN SCORE = 0 THEN 1 ELSE 0 END) AS fraud_events
    FROM transactions
    GROUP BY week
),
weekly_rate AS (
    SELECT
        week,  -- fixed from "weeks" to "week"
        total_transfers,
        fraud_events,
        CASE 
            WHEN total_transfers > 0 THEN (fraud_events / total_transfers) * 100 
            ELSE 0 
        END AS weekly_fraud_rate
    FROM weekly_data
),
weekly_changes AS (
    SELECT
        week,
        total_transfers,
        fraud_events,
        weekly_fraud_rate,
        LAG(fraud_events) OVER (ORDER BY week) AS prev_fraud_events,
        LAG(weekly_fraud_rate) OVER (ORDER BY week) AS prev_fraud_rate
    FROM weekly_rate
)
SELECT
    week,
    total_transfers,
    fraud_events,
    weekly_fraud_rate,
    CASE 
        WHEN prev_fraud_events IS NULL THEN NULL
        ELSE (fraud_events - prev_fraud_events) / prev_fraud_events * 100
    END AS fraud_transfer_pct_change,
    CASE
        WHEN prev_fraud_rate IS NULL THEN NULL
        ELSE (weekly_fraud_rate - prev_fraud_rate) / prev_fraud_rate * 100
    END AS fraud_rate_pct_change
FROM weekly_changes
ORDER BY week;

