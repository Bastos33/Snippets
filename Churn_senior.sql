WITH ultima_compra AS (
    SELECT
        customer_id,
        MAX(event_time) AS ultima_data_compra
    FROM CustomerEvents
--    WHERE event_type = 'purchase'
    GROUP BY customer_id
),
ref_date AS (
    SELECT
        EOMONTH(MAX(event_time)) AS data_referencia
    FROM CustomerEvents
), churn AS
(
SELECT
    u.customer_id,
    u.ultima_data_compra,
    DATEDIFF(
        DAY,
        u.ultima_data_compra,
        r.data_referencia
    ) AS dias_sem_compra
FROM ultima_compra u
CROSS JOIN ref_date r
)
 SELECT  customer_id,
          ultima_data_compra,
		  dias_sem_compra,
		  CASE WHEN dias_sem_compra > 30
		  THEN 1
		  ELSE 0
		  END AS flag_churn
FROM churn
