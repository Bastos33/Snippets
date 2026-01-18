WITH base AS (
    SELECT
        customer_id,
        DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS month_start,
        feature_name
    FROM UsageEvents
    WHERE feature_name IN ('export', 'upload')
),

-- considera SOMENTE eventos de valor (export)
eventos_mensais AS (
    SELECT
        customer_id,
        month_start,
        COUNT(*) AS eventos_mes
    FROM base
    WHERE feature_name = 'export'
    GROUP BY customer_id, month_start
),

historico AS (
    SELECT
        customer_id,
        month_start,
        eventos_mes,
        LAG(eventos_mes) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
        ) AS eventos_mes_anterior
    FROM eventos_mensais
)

SELECT
    customer_id,
    month_start,
    eventos_mes,
    eventos_mes_anterior,

    -- churn: queda >= 70%
    CASE
        WHEN eventos_mes_anterior > 0
         AND eventos_mes <= eventos_mes_anterior * 0.30
        THEN 1
        ELSE 0
    END AS flag_churn,

    -- reativação: recuperação >= 80%
    CASE
        WHEN eventos_mes_anterior > 0
         AND eventos_mes >= eventos_mes_anterior * 0.80
        THEN 1
        ELSE 0
    END AS flag_reativado

FROM historico
ORDER BY customer_id, month_start;
