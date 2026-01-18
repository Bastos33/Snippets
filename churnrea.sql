WITH base AS (
    SELECT
        customer_id,
        DATEFROMPARTS(YEAR(event_date), MONTH(event_date), 1) AS month_start,
        COUNT(*) AS eventos_mes
    FROM CustomerFeatureUsage
    GROUP BY
        customer_id,
        DATEFROMPARTS(YEAR(event_date), MONTH(event_date), 1)
),

regra AS (
    SELECT
        *,
        LAG(eventos_mes) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
        ) AS eventos_mes_anterior
    FROM base
),

flag AS (
    SELECT
        *,
        CASE
            WHEN eventos_mes_anterior IS NOT NULL
             AND eventos_mes <= eventos_mes_anterior * 0.6
            THEN 1
            ELSE 0
        END AS flag_evento
    FROM regra
),

inicio AS (
    SELECT
        *,
        LAG(flag_evento) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
        ) AS flag_evento_anterior
    FROM flag
),

acumulado AS (
    SELECT
        *,
        CASE
            WHEN flag_evento = 1
             AND eventos_mes <= eventos_mes_anterior * 0.1
            THEN 1
            ELSE 0
        END AS flag_reativacao,

        -- acumulado REAL do período de churn
        SUM(
            CASE WHEN flag_evento = 1
            AND flag_evento_anterior = 0 THEN eventos_mes ELSE 0 END
        ) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS uso_acumulado_periodo
    FROM inicio
)

SELECT
    customer_id,
    month_start,
    eventos_mes,
    eventos_mes_anterior,
    flag_evento,
    inicio_periodo,
    uso_acumulado_periodo
FROM acumulado
ORDER BY customer_id, month_start;