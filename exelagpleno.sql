WITH base AS (
    SELECT
        customer_id,
        month_start,
        eventos_mes,
        LAG(eventos_mes) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
        ) AS eventos_mes_anterior
    FROM CustomerMonthlyUsage
),

flag AS (
    SELECT
        customer_id,
        month_start,
        eventos_mes,
        eventos_mes_anterior,
        CASE
            WHEN eventos_mes_anterior IS NOT NULL
             AND eventos_mes <= eventos_mes_anterior * 0.7
            THEN 1
            ELSE 0
        END AS flag_evento
    FROM base
),

inicio AS (
    SELECT
        *,
        CASE
            WHEN flag_evento = 1
             AND (
                  LAG(flag_evento) OVER (
                      PARTITION BY customer_id
                      ORDER BY month_start
                  ) = 0
                  OR LAG(flag_evento) OVER (
                      PARTITION BY customer_id
                      ORDER BY month_start
                  ) IS NULL
             )
            THEN 1
            ELSE 0
        END AS inicio_periodo
    FROM flag
),

acumulado AS (
    SELECT
        *,
        SUM(
            CASE WHEN flag_evento = 1 THEN eventos_mes ELSE 0 END
        ) OVER (
            PARTITION BY customer_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS eventos_periodo
    FROM inicio
)

SELECT
    customer_id,
    month_start,
    eventos_mes,
    eventos_mes_anterior,
    flag_evento,
    inicio_periodo,
    eventos_periodo
FROM acumulado
ORDER BY customer_id, month_start;