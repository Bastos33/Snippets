WITH eventos AS (
    -- Normaliza eventos por cliente e mês
    SELECT
        customer_id,
        DATEFROMPARTS(YEAR(event_date), MONTH(event_date), 1) AS month_start
    FROM customer_events
    GROUP BY
        customer_id,
        DATEFROMPARTS(YEAR(event_date), MONTH(event_date), 1)
),

limites AS (
    -- Define intervalo temporal global
    SELECT
        MIN(month_start) AS min_month,
        MAX(month_start) AS max_month
    FROM eventos
),

calendar AS (
    -- Cria calendário mensal
    SELECT
        min_month AS month_start
    FROM limites

    UNION ALL

    SELECT
        DATEADD(MONTH, 1, month_start)
    FROM calendar c
    CROSS JOIN limites l
    WHERE c.month_start < l.max_month
),

clientes AS (
    -- Lista única de clientes
    SELECT DISTINCT customer_id
    FROM eventos
),

cliente_mes AS (
    -- Grade completa cliente × mês
    SELECT
        c.customer_id,
        cal.month_start
    FROM clientes c
    CROSS JOIN calendar cal
),

atividade AS (
    -- Marca atividade mensal
    SELECT
        cm.customer_id,
        cm.month_start,
        CASE
            WHEN e.month_start IS NOT NULL THEN 1
            ELSE 0
        END AS ativo
    FROM cliente_mes cm
    LEFT JOIN eventos e
           ON e.customer_id = cm.customer_id
          AND e.month_start = cm.month_start
),

churn AS (
    -- Detecta churn por transição de estado
    SELECT
        customer_id,
        month_start,
        ativo,
        CASE
            WHEN ativo = 0
             AND LAG(ativo) OVER (
                    PARTITION BY customer_id
                    ORDER BY month_start
                 ) = 1
            THEN 1
            ELSE 0
        END AS churn
    FROM atividade
)

SELECT *
FROM churn
ORDER BY customer_id, month_start
OPTION (MAXRECURSION 0);