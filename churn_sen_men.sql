WITH limites_cliente AS (
    SELECT
        customer_id,
        DATEFROMPARTS(YEAR(MIN(event_time)), MONTH(MIN(event_time)), 1) AS mes_inicio,
        DATEFROMPARTS(YEAR(MAX(event_time)), MONTH(MAX(event_time)), 1) AS mes_fim
    FROM CustomerEvents
    GROUP BY customer_id
),calendario_cliente AS (
    SELECT
        customer_id,
        mes_inicio AS month_start,
        mes_fim
    FROM limites_cliente

    UNION ALL

    SELECT
        customer_id,
        DATEADD(MONTH, 1, month_start),
        mes_fim
    FROM calendario_cliente
    WHERE month_start < mes_fim
)