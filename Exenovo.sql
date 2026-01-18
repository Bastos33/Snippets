WITH base AS (
    SELECT
        seller_id,
        sale_date,
        total_orders,
        total_revenue,

        AVG(total_orders) OVER (
            PARTITION BY seller_id
            ORDER BY sale_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS avg_orders_7d,

        AVG(total_revenue) OVER (
            PARTITION BY seller_id
            ORDER BY sale_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS avg_revenue_7d,

        LAG(sale_date) OVER (
            PARTITION BY seller_id
            ORDER BY sale_date
        ) AS prev_date
    FROM SellerDailyPerformance
),

flags AS (
    SELECT
        *,
        CASE
            WHEN avg_orders_7d IS NOT NULL
             AND (
                    total_orders  < avg_orders_7d  * 0.60
                 OR total_revenue < avg_revenue_7d * 0.50
             )
            THEN 1 ELSE 0
        END AS flag_queda,

        CASE
            WHEN prev_date IS NOT NULL
             AND DATEDIFF(DAY, prev_date, sale_date) = 1
            THEN 1 ELSE 0
        END AS flag_continuo
    FROM base
),

inicio AS (
    SELECT
        *,
        CASE
            WHEN flag_queda = 1
             AND (
                    prev_date IS NULL
                 OR flag_continuo = 0
                 OR LAG(flag_queda) OVER (
                        PARTITION BY seller_id
                        ORDER BY sale_date
                   ) = 0
             )
            THEN 1 ELSE 0
        END AS inicio_periodo
    FROM flags
),

ilhas AS (
    SELECT
        *,
        SUM(inicio_periodo) OVER (
            PARTITION BY seller_id
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM inicio
),

agregado AS (
    SELECT
        seller_id,
        grupo_id,

        MIN(sale_date) AS inicio_queda,
        MAX(sale_date) AS fim_queda,

        COUNT(*) AS duracao_dias,

        MIN(total_orders)  AS pior_dia_orders,
        MIN(total_revenue) AS pior_dia_revenue,

        AVG(avg_orders_7d)  AS media_orders_baseline,
        AVG(avg_revenue_7d) AS media_revenue_baseline
    FROM ilhas
    WHERE flag_queda = 1
    GROUP BY seller_id, grupo_id
)

SELECT *
FROM agregado
WHERE duracao_dias >= 3
ORDER BY seller_id, inicio_queda;