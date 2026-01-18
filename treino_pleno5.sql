WITH base AS (
    SELECT
        delivery_id,
        driver_id,
        pickup_ready_at,
        out_for_delivery_at,
        delivered_at,
        city,

        -- SLA total
        DATEDIFF(MINUTE, pickup_ready_at, delivered_at) AS sla_total_min,

        -- pickup anterior
        LAG(pickup_ready_at) OVER (
            PARTITION BY driver_id
            ORDER BY pickup_ready_at
        ) AS prev_pickup_ready_at
    FROM DeliveryEvents
),

flags AS (
    SELECT
        *,

        -- gap operacional entre pickups
        DATEDIFF(
            MINUTE,
            prev_pickup_ready_at,
            pickup_ready_at
        ) AS gap_pickup_min,

        -- violação de SLA
        CASE
            WHEN sla_total_min > 35 THEN 1 ELSE 0
        END AS flag_sla_violation
    FROM base
),

inicio_periodo AS (
    SELECT
        *,

        CASE
            WHEN flag_sla_violation = 1
             AND (
                    prev_pickup_ready_at IS NULL       -- primeira viagem do motorista
                 OR gap_pickup_min > 30                -- GAP operacional
                 OR LAG(flag_sla_violation) OVER (
                        PARTITION BY driver_id
                        ORDER BY pickup_ready_at
                    ) = 0                               -- anterior não violado
             )
            THEN 1
            ELSE 0
        END AS inicio_periodo
    FROM flags
),

ilha AS (
    SELECT
        *,

        SUM(inicio_periodo) OVER (
            PARTITION BY driver_id
            ORDER BY pickup_ready_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ilha_periodo
    FROM inicio_periodo
),

analitico AS (
    SELECT
        driver_id,
        city,
        ilha_periodo,

        MIN(pickup_ready_at) AS inicio_periodo,
        MAX(pickup_ready_at) AS fim_periodo,
        COUNT(*) AS qnt_entregas,
        MAX(sla_total_min) AS pior_sla,
        SUM(sla_total_min) AS aumento_total_sla
    FROM ilha
    WHERE flag_sla_violation = 1
    GROUP BY
        driver_id,
        city,
        ilha_periodo
)

SELECT
    driver_id,
    city,
    inicio_periodo,
    fim_periodo,
    qnt_entregas,
    pior_sla,
    aumento_total_sla
FROM analitico
WHERE qnt_entregas >= 3
  AND aumento_total_sla > 15
ORDER BY driver_id, city;