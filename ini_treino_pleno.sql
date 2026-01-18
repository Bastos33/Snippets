WITH base AS (
    SELECT
        d.delivery_id,
        d.delivery_person_id,
        d.distribution_center_id,

        d.pickup_ready_at,
        d.out_for_delivery_at,
        d.delivered_at,

        -- tempos operacionais
        DATEDIFF(MINUTE, d.pickup_ready_at, d.out_for_delivery_at) AS tempo_espera_cd,
        DATEDIFF(MINUTE, d.out_for_delivery_at, d.delivered_at) AS tempo_rota,
        DATEDIFF(MINUTE, d.pickup_ready_at, d.delivered_at) AS sla_total_min,

        -- atributos temporais
        DATEPART(HOUR, d.pickup_ready_at) AS hora_pickup,
        DATEPART(WEEKDAY, d.pickup_ready_at) AS dia_semana
    FROM Deliveries d
),

analitico AS (
    SELECT
        *,

        -- violação de SLA (exemplo: SLA = 60 min)
        CASE
            WHEN sla_total_min > 60 THEN 1 ELSE 0
        END AS flag_sla_violation,

        -- retenção no CD
        CASE
            WHEN tempo_espera_cd > 30 THEN 1 ELSE 0
        END AS flag_retenção_cd,

        -- ordem temporal por motorista
        ROW_NUMBER() OVER (
            PARTITION BY delivery_person_id
            ORDER BY pickup_ready_at
        ) AS ordem_entrega_motorista,

        -- total de atrasos por motorista
        SUM(
            CASE WHEN sla_total_min > 60 THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY delivery_person_id
        ) AS total_atrasos_motorista,

        -- total de atrasos por CD
        SUM(
            CASE WHEN sla_total_min > 60 THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY distribution_center_id
        ) AS total_atrasos_cd,

        -- pior atraso por motorista
        MAX(sla_total_min) OVER (
            PARTITION BY delivery_person_id
        ) AS pior_atraso_motorista,

        -- pior atraso por CD
        MAX(sla_total_min) OVER (
            PARTITION BY distribution_center_id
        ) AS pior_atraso_cd,

        -- média móvel de atraso (últimas 7 entregas do motorista)
        AVG(sla_total_min) OVER (
            PARTITION BY delivery_person_id
            ORDER BY pickup_ready_at
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS media_movel_atraso_7,

        -- ranking de pior entrega por motorista
        DENSE_RANK() OVER (
            PARTITION BY delivery_person_id
            ORDER BY sla_total_min DESC
        ) AS ranking_pior_entrega_motorista

    FROM base
)

SELECT
    delivery_id,
    delivery_person_id,
    distribution_center_id,

    pickup_ready_at,
    out_for_delivery_at,
    delivered_at,

    tempo_espera_cd,
    tempo_rota,
    sla_total_min,

    flag_sla_violation,
    flag_retenção_cd,

    ordem_entrega_motorista,
    total_atrasos_motorista,
    total_atrasos_cd,

    pior_atraso_motorista,
    pior_atraso_cd,

    media_movel_atraso_7,
    ranking_pior_entrega_motorista,

    hora_pickup,
    dia_semana
FROM analitico
ORDER BY delivery_person_id, pickup_ready_at;