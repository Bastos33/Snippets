WITH base AS
(SELECT 
        customer_id,
        event_time,
        LAG(event_time)
        OVER(PARTITION BY customer_id
        ORDER BY event_time) as prev_event_time
        FROM CustomerEvents
),

intervalos AS (
    SELECT
        customer_id,
        prev_event_time AS inicio_inatividade,
        event_time AS evento_atual,
        DATEDIFF(DAY, prev_event_time, event_time) AS dias_sem_atividade
    FROM base
    WHERE prev_event_time IS NOT NULL
), churn AS (
    SELECT
        customer_id,
        inicio_inatividade,
        evento_atual,
        dias_sem_atividade,

        CASE
            WHEN dias_sem_atividade >= 30 THEN 1
            ELSE 0
        END AS flag_churn
    FROM intervalos
)
, reativacao AS (
    SELECT
        customer_id,
        inicio_inatividade,
        evento_atual,
        dias_sem_atividade,
        flag_churn,

        LEAD(evento_atual) OVER (
            PARTITION BY customer_id
            ORDER BY evento_atual
        ) AS proximo_evento
    FROM churn
)
SELECT
    customer_id,
    inicio_inatividade,
    evento_atual AS evento_churn,
    DATEADD(DAY, 30, inicio_inatividade) AS data_churn,
    dias_sem_atividade,
    flag_churn,

    CASE
        WHEN flag_churn = 1
         AND proximo_evento IS NOT NULL
        THEN 1
        ELSE 0
    END AS flag_reativado,

    proximo_evento AS data_reativacao
FROM reativacao
WHERE flag_churn = 1
ORDER BY customer_id, evento_churn;