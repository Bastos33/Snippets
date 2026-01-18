WITH eventos_ordenados AS (
    SELECT
        customer_id,
        event_date,
        LAG(event_date) OVER (
            PARTITION BY customer_id
            ORDER BY event_date
        ) AS evento_anterior
    FROM CustomerDailyEvents
),

intervalos AS (
    SELECT
        customer_id,
        evento_anterior AS data_inicio_inatividade,
        event_date AS data_retorno,
        DATEDIFF(DAY, evento_anterior, event_date) AS dias_sem_atividade
    FROM eventos_ordenados
    WHERE evento_anterior IS NOT NULL
),

churn_detectado AS (
    SELECT
        customer_id,
        data_inicio_inatividade,
        DATEADD(DAY, 30, data_inicio_inatividade) AS data_churn,
        dias_sem_atividade,
        1 AS flag_churn,
        CASE
            WHEN dias_sem_atividade > 30 THEN 1
            ELSE 0
        END AS flag_reativado
    FROM intervalos
    WHERE dias_sem_atividade >= 30
)

SELECT
    customer_id,
    data_inicio_inatividade,
    data_churn,
    dias_sem_atividade,
    flag_churn,
    flag_reativado
FROM churn_detectado
ORDER BY customer_id, data_churn;
