WITH base AS (
    SELECT
        job_id,
        event_time,
        processing_time_ms,
        error_count,

       --AVG(energy_kw) OVER (
       --    PARTITION BY machine_id
       --    ORDER BY reading_time
       --    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
       --) AS mov_energy,

        LAG(event_time) OVER (
            PARTITION BY job_id
            ORDER BY event_time
        ) AS prev_time
    FROM JobProcessingLog
),
flags AS (
    SELECT
        *,
        CASE
            WHEN  processing_time_ms > 2000 OR error_count > 0 
            THEN 1 ELSE 0   END AS periodo_degradado,

        CASE
            WHEN DATEDIFF(MINUTE, prev_time, event_time) > 3 THEN 1
            ELSE 0
        END AS gap_temporal,
        CASE
            WHEN error_count = 0 OR error_count IS NULL
            AND processing_time_ms <= 2000 
            THEN 1 
            ELSE 0
            END AS evento_normal

    FROM base
),
marcados AS (
    SELECT
        *,
        CASE
            WHEN periodo_degradado = 1
             AND gap_temporal = 0
             AND evento_normal = 0
             THEN 1
            ELSE 0
        END AS inicio_ilha_degradacao,
        
        CASE
            WHEN periodo_degradado = 0
             AND gap_temporal = 1
             AND evento_normal = 1
             THEN 1
            ELSE 0
        END AS fim_ilha_degradacao
    FROM flags
),
grupos AS (
    SELECT
        *,
        SUM(inicio_ilha_degradacao) OVER (
            PARTITION BY job_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id_inicio,
                SUM(fim_ilha_degradacao) OVER (
            PARTITION BY job_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id_fim

    FROM marcados
),
ilhas AS (
    SELECT
        job_id,
       -- grupo_id_inicio,
       -- gruo_id_fim,
        MIN(event_time) AS inicio_periodo,
        MAX(event_time) AS fim_periodo,
        DATEDIFF(MINUTE, MIN(event_time), MAX(event_time)) AS duracao_minutos,
        MAX(processing_time_ms) as pior_processing_time,
        COUNT(error_count) AS qtd_erros
    FROM grupos
     GROUP BY job_id
)
SELECT *
FROM ilhas
WHERE duracao_minutos >= 10
ORDER BY job_id, inicio;
------------------------------
--Modo correto
------------------------------
WITH base AS (
    SELECT
        job_id,
        event_time,
        processing_time_ms,
        ISNULL(error_count, 0) AS error_count,

        LAG(event_time) OVER (
            PARTITION BY job_id
            ORDER BY event_time
        ) AS prev_time,

        LAG(
            CASE 
                WHEN processing_time_ms > 2000 
                  OR ISNULL(error_count,0) > 0 
                THEN 1 ELSE 0 
            END
        ) OVER (
            PARTITION BY job_id
            ORDER BY event_time
        ) AS prev_degradado
    FROM JobProcessingLog
),

flags AS (
    SELECT
        *,
        CASE
            WHEN processing_time_ms > 2000
              OR error_count > 0
            THEN 1 ELSE 0
        END AS evento_degradado,

        CASE
            WHEN prev_time IS NOT NULL
             AND DATEDIFF(MINUTE, prev_time, event_time) > 3
            THEN 1 ELSE 0
        END AS gap_temporal
    FROM base
),

inicio_ilha AS (
    SELECT
        *,
        CASE
            WHEN evento_degradado = 1
             AND (
                    prev_time IS NULL
                 OR prev_degradado = 0
                 OR gap_temporal = 1
                 )
            THEN 1
            ELSE 0
        END AS inicio_periodo_degradado
    FROM flags
),

grupos AS (
    SELECT
        *,
        SUM(inicio_periodo_degradado) OVER (
            PARTITION BY job_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM inicio_ilha
),

agregado AS (
    SELECT
        job_id,
        grupo_id,
        MIN(event_time) AS inicio_periodo,
        MAX(event_time) AS fim_periodo,
        DATEDIFF(MINUTE, MIN(event_time), MAX(event_time)) AS duracao_minutos,
        MAX(processing_time_ms) AS pior_processing_time,
        SUM(error_count) AS total_erros
    FROM grupos
    WHERE evento_degradado = 1
    GROUP BY job_id, grupo_id
)

SELECT *
FROM agregado
WHERE duracao_minutos >= 10
ORDER BY job_id, inicio_periodo;
