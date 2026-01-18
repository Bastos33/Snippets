WITH base AS
(SELECT
        job_id,
        event_time,
        processing_time_ms,
        error_count,
        LAG(processing_time_ms) OVER (
            PARTITION BY job_id
            ORDER BY event_time
        ) AS prev_processing,

        LAG(event_time) OVER (
            PARTITION BY job_id
            ORDER BY event_time
        ) AS prev_time
        FROM JobProcessingLog
),
Ag AS(
      SELECT *,
             CASE
             WHEN processing_time_ms <= 2000
             AND error_count = 0
             THEN 1
             ELSE 0
             END AS flag_evento_normal,
            CASE 
            WHEN processing_time_ms > 2000
            OR ISNULL(error_count, 0) > 0
            THEN 1
            ELSE 0
            END AS flag_evento_degradado,
        CASE
            WHEN prev_time IS NOT NULL AND
            DATEDIFF(MINUTE, prev_time, event_time) <= 3 THEN 1
            ELSE 0
            END AS flag_evento_continuo,
        CASE 
           WHEN prev_time IS NOT NULL AND
            DATEDIFF(MINUTE, prev_time, event_time) > 3 THEN 1
            ELSE 0
            END AS flag_gap_temporal    
            FROM base
),
ini AS
(SELECT 
     *,
     CASE
    WHEN flag_evento_degradado = 1
     AND (
            prev_time IS NULL
         OR flag_evento_normal = 1
         OR flag_gap_temporal = 1
         -- OR LAG(flag_evento_degradado) = 0
         )
    THEN 1
    ELSE 0
END AS inicio_periodo_degradado
 FROM Ag
),

--), 
--AgPd AS
--(
--    SELECT CASE
--            WHEN  prev_time IS NULL 
--            OR  flag_evento_normal = 0
--            OR flag_gap_temporal = 0 THEN 1
--            ELSE 0
--        END AS flag_indicador_degradacao

ilha AS
(
    SELECT 
         *,
    SUM(inicio_periodo_degradado) OVER (
    PARTITION BY job_id
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS grupo_id
 FROM ini
),
aggf AS
(
SELECT      job_id,
            grupo_id,
        MIN(event_time) AS inicio_periodo,
        MAX(event_time) AS fim_periodo,
        DATEDIFF(
            MINUTE,
            MIN(event_time),
            MAX(event_time)
        ) AS duracao_minutos,

        SUM(error_count) AS total_erros,
        MAX(processing_time_ms) AS pior_ms
FROM ilha
GROUP BY job_id, grupo_id
)
SELECT *
FROM aggf
WHERE duracao_minutos >= 10
ORDER BY job_id, grupo_id;