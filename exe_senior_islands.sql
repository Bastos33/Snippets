WITH base AS (
    SELECT
        machine_id,
        reading_time,
        energy_kw,

        AVG(energy_kw) OVER (
            PARTITION BY machine_id
            ORDER BY reading_time
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) AS mov_energy,

        LAG(reading_time) OVER (
            PARTITION BY machine_id
            ORDER BY reading_time
        ) AS prev_time
    FROM EnergyReadings
),
flags AS (
    SELECT
        *,
        CASE
            WHEN mov_energy IS NOT NULL
                 AND energy_kw > mov_energy * 1.25
            THEN 1 ELSE 0
        END AS consumo_anomalo,

        CASE
            WHEN prev_time IS NULL THEN 1
            WHEN DATEDIFF(MINUTE, prev_time, reading_time) > 2 THEN 1
            ELSE 0
        END AS gap_tempo
    FROM base
),
marcados AS (
    SELECT
        *,
        CASE
            WHEN gap_tempo = 1 THEN 1
            WHEN consumo_anomalo = 0 THEN 1
            ELSE 0
        END AS inicio_ilha
    FROM flags
),
grupos AS (
    SELECT
        *,
        SUM(inicio_ilha) OVER (
            PARTITION BY machine_id
            ORDER BY reading_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM marcados
),
ilhas AS (
    SELECT
        machine_id,
        grupo_id,
        MIN(reading_time) AS inicio,
        MAX(reading_time) AS fim,
        DATEDIFF(MINUTE, MIN(reading_time), MAX(reading_time)) AS duracao,
        COUNT(*) AS qtd_leituras
    FROM grupos
    WHERE consumo_anomalo = 1
    GROUP BY machine_id, grupo_id
)
SELECT *
FROM ilhas
WHERE duracao >= 10
ORDER BY machine_id, inicio;
