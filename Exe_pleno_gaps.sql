WITH base AS (
    SELECT
        terminal_id,
        check_time,
        status_code,
        latency_ms,

        LAG(latency_ms) OVER (
            PARTITION BY terminal_id
            ORDER BY check_time
        ) AS prev_latency,

        LAG(check_time) OVER (
            PARTITION BY terminal_id
            ORDER BY check_time
        ) AS prev_time

    FROM TerminalHealthLog
),
-- Para cada terminal, traz a leitura imediatamente anterior
-- (tempo e latência), permitindo comparações sequenciais

flags AS (
    SELECT
        *,

        CASE
            WHEN status_code <> 0 THEN 1
            WHEN prev_latency IS NOT NULL
                 AND latency_ms >= prev_latency * 1.3 THEN 1
            ELSE 0
        END AS instavel,

        CASE
            WHEN prev_time IS NULL THEN 1
            WHEN DATEDIFF(MINUTE, prev_time, check_time) > 5 THEN 1
            ELSE 0
        END AS quebra_tempo

    FROM base
),
-- Marca leituras instáveis:
-- • erro explícito (status_code)
-- • salto relevante de latência
-- Marca quebra quando há gap temporal > 5 minutos
-- ou quando não existe leitura anterior (primeiro registro)

marcacao AS (
    SELECT
        *,

        CASE
            WHEN quebra_tempo = 1 THEN 1
            WHEN instavel = 0 THEN 1
            ELSE 0
        END AS inicio_ilha

    FROM flags
),
-- Uma nova ilha começa quando:
-- • há quebra de tempo
-- • ou o terminal volta a ficar estável

grupos AS (
    SELECT
        *,

        SUM(inicio_ilha) OVER (
            PARTITION BY terminal_id
            ORDER BY check_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id

    FROM marcacao
),
-- Soma acumulada cria um identificador único por ilha
-- Cada incremento indica início de um novo período

agregado AS (
    SELECT
        terminal_id,
        grupo_id,

        MIN(check_time) AS inicio_periodo,
        MAX(check_time) AS fim_periodo,

        DATEDIFF(
            MINUTE,
            MIN(check_time),
            MAX(check_time)
        ) AS duracao_minutos,

        COUNT(*) AS qtd_leituras,
        SUM(instavel) AS qtd_instaveis,
        MAX(latency_ms) AS max_latency

    FROM grupos
    WHERE instavel = 1
    GROUP BY terminal_id, grupo_id
)
-- Consolida apenas períodos com leituras instáveis
-- Calcula duração, volume e pior latência

SELECT *
FROM agregado
WHERE duracao_minutos >= 10
  AND qtd_instaveis >= 2
ORDER BY terminal_id, inicio_periodo;
