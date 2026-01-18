WITH base AS (
    SELECT
        device_id,
        event_time,
        temp,
        pressure,

        -- Média móvel das 3 leituras anteriores de temperatura (ROWS = quantidade, não tempo)
        AVG(temp) OVER (
            PARTITION BY device_id
            ORDER BY event_time
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS mov_temp,

        -- Média móvel das 4 leituras anteriores de pressão
        AVG(pressure) OVER (
            PARTITION BY device_id
            ORDER BY event_time
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS mov_press,

        -- Última leitura de tempo
        LAG(event_time) OVER (
            PARTITION BY device_id ORDER BY event_time
        ) AS prev_time

    FROM DeviceLog
),--Prepara a base trazendo a última leitura de tempo, leitura de temperatura e pressão atuais 
-- as 3 leituras anteriores de temperatura e as 4 leituras de pressão anteriores.
--Isso permite verificar a regularidade de ambos os sinais.

--Versão sênior:
--Aqui construímos a linha do tempo analítica.
--Nada de regra de negócio ainda — apenas contexto temporal e estatístico mínimo para avaliar estabilidade.
flags AS (
    SELECT
        *,
        -- Temp irregular se desvio > 15%
        CASE 
            WHEN mov_temp IS NOT NULL 
                 AND ABS(temp - mov_temp) > mov_temp * 0.15
            THEN 1 ELSE 0
        END AS temp_irregular,

        -- Pressão irregular se desvio > 10%
        CASE 
            WHEN mov_press IS NOT NULL
                 AND ABS(pressure - mov_press) > mov_press * 0.10
            THEN 1 ELSE 0
        END AS press_irregular,

        -- GAP temporal (sem leitura > 90 segundos)
        CASE 
            WHEN prev_time IS NULL THEN 1
            WHEN DATEDIFF(SECOND, prev_time, event_time) > 90 THEN 1
            ELSE 0
        END AS gap

    FROM base
),--Se existe um registro anterior a leitura atual e a temperatura variou mais de 15% em relação as 3 leituras
--anteriores, marca-se 1 para temperatura irregular.
--Se existe registro anterior em relação a leitura de pressão atual e a leitura atual é 10% maior em 
--relação as 4 anteriores, marcamos 1 para instabilidade de pressão.
--Se não há leitura anterior marca-se 1 e se há uma diferença de 90 segundos entre as leituras simboliza 
--que houve interrupção na ilha. Consolida os eventos relevantes a continuidade da ilha.

--Versão sênior:
--Esta etapa transforma números em decisões.
--A partir daqui, cada linha já “opina” se representa risco ou continuidade saudável.
marcados AS (
    SELECT
        *,
        -- Início de uma nova ilha:
        -- qualquer GAP quebra a sequência,
        -- ou quando nenhuma irregularidade está ativa
        CASE
            WHEN gap = 1 THEN 1
            WHEN temp_irregular = 0 AND press_irregular = 0 THEN 1
            ELSE 0
        END AS inicio_ilha
    FROM flags
),--Aqui separamos as novas ilhas, as que interromperam a sequência ou aquelas que não se enquadraram nas
--métricas de uma ilha irregular.

--Versão sênior:
--Uma ilha só existe enquanto o problema persiste sem interrupção.
--Se o sinal volta ao normal ou há ausência de dados, a ilha termina — mesmo que volte depois.
--Esse ponto é onde gaps x islands realmente acontece, não na soma.
grupos AS (
    SELECT
        *,
        -- Running sum: identifica cada ilha por device
        SUM(inicio_ilha) OVER (
            PARTITION BY device_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM marcados
),--Identificamos desde o ínicio até o momento atual ilhas por dispositivos.

--Versão sênior:
--Este padrão é canônico em SQL analítico.
--Não “agrupa” dados — cria uma chave semântica de continuidade.
duracao AS (
    SELECT
        device_id,
        grupo_id,

        MIN(event_time) AS inicio,
        MAX(event_time) AS fim,

        DATEDIFF(SECOND, MIN(event_time), MAX(event_time)) AS duracao,

        SUM(temp_irregular)  AS qtd_temp_irreg,
        SUM(press_irregular) AS qtd_press_irreg

    FROM grupos
    WHERE temp_irregular = 1 OR press_irregular = 1
    GROUP BY device_id, grupo_id
)--Para cada ilha com temperatura maior que 15% em relação as 3 últimas leituras ou pressão maior que 10%
-- em relação as 4 leituras anteriores início e fim da leitura, duração em minutos e o cálculo de tempo e
--pressão irregular por dispositivo. O objetido é saber quais leituras tiveram temperatura ou pressão irregulares.

--Versão sênior:
--Aqui deixamos de olhar eventos e passamos a olhar incidentes.
--É isso que operações, SRE ou engenharia consomem.
SELECT *
FROM duracao
WHERE duracao >= 120          -- Filtro de ilhas com pelo menos 2 minutos
ORDER BY device_id, inicio;
