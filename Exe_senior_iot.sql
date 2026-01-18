WITH base AS (
    SELECT
        device_id,
        event_time,
        reading,
        AVG(reading) OVER (
            PARTITION BY device_id
            ORDER BY event_time
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS mov_media,
        LAG(event_time) OVER (
            PARTITION BY device_id ORDER BY event_time
--Recupera o timestamp da leitura imediatamente anterior (mesmo device), para medir continuidade temporal.
        ) AS prev_time
    FROM SensorReadings--Para cada sensor retorna a média móvel dos 5 intervalos anteriores ao atual
    -- por dispositivo. Verifica o evento anterior de cada sensor para cada linha. Basicamente estão 
    --organizando uma linha temporal dos dispositos.

--Função:
--Calcula a média das 5 leituras imediatamente anteriores à leitura atual, para cada dispositivo,
-- independentemente do tempo entre elas.

--Isto é importante: ROWS trabalha com quantidade de linhas, não com intervalos de tempo.
),
flags AS (
    SELECT
        *,
        CASE 
            WHEN mov_media IS NOT NULL 
                 AND ABS(reading - mov_media) > mov_media * 0.12
            THEN 1 ELSE 0
        END AS fora_tolerancia,
        CASE 
            WHEN prev_time IS NULL THEN 1
            WHEN DATEDIFF(SECOND, prev_time, event_time) > 120 THEN 1
            ELSE 0
        END AS quebra
    FROM base
),--Aplicamos case se houver registro na média móvel e a leitura atual for 12% maior em relação as 5 leituras
--anteriores marcamos 1, para identificar os sensores descalibrados.
--Se não há um período anterior de registro(null), marcamos 1. Ainda para constatar gap na continuidade verificamos
--se o sensor ficou off mais de 120 segundos, selecionamos tbm.

--Função sênior:
--Marca se a leitura atual é uma anomalia estatística local, comparada às últimas 5.
--Não tem relação com falha do dispositivo — é apenas um desvio de comportamento.

--Quebra: Identifica quebras na linha do tempo:
--prev_time IS NULL → início absoluto do sensor.
--diff > 120 → o sensor ficou tempo demais sem enviar leitura → um GAP.
--Gaps segmentam a linha do tempo.
marcados AS (
    SELECT
        *,
        CASE 
            WHEN fora_tolerancia = 1 AND quebra = 0 THEN 0
            ELSE 1
        END AS inicio_ilha
    FROM flags
),--Aqui se o dispositivo tem a leitura atual 12% que as 5 anteriores, mas não ficou mais de 120 segundos
--offline está ok. Para as demais combinações de situações selecionamos 1, incluindo aquelas que apontam
--fora da tolerância e quebra.

--Função sênior — ponto mais importante:

--inicio_ilha = 0 → a anomalia continua dentro da mesma ilha, pois não houve quebra temporal.
--inicio_ilha = 1 → uma nova ilha começa aqui, seja por:
--leitura normal, leitura anômala após um GAP, início absoluto.

--Resumo:
--inicio_ilha não diz “sensor ok” nem “sensor ruim”.
--Ele controla segmentação das ilhas de anomalias.
grupos AS (
    SELECT
        *,
        SUM(inicio_ilha) OVER (
            PARTITION BY device_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM marcadosFunção sênior:
A soma acumulada cria o identificador de cada ilha contínua de eventos, segmentada pelas quebras.

--É o padrão clássico de Gaps & Islands com running sum.
---Nesta cte organizamos o acumulado dos dispositivos separados pela lógica acima, particionamos 
--por sensor e consideramos a primeira leitura até o momento atual.

--Função sênior:
--A soma acumulada cria o identificador de cada ilha contínua de eventos, segmentada pelas quebras.
--É o padrão clássico de Gaps & Islands com running sum.

duracao AS (
    SELECT
        device_id,
        grupo_id,
        MIN(event_time) AS inicio,
        MAX(event_time) AS fim,
        DATEDIFF(SECOND, MIN(event_time), MAX(event_time)) AS duracao,
        COUNT(*) AS qtd_leituras
    FROM grupos
    WHERE fora_tolerancia = 1
    GROUP BY device_id, grupo_id
)--Após as classificações realizadas acima com case. Separamos para cada sensor e cada ilha que estiverem
--fora da tolerância, quantidade de leituras, Início, fim e duração de tempo.

--CTE: duracao
--Seleciona apenas ilhas onde houve anomalia (fora_tolerancia = 1) e calcula:
--início da ilha, fim, duração total, nº de leituras anômalas

SELECT *
FROM duracao
WHERE duracao >= 180   -- pelo menos 3 minutos
ORDER BY device_id, inicio;---Aqui separamos todos os sensores quebrados ou possívelmente descalibrados
-- são separados. Fornecemos uma visão confiável de quais dos sensores estiveram offline por 180 segundos 
--ou mais, portanto, aqueles que apresentam problema.

--Extrai apenas ilhas de anomalias duradouras (≥ 3 min).
--Não indica quebra de hardware — indica persistência anômala, que pode sugerir descalibração.
