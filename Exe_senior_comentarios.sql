WITH base AS (
    SELECT
        node_id,
        check_time,
        status_code,
        latency_ms,
        LAG(latency_ms) OVER (PARTITION BY node_id ORDER BY check_time) AS prev_lat,
        LAG(check_time) OVER (PARTITION BY node_id ORDER BY check_time) AS prev_time
    FROM SystemHealthLog
),--Prepara base e informações do nó, tempo, status e ms de latencia.
--Lag latency verifica o atraso na comunicação em ms por nó ordenado pelo timestamp do servidor.
--Lag check_time permite o cálculo de atraso do tempo por nó e momento do acontecimento.

--Estou trazendo o registro anterior para cada linha, por nó e em ordem cronológica, 
--para medir saltos de latência e rupturas de tempo

--Versão sênio:
-- Prepara a base trazendo, para cada node, o registro atual e o registro imediatamente anterior.
-- prev_lat: latência da medição anterior do mesmo node_id.
-- prev_time: timestamp anterior do mesmo node_id.
-- Isso permite comparar leituras consecutivas para identificar saltos de latência e intervalos irregulares.
flags AS (
    SELECT
        *,
        CASE 
            WHEN status_code > 0 THEN 1
            WHEN prev_lat IS NOT NULL AND latency_ms > prev_lat * 1.4 THEN 1
            ELSE 0
        END AS instavel,
        CASE
            WHEN prev_time IS NULL THEN 1
            WHEN DATEDIFF(MINUTE, prev_time, check_time) > 3 THEN 1
            ELSE 0
        END AS quebra_tempo
    FROM base
),---Onde o status_code for superior a 0, ou seja, lentidão ou falha marca-se 1. Ou onde prev_lat
--não for a primeira medida desta e a latência aumentou em 40 % em relação período anterior marca 1.
--(instável)
--Onde for início de grupo no nó(is null), marque 1.
--Calculamos a diferença de prev_time e check_time se for maior que 3 minutos marcamos 1. 

-- instavel:
--   - Marca 1 quando o status_code indica erro/alerta (>0).
--   - Ou quando a latência atual supera em 40% a latência anterior (saltos abruptos).
--
-- quebra_tempo:
--   - Marca 1 no primeiro registro do node (prev_time NULL).
--   - Também marca 1 quando o intervalo entre duas medições ultrapassa 3 minutos,
--     indicando um buraco (gap) na linha do tempo do node.

marcados AS (
    SELECT
        *,
        CASE 
            WHEN instavel = 1 OR quebra_tempo = 1 THEN 1
            ELSE 0
        END AS marca_ilha
    FROM flags
),--Consolidamos os grupos com instabilidade, início de grupo no nó, com latência 40% maior e com
--diferença de prev_time e check_time maior que 3 minutos marcamos novamente em 1.

-- marca_ilha consolida os dois tipos de eventos relevantes:
--   - Instabilidades (saltos de latência ou status crítico)
--   - Rupturas de tempo (gaps > 3 min ou início do node)
-- Qualquer uma dessas condições inicia uma nova ilha.

grupos AS (
    SELECT
        *,
        SUM(marca_ilha) OVER (
            PARTITION BY node_id
            ORDER BY check_time
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM marcados
),--Aqui selecionamos o consolidado e somamos para identificar o que é início no nó, o que é continuação e
--o q é ruptura.

-- Para cada node_id, acumulamos os "inícios" marcados por marca_ilha.
-- Cada valor 1 inicia um novo grupo (ilha).
-- grupo_id: identifica cada bloco contínuo entre eventos de instabilidade ou rupturas de tempo.

filtrado AS (
    SELECT
        node_id,
        grupo_id,
        MIN(check_time) AS inicio,
        MAX(check_time) AS fim,
        DATEDIFF(MINUTE, MIN(check_time), MAX(check_time)) AS minutos,
        MAX(status_code) AS pior_status,
        MAX(latency_ms) AS max_lat
    FROM grupos
    WHERE instavel = 1
    GROUP BY node_id, grupo_id
),--Aqui calculamos início, fim do intervalo e duração de minutos entre eles.
--O pior status e a maior latência na ilha, para cada grupo de nó que apontaram lentidão, falha ou
-- latência maior que 40%(Instáveis).

-- Agrega cada grupo que contém instabilidade real (instavel = 1):
--   - início e fim do intervalo de instabilidade
--   - duração do período em minutos
--   - pior status_code registrado dentro da ilha
--   - maior latência registrada dentro da ilha
-- O objetivo é resumir cada ilha de instabilidade.

final AS (
    SELECT *
    FROM filtrado
    WHERE minutos >= 5
)---Retorna todas as ilhas com duração maior ou igual a 5 minutos.
SELECT *
FROM final
ORDER BY node_id, inicio;
--Retornamos um panorama do início, fim do intervalo e duração de minutos entre eles. Do pior status e
-- a maior latência na ilha de todas aquelas com duração maior ou igual a 5 minutos.
