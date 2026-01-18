WITH diffs AS (
    SELECT
        leitura_id,
        data_leitura,
        temperatura,
        temperatura 
          - LAG(temperatura) OVER (ORDER BY data_leitura) AS diff_temp
    FROM Temperaturas
),--Prepara a base bruta de leitura de temperatura. Traz um panorama de leitura por data temperatura e 
--a diferença de temperatura. Então, temperatura - Lag(temperatura) calculada temperatura menos temperatura 
--do mês anterior sobre data de leitura

--Versão sênior:
-- Calcula a diferença entre a leitura atual e a anterior.
-- LAG permite comparar leituras consecutivas sem auto-join.
-- Se a diferença passar do limite, isso indica um possível início de nova "ilha".

marcados AS (
    SELECT
        *,
        CASE 
            WHEN diff_temp IS NULL THEN 0 
            WHEN diff_temp > 1.5 THEN 1 
            ELSE 0 
        END AS inicio_grupo
    FROM diffs
),--Seleciona os dados e contagem de temperatura de Diffs. Aplica tratamento de case para tratar null,
--q deve ser 0 e se a diferença de temperatura for maior de 1.5 será marcado com 1. Bem como é nomeado
--este campo(inicio_grupo). Isso representa um novo ciclo.

--Versão sênior:
-- Marca explicitamente os pontos onde um novo grupo deve começar.
-- "inicio_grupo" é um *marcador de ruptura*, não o identificador final.
agrupados AS (
    SELECT
        *,
        SUM(inicio_grupo) OVER (
            ORDER BY data_leitura
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grupo_id
    FROM marcados
)--Trata o campo de de inicio_grupo e o soma por data de leitura do início até o momento corrente
-- para verificar a contagem e estabelecer o início de um grupo.

--Versão sênior:
-- Constrói o identificador dos grupos.
-- A soma cumulativa transforma “marcadores de início” em um número de grupo crescente.
-- RANGE é usado para acumular todos os valores anteriores na ordem temporal.
-- Cada salto incrementa o total → cada intervalo recebe um grupo_id único.

SELECT
    grupo_id,
    MIN(data_leitura) AS inicio_periodo,
    MAX(data_leitura) AS fim_periodo,
    MIN(temperatura) AS temp_min,
    MAX(temperatura) AS temp_max,
    COUNT(*) AS total_leituras
FROM agrupados
GROUP BY grupo_id
ORDER BY grupo_id;

--Calcula para cada grupo_id início de periodo, fim de período, em: -Min, Max.
--Temperatura máxima e mínima nas agregações semelhantes abaixo e a quantidade final de leituras.

--Versão sênior:
--LAG compara o valor atual com o anterior sem auto-joins.

--diff_temp detecta rupturas.

--inicio_grupo marca onde uma ilha começa.

--SUM cumulativa transforma marcas em grupos.

--O GROUP BY reconstrói os intervalos contínuos (as “ilhas”).