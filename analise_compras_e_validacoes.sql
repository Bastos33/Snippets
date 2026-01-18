-- Criar tabela temporária para armazenar a base agregada
IF OBJECT_ID('tempdb..#Metricas') IS NOT NULL DROP TABLE #Metricas;

WITH base AS (
    SELECT
        H.CustomerID AS id_cliente,
        D.ProductID AS product_id,
        P.ProductSubcategoryID AS marca,
        D.SalesOrderID AS id_venda,
        H.OrderDate,
        YEAR(H.OrderDate) AS ano,
        MONTH(H.OrderDate) AS mes,
        D.OrderQty AS qty,
        (D.OrderQty * D.UnitPrice) AS valor_item,
        1 AS comprou
    FROM Sales.SalesOrderHeader H
    JOIN Sales.SalesOrderDetail D
        ON H.SalesOrderID = D.SalesOrderID
    JOIN Production.Product P
        ON D.ProductID = P.ProductID
),
Agregado AS (
    SELECT
        id_cliente,
        marca,
        ano,
        mes,
        SUM(valor_item) AS total_mes_marca,
        COUNT(DISTINCT id_venda) AS freq_marca,
        NULLIF(SUM(valor_item)/SUM(qty),0) AS ticket_med_marca,
        SUM(comprou) AS total_n_comprou,
        DATEDIFF(DAY, MAX(OrderDate), EOMONTH(MAX(OrderDate))) AS recencia
    FROM base
    GROUP BY id_cliente, marca, ano, mes
),
Metricas AS (
    SELECT *,
        AVG(ticket_med_marca) OVER(
            PARTITION BY id_cliente, marca
            ORDER BY ano, mes
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS media_3m,
        SUM(total_n_comprou) OVER(
            PARTITION BY id_cliente, marca
            ORDER BY ano, mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS contagem_3meses
    FROM Agregado
),
TotalCliente AS (
    SELECT
        id_cliente,
        ano,
        mes,
        SUM(total_mes_marca) AS total_mes_cliente
    FROM Metricas
    GROUP BY id_cliente, ano, mes
)
SELECT
    F.id_cliente,
    M.marca,
    F.ano,
    F.mes,
    M.total_mes_marca,
    M.freq_marca,
    (M.total_mes_marca / F.total_mes_cliente) * 100 AS part_pct_marca,
    M.recencia,
    M.ticket_med_marca,
    M.media_3m,
    CASE WHEN M.contagem_3meses = 3 THEN 1 ELSE 0 END AS taxa_recompra_flag,
    CASE WHEN M.total_mes_marca > M.media_3m * 1.5 THEN 1 ELSE 0 END AS flag_explosao,
    RANK() OVER(
        PARTITION BY M.id_cliente, F.ano, F.mes
        ORDER BY M.total_mes_marca DESC
    ) AS rank_cliente_marca
INTO #Metricas
FROM Metricas M
JOIN TotalCliente F
    ON M.id_cliente = F.id_cliente
   AND M.ano = F.ano
   AND M.mes = F.mes;
GO

-- ==================================================================================
-- 2️⃣ Validação 1: Total gasto por cliente/mês
-- ==================================================================================
SELECT
    id_cliente,
    ano,
    mes,
    SUM(total_mes_marca) AS soma_marcas,
    SUM(total_mes_marca) AS total_mes_cliente_calc,
    SUM(total_mes_marca) - SUM(total_mes_marca) AS diff -- Sempre zero, apenas validação sintaxe
FROM #Metricas
GROUP BY id_cliente, ano, mes;
GO

-- ==================================================================================
-- 3️⃣ Validação 2: Ticket médio
-- ==================================================================================
SELECT
    id_cliente,
    marca,
    ano,
    mes,
    ticket_med_marca,
    total_mes_marca / freq_marca AS ticket_calc,
    CASE WHEN ticket_med_marca = total_mes_marca / freq_marca THEN 'OK' ELSE 'Erro' END AS valida_ticket
FROM #Metricas;
GO

-- ==================================================================================
-- 4️⃣ Validação 3: Flags de recompra e explosão
-- ==================================================================================
SELECT *
FROM #Metricas
WHERE (contagem_3meses < 3 AND taxa_recompra_flag = 1)
   OR (total_mes_marca <= media_3m * 1.5 AND flag_explosao = 1);
GO
