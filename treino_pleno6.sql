WITH base AS (
    SELECT 
        h.SalesOrderID,
        c.CustomerID,
        h.OrderDate,
        h.TotalDue,
        h.OnlineOrderFlag,
        c.AccountNumber,
        c.PersonID,
        c.StoreID,

        LAG(h.OrderDate) OVER (
            PARTITION BY c.CustomerID
            ORDER BY h.OrderDate
        ) AS pedido_anterior

    FROM Sales.SalesOrderHeader h
    JOIN Sales.Customer c
      ON h.CustomerID = c.CustomerID
),

diff AS (
    SELECT *,
           DATEDIFF(DAY, pedido_anterior, OrderDate) AS diff_dias_pedidos
    FROM base
),

mov AS (
    SELECT *,
           CASE
               WHEN diff_dias_pedidos <
                    LAG(diff_dias_pedidos) OVER (
                        PARTITION BY CustomerID
                        ORDER BY OrderDate
                    )
               THEN 1 ELSE 0
           END AS flag_aceleracao,

           CASE
               WHEN diff_dias_pedidos >
                    LAG(diff_dias_pedidos) OVER (
                        PARTITION BY CustomerID
                        ORDER BY OrderDate
                    )
               THEN 1 ELSE 0
           END AS flag_desaceleracao
    FROM diff
),

reg AS (
    SELECT *,
           MAX(flag_aceleracao) OVER (
               PARTITION BY CustomerID
               ORDER BY OrderDate
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS teve_aceleracao_antes
    FROM mov
)

SELECT *
FROM reg
WHERE flag_desaceleracao = 1
  AND teve_aceleracao_antes = 1
  AND OnlineOrderFlag = 1
ORDER BY CustomerID, OrderDate;