WITH base AS
           (
            SELECT     
                    --h.SalesOrderID,
                    c.CustomerID,
                    --c.PersonID,
                    h.OrderDate,
                    p.FirstName,
                    p.LastName,
                    h.OnlineOrderFlag,
                     LAG(h.OrderDate) OVER (
            PARTITION BY c.CustomerID
            ORDER BY h.OrderDate
        ) AS pedido_anterior
  FROM Sales.SalesOrderHeader h
JOIN Sales.Customer c
  ON h.CustomerID = c.CustomerID
LEFT JOIN Person.Person p
  ON c.PersonID = p.BusinessEntityID

), diff AS(
            SELECT *,
                 DATEDIFF(DAY, pedido_anterior, OrderDate) AS diff_dias_pedidos
            FROM base
), flag AS(
           SELECT 
                  *,
                CASE
                    WHEN diff_dias_pedidos < LAG(diff_dias_pedidos) OVER (
                    PARTITION BY CustomerID
                    ORDER BY OrderDate
         )
     AND LAG(diff_dias_pedidos) OVER (
             PARTITION BY CustomerID
             ORDER BY OrderDate
         )
         < LAG(diff_dias_pedidos, 2) OVER (
             PARTITION BY CustomerID
             ORDER BY OrderDate
         )
    THEN 1
    ELSE 0
END AS flag_aceleracao
           FROM diff       
), flag2 AS(
            SELECT 
                   *,
           MAX(flag_aceleracao) OVER (
               PARTITION BY CustomerID
               ORDER BY OrderDate
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS teve_aceleracao_antes   
    FROM flag       
), flagdes AS(
             SELECT
                    *,
                   CASE
               WHEN teve_aceleracao_antes = 1
               AND diff_dias_pedidos >
                    LAG(diff_dias_pedidos) OVER (
                        PARTITION BY CustomerID
                        ORDER BY OrderDate
                        ) 
                        THEN 1
                        ELSE 0
                        END AS flag_desaceleracao
FROM flag2
), ilha AS(
             SELECT
                    *,
                    CASE WHEN teve_aceleracao_antes = 1
                    AND OnlineOrderFlag = 1 
                    THEN SUM(flag_desaceleracao) OVER (
               PARTITION BY CustomerID
               ORDER BY OrderDate
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)           
                        ELSE 0
                        END AS periodo_valido
                    FROM flagdes
) SELECT CustomerID,
         FirstName,
         LastName,
         OrderDate,
         diff_dias_pedidos AS intervalo_dias,  
         flag_aceleracao AS aceleracao,
         flag_desaceleracao AS flag_desaceleracao
         FROM ilha            