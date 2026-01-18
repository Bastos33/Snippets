    WITH base AS(
        SELECT  
        order_id,
        warehouse_id,
        payment_at,
        invoice_at,

        -- atraso de faturamento (> 2h)
        DATEDIFF(MINUTE, payment_at, invoice_at) AS atraso_minutos,

        -- pedido anterior no mesmo warehouse
        LAG(payment_at) OVER (
            PARTITION BY warehouse_id
            ORDER BY payment_at
        ) AS prev_payment,

        LAG(
            DATEDIFF(MINUTE, payment_at, invoice_at)
        ) OVER (
            PARTITION BY warehouse_id
            ORDER BY payment_at
        ) AS prev_atraso_minutos
    FROM orders
    ), flags AS
    (
        SELECT
               *,
                -- faturado com atraso
        CASE
            WHEN atraso_minutos > 120 THEN 1
            ELSE 0
        END AS faturado_atrasado,
                 --CASE 
                    -- WHEN warehouse_id = LAG(warehouse_id) OVER (ORDER BY payment_at)
                  --THEN 1
                  --ELSE 0
                 -- END AS mesmo_warehouse,
                 -- gap operacional
        CASE
            WHEN prev_payment IS NULL THEN 0
            WHEN DATEDIFF(MINUTE, prev_payment, payment_at) > 30 THEN 1
            ELSE 0
        END AS gap_operacional
        FROM base
    ),
     inicio_periodo AS (
    SELECT
        *,
        CASE
            WHEN faturado_atrasado = 1
             AND (
                    prev_payment IS NULL              -- primeiro pedido do warehouse
                 OR prev_atraso_minutos <= 120        -- pedido anterior não atrasado
                 OR gap_operacional = 1               -- houve GAP
                 )
            THEN 1
            ELSE 0
        END AS inicio_periodo_critico
    FROM flags
    ), ilhas AS (
    SELECT
        *,
        SUM(inicio_periodo_critico) OVER (
            PARTITION BY warehouse_id
            ORDER BY payment_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ilha_periodo
    FROM inicio_periodo
    WHERE faturado_atrasado = 1
    ), 
     atri AS(
            SELECT
                  order_id,
                  warehouse_id,
                  payment_at, 
                  invoice_at, 
                  shipped_at,
                  inicio_periodo,
                  fim_periodo,
                  DATEDIFF(MINUTE,shipped_at, invoice_at) AS duracao_minutos,
                  COUNT(*) AS qnt_pedidos,
                  MAX(diff_pag_fatura) as pior_atraso,
                  prev_payment,
                  ilha_pedidos
                  FROM ilha
    WHERE faturado_atrasado = 1
    GROUP BY order_id, 
             warehouse_id,
             payment_at, 
             invoice_at, 
             shipped_at,
             inicio_periodo,
             fim_periodo,
             prev_payment,
             ilha_pedidos
     )
     SELECT *
     FROM atri
WHERE duracao_minutos >= 60
  AND qnt_pedidos >= 3
ORDER BY order_id, warehouse_id;
