  WITH base AS(
        SELECT  
             order_id,
             hub_id,
             payment_confirmed_at,
             picking_started_at,
             packed_at,
             shipped_at,
             items_count,
             priority_flag,
             --minutos para calcular atraso e preparação
             DATEDIFF(MINUTE, payment_confirmed_at, packed_at) AS duracao_minutos,
        -- pedido anterior no mesmo warehouse
        LAG(payment_confirmed_at) OVER (
            PARTITION BY hub_id
            ORDER BY payment_confirmed_at
        ) AS prev_payment
    FROM OrderFulfillmentEvents
  ), flags AS(
              SELECT
                     *,
              CASE
                   WHEN duracao_minutos > 90
                   THEN 1
                   ELSE 0
                   END AS pedido_atrasado,
              CASE
                  WHEN DATEDIFF(MINUTE, prev_payment, payment_confirmed_at) > 30
                  THEN 1
                  ELSE 0
                  END AS gap_operacional
                  FROM base            
  ), inicio_periodo AS(
                       SELECT
                              *,
                              CASE
                                  WHEN pedido_atrasado = 1
                                  AND ( prev_payment IS NULL
                                      OR LAG(pedido_atrasado) OVER (
                        PARTITION BY hub_id
                        ORDER BY payment_confirmed_at
                    ) = 0
                       OR gap_operacional = 1)
                                  THEN 1
                                  ELSE 0
                                  END AS inicio_periodo_critico
                                  FROM flags
 ), ilha AS(
            SELECT
                   *,
                   SUM(inicio_periodo_critico) OVER(
                    PARTITION BY hub_id
                    ORDER BY payment_confirmed_at
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS inicio_ilha
                    FROM inicio_periodo
                    WHERE pedido_atrasado = 1
                   
 ),metricas AS (
    SELECT
        hub_id,
        ilha,

        MIN(payment_confirmed_at) AS inicio_periodo,
        MAX(payment_confirmed_at) AS fim_periodo,

        DATEDIFF(
            MINUTE,
            MIN(payment_confirmed_at),
            MAX(payment_confirmed_at)
        ) AS duracao_periodo_minutos,

        COUNT(*) AS qtd_pedidos,
        SUM(CASE WHEN priority_flag = 1 THEN 1 ELSE 0 END) AS qtd_prioritarios,
        MAX(duracao_minutos) AS pior_preparacao
    FROM ilha
    GROUP BY hub_id, ilha
)
SELECT *
FROM metricas
WHERE duracao_total_minutos >= 45
  AND quantidade_pedidos >= 2
ORDER BY hub_id, inicio_periodo;