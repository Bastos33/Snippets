WITH base AS (
   select 
       d.driver_id,
       d.driver_name,
	   e.delivery_id,
	   e.pickup_ready_at,
	   e.out_for_delivery_at,
	   e.delivered_at,
       DATEDIFF(MINUTE, e.pickup_ready_at, e.out_for_delivery_at) AS retencao,
       --DATEDIFF(MINUTE, e.out_for_delivery_at, e.delivered_at) AS tempo_rota,
        DATEDIFF(MINUTE, e.out_for_delivery_at, e.delivered_at) AS sla_total_min,

        -- atributos temporais
        DATEPART(HOUR, e.pickup_ready_at) AS hora_pickup,
        DATEPART(WEEKDAY, e.pickup_ready_at) AS dia_semana
from drivers d
LEFT JOIN Deliveries e
ON d.driver_id = e.driver_id
),
cal AS (
    SELECT driver_id,
driver_name,
delivery_id,
pickup_ready_at,
out_for_delivery_at,
delivered_at,
    sla_total_min,
    LAG(sla_total_min,1) OVER(
        PARTITION BY driver_id
        ORDER BY delivered_at
        ) AS sla_total1,
        LAG(sla_total_min,2) OVER(
        PARTITION BY driver_id
        ORDER BY delivered_at
        ) AS sla_total2
    -- AVG(sla_total_min) OVER (
--           PARTITION BY driver_id, delivery_id
--           ORDER BY pickup_ready_at
--           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
--       ) AS media_movel_atraso_3seguidos
 FROM base
)
    SELECT *
    FROM cal
    WHERE sla_total_min > sla_total1
    AND sla_total1 > sla_total2

