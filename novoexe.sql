WITH base AS
(
    SELECT
          	 c.customer_id,
			 p.product_id,
			 p.product_name,
			 u.usage_date,
			 u.usage_count,
             c.customer_name,
             c.signup_date,
			 p.is_critical
	  FROM  Customers c
      LEFT JOIN CustomerProductUsage u
      ON c.customer_id = u.customer_id
	  LEFT JOIN Products p on u.product_id = p.product_id
    ),
    cal AS(
        SELECT customer_id,
        product_id,
        DATEFROMPARTS(YEAR(usage_date), MONTH(usage_date), 1) AS month_start,
        SUM(usage_count) AS eventos_mes
              FROM base
              GROUP BY customer_id, 
                       product_id,
                      DATEFROMPARTS(YEAR(usage_date), MONTH(usage_date), 1)
    ), mant AS
    (
        SELECT *,
        LAG(eventos_mes) OVER(
            PARTITION BY customer_id, product_id
            ORDER BY month_start
            ) AS eventos_mes_anterior
         FROM cal
    ),reg AS
    (
    SELECT
        *,
        CASE
            WHEN eventos_mes_anterior > 0
             AND eventos_mes <= eventos_mes_anterior * 0.6
            THEN 1
            ELSE 0
        END AS flag_risco
        FROM mant
    )
    SELECT
         customer_id,
         product_id,
         month_start,
         eventos_mes AS uso_mes,
         eventos_mes_anterior AS uso_mes_anterior,
    (
    (evento_mes - evento_mes_anterior) * 100.0
    / NULLIF(evento_mes_anterior, 0)
)  AS pct_variacao,
flag_risco
FROM reg