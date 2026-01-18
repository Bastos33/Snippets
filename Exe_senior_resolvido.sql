WITH base AS (---o Date_trunc é utilizado para manter o tipo de dado original para realizar cálculos
--de datas subsequentes sem conversão explícitas é crucial para análises temporais consistentes, agregações
--precisas e, muitas vezes, melhor desempenho com índices. 
    SELECT
        s.customer_id,
        s.sale_date,
        DATE_TRUNC('month', s.sale_date) AS mes,
        s.category,
        s.amount
    FROM sales s
),---Seleciona campos necessários dentro do mes, mantendo dado de data original.
agg AS (
    SELECT
        customer_id,
        COUNT(*) AS qtd_compras,
        SUM(amount) AS total_gasto,
        AVG(amount) AS ticket_medio,
        MAX(sale_date) AS ultima_compra
    FROM base
    GROUP BY customer_id
),--O count sugere 1 compra por linha cliente, as demais agregações são Soma e média de valor e Max 
--para última compra.
fav_cat AS (
    SELECT
        customer_id,
        category,
        SUM(amount) AS total_categoria,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY SUM(amount) DESC
        ) AS rk
    FROM base
    GROUP BY customer_id, category
),--Realiza-se o cálculo de valor para categoria. É realizado o cálculo particionando por cliente
-- do maior valor. Atribuindo o primeiro/maior valor evitando uma duplicação deste match. 
    -- Aqui calcula-se o total por categoria e aplica-se um ranking para identificar a categoria
    -- mais relevante para cada cliente, evitando duplicidades ao pegar apenas a de maior valor.
monthly_rank AS (
    SELECT
        mes,
        customer_id,
        SUM(amount) AS total_mes,
        RANK() OVER (
            PARTITION BY mes
            ORDER BY SUM(amount) DESC
        ) AS rank_mes
    FROM base
    GROUP BY mes, customer_id
),--Cálculo é realizado mês a mes, e usa-se a cte justamente para realizar o cálculo do cliente neste
--período determinado. É realizado um ranking quem mais gastou em cada mes.
churn AS (
    SELECT
        customer_id,
        CASE WHEN MAX(sale_date) < CURRENT_DATE - INTERVAL '180 days' THEN 1 ELSE 0 END AS churn_flag
    FROM base
    GROUP BY customer_id
)--Aqui estipula-se que a última compra deve ser menor q a data corrente menos o intervalo de 180 dias.
--Se for verdadeiro 1 senão 0. Ou seja, seleciona os clientes que correspondem a 1.
-- Define churn como clientes cuja última compra ocorreu há mais de 180 dias.
SELECT
    c.customer_id,
    c.country,
    a.ultima_compra,
    a.qtd_compras,
    a.total_gasto,
    a.ticket_medio,
    f.category AS categoria_favorita,
    ch.churn_flag,
    m.mes,
    m.rank_mes
FROM customers c
LEFT JOIN agg a ON c.customer_id = a.customer_id
LEFT JOIN fav_cat f ON c.customer_id = f.customer_id AND f.rk = 1
LEFT JOIN churn ch ON c.customer_id = ch.customer_id
LEFT JOIN monthly_rank m ON c.customer_id = m.customer_id
ORDER BY c.customer_id, m.mes;
    -- O SELECT final realiza as junções para compor a visão completa do cliente,
    -- trazendo comportamento, categoria favorita, churn e ranking mensal.
