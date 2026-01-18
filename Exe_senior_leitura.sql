--Você trabalha com dados de renovação de garantias de produtos vendidos a clientes.
--Há três tabelas:
--sales:
--sale_id, customer_id, product_id, sale_date, sale_price

--warranty_periods:
--product_id, start_date, end_date, coverage_level

--claims:
--claim_id, sale_id, claim_date, claim_cost
WITH warr_base AS (
    SELECT
        w.product_id,
        w.start_date,
        w.end_date,
        w.coverage_level,
        ROW_NUMBER() OVER (
            PARTITION BY w.product_id, w.start_date
            ORDER BY w.end_date DESC
        ) AS rk
    FROM warranty_periods w
),--Prepara a base bruta para cada produto no inicio de sua cobertura e marca por endDate desc (mais recente)
--qnd houver período dobrado ou multiplos produtos com msm inicio.

--Marca a linha mais recente de cada período de garantia usando product_id + start_date como chave.
 --Garante que, se existirem duas garantias começando no mesmo dia, ficamos apenas com o período cujo
--end_date é mais recente.

warr_clean AS (
    SELECT
        product_id,
        start_date,
        end_date,
        coverage_level
    FROM warr_base
    WHERE rk = 1
),--deduplica os produtos marcados acima, associando apenas um produto para cobertura ativa no período.
--Seleciona o top 1 do rn acima garantindo o período mais atual evitando duplicação de registros 

sales_month AS (
    SELECT
        s.sale_id,
        s.customer_id,
        s.product_id,
        s.sale_date,
        DATE_TRUNC('month', s.sale_date) AS mes,
        s.sale_price
    FROM sales s
),--Prepara base bruta da tabela de vendas, por pedido, cliente, produto e data do pedido.
-- Trunca mes para analise mensal. apenas prepara o calendário mensal de vendas.

claims_base AS (
    SELECT
        c.claim_id,
        c.sale_id,
        c.claim_date,
        DATE_TRUNC('month', c.claim_date) AS mes,
        c.claim_cost,
        ROW_NUMBER() OVER (
            PARTITION BY c.claim_id
            ORDER BY c.claim_date DESC
        ) AS rk
    FROM claims c
),--Prepara a base bruta da terceira tabela preparando id, data e custo da reinvindicação.
--Trunca mes para analise mes a mes, marca reinvindicação possíveis de duplicação pela data mais atual 
--caso a msm reinvindicação apareça para muitas datas. 

-- Ele existe para prevenir duplicações sistêmicas, tipo:

--claim duplicado por erro, claim registrado duas vezes com datas diferentes, carga duplicada de logs
--Garante que, se um mesmo claim_id aparecer múltiplas vezes (duplicação de sistema),
-- ficamos apenas com a ocorrência mais recente.

claims_clean AS (
    SELECT
        claim_id,
        sale_id,
        claim_date,
        mes,
        claim_cost
    FROM claims_base
    WHERE rk = 1
),--Filtra a garantia mais recente no pedido por mes e seu custo. 
--Garante granularidade antes do match abaixo.

claim_match AS (
    SELECT
        cl.claim_id,
        cl.sale_id,
        cl.claim_date,
        cl.mes,
        cl.claim_cost,
        s.customer_id,
        s.product_id,
        w.coverage_level,
        ROW_NUMBER() OVER (
            PARTITION BY cl.claim_id
            ORDER BY w.end_date DESC
        ) AS rk
    FROM claims_clean cl
    LEFT JOIN sales_month s
        ON cl.sale_id = s.sale_id
    LEFT JOIN warr_clean w
        ON s.product_id = w.product_id
        AND cl.claim_date BETWEEN w.start_date AND w.end_date
),--Associa a reinvindicação do pedido por data/ mes e qual cobertura ativa por cliente.
--A reinvindicação precisa estar no intervalo das datas de cobertura ativa. Utiliza-se a reinvindicação
--mais atual para o caso de haver mais de uma no período(marca). E assim conseguir associar cobertura para o produto.
--na data de claim.

--Aqui o RN serve para deduplicar a associação claim -> garantia.

--Porque um mesmo claim pode cair em:
--duas garantias sobrepostas (ajustes históricos), dois períodos diferentes porque o sistema registrou errado
--dois planos do mesmo produto com datas que se cruzam

--Aqui ocorre o join entre claim, venda e garantia.
--Para cada claim, escolhemos a garantia com end_date mais recente que cobre aquela data do claim.
--Esse RN não é para deduplicar claims — é para deduplicar qual garantia será escolhida.

resolved_claims AS (
    SELECT
        claim_id,
        sale_id,
        customer_id,
        product_id,
        mes,
        claim_cost,
        coverage_level
    FROM claim_match
    WHERE rk = 1
),--Pega a cobertura ativa para o produto na data mais recente da reinvindicação, deduplica!
--Tbm traz o mes e o custo da claim. 

agg_sales AS (
    SELECT
        customer_id,
        mes,
        COUNT(*) AS produtos_no_mes,
        SUM(sale_price) AS receita_mes
    FROM sales_month
    GROUP BY customer_id, mes
),--Aqui são realizados os calculos quantidade de produtos e total da venda para cada cliente mensalmente.

agg_claims AS (
    SELECT
        customer_id,
        mes,
        COUNT(*) AS qtd_claims,
        SUM(claim_cost) AS custo_claims
    FROM resolved_claims
    GROUP BY customer_id, mes
)--REaliza-se o calculo de quantidade de reinvindicações e custo delas para cada cliente no mes. 

SELECT
    COALESCE(s.customer_id, c.customer_id) AS customer_id,
    COALESCE(s.mes, c.mes) AS mes,
    s.produtos_no_mes,
    s.receita_mes,
    c.qtd_claims,
    c.custo_claims
FROM agg_sales s
FULL OUTER JOIN agg_claims c
    ON s.customer_id = c.customer_id
    AND s.mes = c.mes
ORDER BY customer_id, mes;
--Associa-se cliente por venda e claim para trazer produtos na receita da qnt e custo das garantias 
--por mes.

--Faz um FULL OUTER JOIN para unificar o calendário mensal de vendas e o calendário mensal de claims.
--Permite que meses com somente vendas ou somente claims também apareçam.
--COALESCE garante que customer_id e mês sejam preenchidos mesmo quando um dos lados não existe.