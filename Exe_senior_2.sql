WITH order_clean AS (
    SELECT
        o.order_id,
        o.product_id,
        o.customer_id,
        o.order_date,
        o.quantity,
        o.unit_price,
        ROW_NUMBER() OVER (
            PARTITION BY o.order_id, o.product_id
            ORDER BY o.order_date DESC
        ) AS rk
    FROM orders o
),--Sim, aqui ocorre uma deduplicação por pedido + produto, e o ORDER BY o.order_date DESC garante que,
-- se houver linhas duplicadas desse par, você fica apenas com a mais recente.
--O objetivo real desta camada não é “selecionar dados essenciais”, mas sim criar uma versão confiável
-- dos pedidos, sem múltiplas linhas do mesmo produto dentro do mesmo pedido.
price_base AS (
    SELECT
        p.product_id,
        p.start_date,
        p.end_date,
        p.price,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_id, p.start_date
            ORDER BY p.end_date DESC
        ) AS rn
    FROM price_history p
),--O RN aqui não reduz registros ainda — ele marca qual
-- linha é “a melhor” dentro do mesmo ProductID + StartDate.
price_clean AS (
    SELECT
        product_id,
        start_date,
        end_date,
        price
    FROM price_base
    WHERE rn = 1
),-- Aqui finalmente ocorre o afunilamento real, deixando 1 único registro por ProductID + StartDate.
price_match AS (
    SELECT
        o.order_id,
        o.product_id,
        o.customer_id,
        o.order_date,
        o.quantity,
        o.unit_price,
        ph.price AS historical_price,
        ROW_NUMBER() OVER (
            PARTITION BY o.order_id, o.product_id
            ORDER BY ph.end_date DESC
        ) AS rk_price
    FROM order_clean o
    LEFT JOIN price_clean ph
        ON o.product_id = ph.product_id
        AND o.order_date BETWEEN ph.start_date AND ph.end_date
),--é realizado uma consulta para match dos campos com base redução de registros feitas nas ctes acima(price_clean),
--o rn desta cte faz o particionamento por pedido e produto oriundos de um afunilamento de rn 
--ordenando por uma data afunilada no rn de outra camada. E a lógica deve considerar q o orderDate deve estar
--entre um startdate e enddate afunilados na camada acima.
--Sim, esta camada tenta atribuir um preço histórico válido para cada pedido.
--A regra essencial é:

-- order_date precisa estar dentro do intervalo StartDate–EndDate
-- price_clean garante que temos apenas intervalos válidos
-- se ainda assim houver mais de um match (problema comum),
 --o RN aqui escolhe o intervalo mais recente (ordenando por EndDate DESC)
 --Este join funciona como um LIKE um filtro temporal inteligente
resolved_price AS (
    SELECT
        order_id,
        product_id,
        customer_id,
        order_date,
        quantity,
        CASE
            WHEN unit_price IS NULL OR unit_price <= 0
            THEN historical_price
            ELSE unit_price
        END AS final_price
    FROM price_match
    WHERE rk_price = 1
),--Seleciona campos no topo do rn da camada price_match para tratar nulls, estipular 
--regra para preço histórico e atravé do case determinar o preço final.

--Se o preço do pedido é inválido (nulo ou ≤ 0) → usa preço histórico
--Se não → usa o preço do pedido
-- Este é o ponto onde a lógica de preço é resolvida definitivamente.
anomaly AS (
    SELECT
        order_id,
        product_id,
        customer_id,
        order_date,
        quantity,
        final_price,
        CASE
            WHEN final_price < 0.5 * pct.avg_price THEN 'SUSPEITO'
            WHEN final_price > 1.8 * pct.avg_price THEN 'SUSPEITO'
            ELSE 'OK'
        END AS price_flag
    FROM resolved_price rp
    LEFT JOIN (
        SELECT
            product_id,
            AVG(price) AS avg_price
        FROM price_clean
        GROUP BY product_id
    ) pct
        ON rp.product_id = pct.product_id
)--Analisa uma anomalia no preço retornando o preço médio da subconsulta e estabelecendo parâmetros no case
--do que seria um preço suspeito. Se menor 0.5 multiplicado pelo preço da sub suspeito e Se maior q 1.8 
--multiplicado pelo preço obtido na sub tbm suspeito q retornarão os parâmetros na price_flag 
--
--Sim, este bloco compara o final_price da venda com o preço médio 
--histórico do produto, obtido diretamente da price_clean.
--A regra é bem simples:
--muito abaixo do padrão (menos de 50%) → SUSPEITO
--muito acima (mais de 180%) → SUSPEITO
--caso contrário → OK
--O pct.avg_price vem da subconsulta, que calcula o preço médio real do produto ao longo
SELECT *
FROM anomaly
ORDER BY order_date, product_id;--Retorna a consulta da camada acima.
