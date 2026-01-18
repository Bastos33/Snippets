WITH price_base AS (--para cada produto (ou para cada venda) escolher uma faixa de preço que cubra a data alvo
    SELECT
        p.ProductID,
        p.StartDate,
        p.EndDate,
        p.Price,--o RN está reduzindo apenas conflitos que compartilham exatamente o mesmo StartDate.
        ROW_NUMBER() OVER (
            PARTITION BY p.ProductID, p.StartDate
            ORDER BY p.EndDate DESC
        ) AS rn ----O RN não particiona por StartDate, ele ordena por StartDate
        -- mais recente dentro do particionamento por ProductID.
    FROM PriceHistory p
),
price_clean AS (
    -- remove conflitos dentro do histórico de preços
    SELECT *
    FROM price_base
    WHERE rn = 1
), --Depois que ordenei a base eu pego sempre a com startDate e EndDate mais recente?
campaign_match AS (
    -- associa vendas às campanhas vigentes
    SELECT
        s.SaleID,
        s.ProductID,
        s.SaleDate,
        s.Qty,
        c.DiscountPct,
        ROW_NUMBER() OVER (
            PARTITION BY s.SaleID
            ORDER BY c.DiscountPct DESC
        ) AS rn --este rn é particionado por pedido e ordena o maior desconto?
    FROM Sales s
    LEFT JOIN Campaigns c
        ON s.ProductID = c.ProductID
        AND s.SaleDate BETWEEN c.StartDate AND c.EndDate
),
campaign_clean AS (
    -- pega a melhor campanha (maior desconto)
    SELECT *
    FROM campaign_match
    WHERE rn = 1
),----Seleciona os pedidos com maior descontos ordenados pela rn da cte acima?
price_join AS (
    -- associa vendas ao preço vigente na data
    SELECT
        cc.SaleID,
        cc.ProductID,
        cc.SaleDate,
        cc.Qty,
        cc.DiscountPct,
        ph.Price,
        ROW_NUMBER() OVER (
            PARTITION BY cc.SaleID
            ORDER BY ph.StartDate DESC
        ) AS rn
    FROM campaign_clean cc
    LEFT JOIN price_clean ph
        ON cc.ProductID = ph.ProductID
        AND cc.SaleDate BETWEEN ph.StartDate AND ph.EndDate
),--O left join com a regra serve com um tipo filtro.
final_price AS (
    SELECT
        SaleID,
        ProductID,
        SaleDate,
        Qty,
        Price,
        DiscountPct,
        CASE
            WHEN DiscountPct IS NULL THEN Qty * Price
            ELSE Qty * Price * (1 - DiscountPct)
        END AS PrecoEfetivo
    FROM price_join
    WHERE rn = 1
)
SELECT *
FROM final_price
ORDER BY SaleID;
