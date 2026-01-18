WITH contract_base AS (
    SELECT
        contract_id,
        customer_id,
        start_date,
        end_date,
        base_price,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id, start_date
            ORDER BY end_date DESC
        ) AS rk
    FROM contracts
),--Seleciona campos confiáveis(não duplicados), a deduplicação por contract_id e start_date. Se houver 
--duplicação preserva-se o contrato mais recente.

-- Aqui não é apenas “selecionar campos confiáveis”.
-- O objetivo real é: resolver sobreposições de vigência em um mesmo contrato.
-- Se existir mais de um registro com o mesmo contract_id + start_date,
-- pega o registro com o maior end_date (o mais atualizado da linha do tempo).
-- É uma deduplicação temporal, não apenas estrutural.

contract_clean AS (
    SELECT
        contract_id,
        customer_id,
        start_date,
        end_date,
        base_price
    FROM contract_base
    WHERE rk = 1
), --Através da marcação de dados acima, nesta etapa ocorre o afunilamento dos contratos mais recentes.

-- Nesta etapa fica somente *um* registro por (contract_id, start_date),
-- garantindo que todas as vigências usadas depois não tenham versões antigas.
-- É uma camada de “vigência consolidada” — não só afunilar: é limpar versões.

contract_match AS (
    SELECT
        b.bill_id,
        b.contract_id,
        b.bill_date,
        b.qty,
        c.customer_id,
        c.base_price,
        ROW_NUMBER() OVER (
            PARTITION BY b.bill_id
            ORDER BY c.end_date DESC
        ) AS rk
    FROM billing b
    LEFT JOIN contract_clean c
        ON b.contract_id = c.contract_id
        AND b.bill_date BETWEEN c.start_date AND c.end_date
),--Join estabelecendo um filtro onde a cobrança deve estar de acordo com a StartDate e EndDate da cte
--que afunilou os registros pegando o contrato mais recente.
--Faz a junção do contrato com as cobranças através do particionamento do contrato dentro da data do 
--contrato(Dado deduplicado na camada acima) mais recente.

-- Aqui realmente acontece o "filtro" de vigência.
-- Para cada fatura (bill_id), podem existir várias versões de contrato
-- que tecnicamente cobrem aquela data (em ambientes reais isso acontece).
-- O row_number escolhe a versão que termina mais tarde (a mais recente válida).
-- É um match temporal + deduplicação em cima da fatura.

resolved AS (
    SELECT
        bill_id,
        contract_id,
        customer_id,
        bill_date,
        qty,
        base_price,
        qty * base_price AS billed_value
    FROM contract_match
    WHERE rk = 1
),--Retorna o match de contrato e cobrança realizado acima, estabelece o filtro no where para garantir a 
--deduplicação de dados desta junção de cobrança na data final do contrato mais recente.

-- Aqui se fixa a versão final do contrato para cada cobrança:
-- só o registro com rk=1, ou seja, a versão válida e mais recente.
-- É onde amarra o preço final que deve ser faturado.

agg AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', bill_date) AS mes,
        SUM(billed_value) AS receita_mensal,
        COUNT(*) AS qtd_faturas
    FROM resolved
    GROUP BY customer_id, DATE_TRUNC('month', bill_date)
)--Cria uma camada para possibilitar o cálculo do total de cobranças mes a mes.
SELECT
    a.customer_id,
    a.mes,
    a.receita_mensal,
    a.qtd_faturas
FROM agg a
ORDER BY a.customer_id, a.mes;--Retorna o cálculo de quantidade de faturas por cliente, total mensal cliente
--realizado na camada acima. 