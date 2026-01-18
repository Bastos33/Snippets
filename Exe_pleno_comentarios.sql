WITH sub_base AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, start_date
            ORDER BY end_date DESC
        ) AS rk
    FROM subscriptions s
),--Prepara a linha temporal de assinaturas soluciona a possível multiplicidade de períodos com mesmo start_date
--Marcando apenas o período com EndDate mais atual por cliente.

sub_clean AS (
    SELECT
        subscription_id,
        customer_id,
        plan,
        price,
        start_date,
        end_date
    FROM sub_base
    WHERE rk = 1
),--Remove períodos conflitantes, preserva apenas a assinatura válida mais recente para cada cliente,
-- start_date.

sub_month AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', start_date) AS mes,
        GREATEST(start_date, DATE_TRUNC('month', start_date)) AS inicio_mes,
        LEAST(
            end_date,
            DATE_TRUNC('month', start_date) + INTERVAL '1 month - 1 day'
        ) AS fim_mes,
        price
    FROM sub_clean
),--Cria uma cte para trunca mes do start_date para analise mensal, através de funções do postgresql
--cria o campo inicio mes e fim. Neste último determina um intervalo em postgresql e deveria ser t-sql.
--Associa os períodos de atividade do cliente a preço em diferentes partes do mes.--Confuso a beça!

usage_agg AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', event_timestamp) AS mes,
        COUNT(*) AS qtd_eventos
    FROM events
    GROUP BY customer_id, DATE_TRUNC('month', event_timestamp)
),--Conta quantidade de eventos no mês por cliente.

--Mensura a atividade real(engajamento)

billing_agg AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', invoice_date) AS mes,
        SUM(amount) AS receita_mes
    FROM billing
    WHERE status = 'paid'
    GROUP BY customer_id, DATE_TRUNC('month', invoice_date)
),--Calcula a receita mensal das invoices pagas por cliente.

--Objetivo:
--Medir faturamento consolidado do cliente no mês.

active_days AS (
    SELECT
        customer_id,
        mes,
        SUM(fim_mes::date - inicio_mes::date + 1) AS dias_ativos
    FROM sub_month
    GROUP BY customer_id, mes
),--Utiliza novamente funções postgresql, especificamente Cast, qnd deveria usar t-sql.
--A cte é usada para associar a cada cliente no mes os dias ativos.

--Cálculo de dias ativos no mês
--Com base nos intervalos mensais de assinatura criados em sub_month: fim e inicio mes.
--Determinar o tempo real em que o usuário esteve habilitado a usar o serviço naquele mês.
final AS (
    SELECT
        COALESCE(a.customer_id, u.customer_id, b.customer_id) AS customer_id,
        COALESCE(a.mes, u.mes, b.mes) AS mes,
        a.dias_ativos,
        u.qtd_eventos,
        b.receita_mes,
        CASE 
            WHEN a.dias_ativos IS NULL THEN 1
            ELSE 0
        END AS churn_flag
    FROM active_days a
    FULL JOIN usage_agg u 
        ON a.customer_id = u.customer_id AND a.mes = u.mes
    FULL JOIN billing_agg b
        ON COALESCE(a.customer_id, u.customer_id) = b.customer_id
        AND COALESCE(a.mes, u.mes) = b.mes
)--Faz Full outer join para garantir a manutenção de usuários sem dias ativos ou sem invoices pagas.
--Faz um match das ctes deduplicadas para atribuir ao cliente unificado a qnt de eventos, dias ativos e 
--receita. Cria um campo churn para identificar se o cliente está inativo. Determina que a data uso e dias ativos
--estejam dentro do mes de cobrança. 
SELECT *
FROM final
ORDER BY customer_id, mes;
--Retorna dias ativos, qnt eventos, receita mes, métrica de inatividade para cliente/mês, ordenando estes últimos.
