WITH sub_base AS (
    SELECT
        customer_id,
        subscription_id,
        start_date,
        end_date,
        plan_type,
        monthly_fee,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, start_date
            ORDER BY end_date DESC
        ) AS rk
    FROM subscriptions
),--Prepara base bruta de subscriptions, marca a linha de cada período de cliente ativo usando customer_id + start_date
-- Trata a granularidade no caso de mais de uma assinatura de start_date igual determinando o end_date mais atual.

--Prepara a linha temporal das assinaturas e resolve conflitos de múltiplos períodos com mesmo start_date,
--mantendo apenas o período com end_date mais recente para cada cliente.”
sub_clean AS (
    SELECT
        customer_id,
        subscription_id,
        start_date,
        end_date,
        plan_type,
        monthly_fee
    FROM sub_base
    WHERE rk = 1
),--Deduplica os produtos marcados acima, associa apenas um cliente por assinatura ativa no período.
--Evita duplicidade de resgistros selecionando o top 1 do rn acima.

--“Remove períodos conflitantes, mantendo apenas a assinatura válida mais recente para cada 
--(cliente, start_date).”

logs_base AS (
    SELECT
        log_id,
        customer_id,
        log_date,
        feature,
        duration_minutes,
        DATE_TRUNC('month', log_date) AS mes,
        ROW_NUMBER() OVER (
            PARTITION BY log_id
            ORDER BY log_date DESC
        ) AS rk
    FROM usage_logs
),--Prepara a base bruta de usage_logs, trunca mes para análise mes a mes. E o mais importante, garante um 
--único log para cliente evitando a duplicidade de logs por erro, registrado duplamente com datas distintas.
--Através de log_id + log_date ficamos apenas com a ocorrência mais recente.


--Normaliza logs removendo duplicidades por log_id, mantendo a versão mais recente de um log. Não 
--reduz quantidade de logs reais, apenas corrige duplicações da origem.
logs_clean AS (
    SELECT
        log_id,
        customer_id,
        log_date,
        feature,
        duration_minutes,
        mes
    FROM logs_base
    WHERE rk = 1
),--Garante o log mais recente por log_id e total de minutos de uso
--da feature. Garante a granularidade para o match abaixo.

logs_match AS (
    SELECT
        l.log_id,
        l.customer_id,
        l.log_date,
        l.duration_minutes,
        l.mes,
        s.subscription_id,
        s.monthly_fee,
        ROW_NUMBER() OVER (
            PARTITION BY l.log_id
            ORDER BY s.end_date DESC
        ) AS rk
    FROM logs_clean l
    LEFT JOIN sub_clean s
        ON l.customer_id = s.customer_id
        AND l.log_date BETWEEN s.start_date AND s.end_date
),--Match das ctes deduplicadas acima. Associa assinatura a cliente, bem como valor do uso mensal de acordo
--com uso da feature(duration minutes). Garante a singularidade do log de cair em assinaturas justapostas ou
--com períodos distintos. No join ocorre junção de logs e subs. Escolhemos o log mais recente e onde sua data
--cubra a data de log. O log deve estar no período de assinatura válida.

--Associa cada log à assinatura ativa no momento do uso.
--Quando mais de uma assinatura cobre o mesmo log (overlap temporal),
-- mantém a que tem o end_date mais recente.”

resolved_logs AS (
    SELECT
        log_id,
        customer_id,
        log_date,
        duration_minutes,
        mes,
        subscription_id,
        monthly_fee
    FROM logs_match
    WHERE rk = 1
),--Pega assinatura ativa para o log mais recente do cliente, bem como duração do evento e custo do mês 
--a cada mes.

agg_usage AS (
    SELECT
        customer_id,
        mes,
        SUM(duration_minutes) AS total_minutes,
        COUNT(DISTINCT DATE(log_date)) AS active_days
    FROM resolved_logs
    GROUP BY customer_id, mes
),--Agragação ou cálculos de total de uso, período de atividade do log por cliente/mes.

last_use AS (
    SELECT
        customer_id,
        mes,
        MAX(log_date) OVER (
            PARTITION BY customer_id
            ORDER BY mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_use_before_month
    FROM (
        SELECT DISTINCT customer_id, mes, log_date
        FROM resolved_logs
    ) t
),--Comparação de atividade do log cliente/mes comparando a última data de uso com a atual.

usage_final AS (
    SELECT
        a.customer_id,
        a.mes,
        a.total_minutes,
        a.active_days,
        DATE_DIFF('day', l.last_use_before_month, DATE_TRUNC('month', a.mes)) AS days_since_last_use
    FROM agg_usage a
    LEFT JOIN last_use l
        ON a.customer_id = l.customer_id
        AND a.mes = l.mes
),--Trata da atividade em total de minutos até o último dia de uso para cliente/mes.

cancel_month AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', cancel_date) AS mes,
        1 AS churned_month
    FROM cancellations
),--Cria-se o campo de flag churned_month onde está a informação de mês de cancelamento do cliente.

final AS (
    SELECT
        COALESCE(u.customer_id, c.customer_id) AS customer_id,
        COALESCE(u.mes, c.mes) AS mes,
        u.total_minutes,
        u.active_days,
        u.days_since_last_use,
        COALESCE(c.churned_month, 0) AS churned_month,
        CASE
            WHEN COALESCE(c.churned_month, 0) = 1 THEN 0
            WHEN (u.active_days < 2 OR u.days_since_last_use > 20) THEN 1
            ELSE 0
        END AS churn_risk
    FROM usage_final u
    FULL OUTER JOIN cancel_month c
        ON u.customer_id = c.customer_id
        AND u.mes = c.mes
)--Fz um outer permitindo que total de minutos, days de atividade e dias desde o ultimo uso sejam mantidos
--com as métrica de engajamento, estabelecidas em churned_month e no tratamento de case se mes de cancelamento
--existe 1 e quando atividade menor que 2 ou dias desde a ultima atividade menor que 20 retorne o dado. 

SELECT *
FROM final
ORDER BY customer_id, mes; --Retorna as métricas de churn por cliente, mes. 
