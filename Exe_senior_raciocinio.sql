WITH lic_base AS (
    SELECT
        l.license_id,
        l.customer_id,
        l.start_date,
        l.end_date,
        l.license_type,
        l.price,
        ROW_NUMBER() OVER (
            PARTITION BY l.customer_id, l.start_date
            ORDER BY l.end_date DESC
        ) AS rk
    FROM licenses l
),--Prepara a base bruta e marca o começo e fim mais recente da licença por usuário, através 
--de 1 das 3 tabelas. No caso Licenses.

--Prepara a base bruta da tabela licenses e marca, para cada cliente e data de início, 
--qual é o período mais recente.
--O ROW_NUMBER ordenando por end_date DESC garante que, quando houver períodos sobrepostos ou
-- múltiplas licenças com o mesmo start_date, a linha com o fim mais recente receba rk = 1.

lic_clean AS (
    SELECT
        license_id,
        customer_id,
        start_date,
        end_date,
        license_type,
        price
    FROM lic_base
    WHERE rk = 1
),--Faz uma limpeza dos dados da cte acima, usa usuário por datas de licença no mesmo inicio e com enddate
-- mais recente usando esta regra de seleção para deduplicar multiplas licenças associadas ao mesmo usuário. 


--Limpa a CTE anterior mantendo somente as licenças únicas por cliente e start_date.
--Aqui ocorre de fato a deduplicação dos períodos sobrepostos, preservando apenas 
--o registro mais atual (o de maior end_date). Msm coisa dita de outra forma.
usage_base AS (
    SELECT
        u.event_id,
        u.customer_id,
        u.event_date,
        u.usage_amount,
        DATE_TRUNC('month', u.event_date) AS mes,
        ROW_NUMBER() OVER (
            PARTITION BY u.event_id
            ORDER BY u.event_date DESC
        ) AS rk
    FROM usage_logs u
),--Prepara a base bruta da segunda das 3 tabelas, trunca mes para analises futuras.
--Marca enumerando o eventid pelo eventdate mais recente. 

--Prepara a base bruta dos eventos de uso, cria o campo mes com o DATE_TRUNC para análises mensais, 
--e marca possíveis duplicações de event_id.
--O ROW_NUMBER garante que, caso o mesmo event_id apareça com múltiplas datas, escolhemos a mais recente.

usage_clean AS (
    SELECT
        event_id,
        customer_id,
        event_date,
        usage_amount,
        mes
    FROM usage_base
    WHERE rk = 1
),--Limpa os dados duplicados mantendo apenas o event ou evento mais atual. Foi o que eu quis dizer, mas
-- tá confuso.

--Filtra somente o evento mais recente para cada event_id, garantindo granularidade correta antes
-- de associar aos períodos de licença.

usage_match AS (
    SELECT
        u.event_id,
        u.customer_id,
        u.event_date,
        u.mes,
        u.usage_amount,
        c.license_id,
        c.price,
        c.license_type,
        ROW_NUMBER() OVER (
            PARTITION BY u.event_id
            ORDER BY c.end_date DESC
        ) AS rk
    FROM usage_clean u
    LEFT JOIN lic_clean c
        ON u.customer_id = c.customer_id
        AND u.event_date BETWEEN c.start_date AND c.end_date
),--Match das tabelas que foram tratadas para granularidade. Une as licenças 
--a cada evento válido.(marca). Anteriormente, foi citado que era um parâmetro de temporalidade.
--Não me lembro a expressão exata, mas é devido a possibilidade de mais de um período. Está próximo, mas ainda confuso.

--Faz o match entre o uso e a licença correspondente ao período.
--O evento só se associa à licença se sua data estiver dentro do intervalo (start_date, end_date).
--Como o cliente pode ter mais de uma licença válida naquele período, um ROW_NUMBER ordenando pelo 
--fim mais recente (end_date DESC) escolhe a licença correta para o evento.

resolved AS (
    SELECT
        event_id,
        customer_id,
        event_date,
        mes,
        usage_amount,
        price,
        license_id,
        license_type
    FROM usage_match
    WHERE rk = 1
),--Retorna os eventos,usuários por licença, tipo de licença, data de uso, quantia de uso, preço daqueles
--com uso dentro do final mais recente e onde este uso/evento esteja entre o inicio e fim da data da licença.

--Deduplica o match acima, mantendo apenas a combinação evento-licença correta (a de período mais recente).
--Aqui sai a versão “resolvida” do dado: cada evento tem exatamente uma licença associada. Falta ser clara.
agg AS (
    SELECT
        r.customer_id,
        ct.country,
        r.mes,
        SUM(r.usage_amount) AS total_usage,
        SUM(r.price) AS receita_mes,
        COUNT(*) AS eventos
    FROM resolved r
    LEFT JOIN countries ct
        ON r.customer_id = ct.customer_id
    GROUP BY r.customer_id, ct.country, r.mes
)--Seleciona os dados deduplicados para efetuar agregações/cálculos e estimar o custo de uso, quanto foi usado,
--por usuário, por país em cada mês. Me parece q agg tbm retorna quem gastou mais no mes.

--Agora que os dados estão deduplicados e associados corretamente, esta camada agrega por cliente, país e mês:
--total de uso, total faturado, quantidade de eventos
--Esta camada resume o comportamento do cliente no mês com base no uso e na licença que estava ativa.

SELECT *
FROM agg
ORDER BY customer_id, mes;--Retorna a analise de qual foi o custo, a quantidade de uso, 
--de cada usuário no país por mes.
