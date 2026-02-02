WITH dados_com_ordem AS (
    SELECT 
        d.cnpj,
        o.razao_social,
        d.ano,
        d.trimestre,
        d.valor_despesas,
        ROW_NUMBER() OVER (PARTITION BY d.cnpj ORDER BY d.ano, d.trimestre) as posicao_primeiro,
        ROW_NUMBER() OVER (PARTITION BY d.cnpj ORDER BY d.ano DESC, d.trimestre DESC) as posicao_ultimo
    FROM despesas_consolidadas d
    LEFT JOIN operadoras o ON TRIM(d.cnpj) = TRIM(o.registro_operadora)  -- ← MUDANÇA AQUI
),
primeira_despesa AS (
    SELECT 
        cnpj,
        razao_social,
        valor_despesas as primeira_despesa
    FROM dados_com_ordem
    WHERE posicao_primeiro = 1
),
ultima_despesa AS (
    SELECT 
        cnpj,
        valor_despesas as ultima_despesa
    FROM dados_com_ordem
    WHERE posicao_ultimo = 1
)
SELECT 
    COALESCE(pd.razao_social, pd.cnpj) as razao_social,
    ROUND(pd.primeira_despesa, 2) as primeira_despesa,
    ROUND(ud.ultima_despesa, 2) as ultima_despesa,
    ROUND(
        100.0 * (ud.ultima_despesa - pd.primeira_despesa) / NULLIF(pd.primeira_despesa, 0), 
        2
    ) as crescimento_percentual
FROM primeira_despesa pd
JOIN ultima_despesa ud ON pd.cnpj = ud.cnpj
WHERE pd.primeira_despesa > 0
  AND ABS(pd.primeira_despesa) < 1000000000  -- Filtrar valores absurdos
  AND ABS(ud.ultima_despesa) < 1000000000
ORDER BY crescimento_percentual DESC
LIMIT 5;