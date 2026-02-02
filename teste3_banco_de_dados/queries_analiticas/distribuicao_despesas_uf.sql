WITH despesas_por_operadora_uf AS (
    SELECT 
        COALESCE(UPPER(TRIM(o.uf)), 'SEM_UF') as uf,
        d.cnpj,
        SUM(d.valor_despesas) as total_por_operadora
    FROM despesas_consolidadas d
    LEFT JOIN operadoras o ON TRIM(d.cnpj) = TRIM(o.registro_operadora)  -- ← MUDANÇA AQUI
    WHERE ABS(d.valor_despesas) < 1000000000  -- Filtrar valores absurdos
    GROUP BY uf, d.cnpj
),
resumo_uf AS (
    SELECT 
        uf,
        SUM(total_por_operadora) as total_despesas,
        COUNT(DISTINCT cnpj) as qtd_operadoras,
        AVG(total_por_operadora) as media_por_operadora
    FROM despesas_por_operadora_uf
    GROUP BY uf
)
SELECT 
    uf,
    ROUND(total_despesas, 2) as total_despesas,
    qtd_operadoras,
    ROUND(media_por_operadora, 2) as media_por_operadora_na_uf
FROM resumo_uf
WHERE uf != 'SEM_UF'  -- Remover registros sem UF
ORDER BY total_despesas DESC
LIMIT 5;