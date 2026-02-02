WITH media_geral AS (
    SELECT AVG(valor_despesas) as media
    FROM despesas_consolidadas
    WHERE ABS(valor_despesas) < 1000000000  -- Apenas valores válidos para média
),
trimestres_acima_media AS (
    SELECT 
        d.cnpj,
        CASE WHEN d.valor_despesas > mg.media THEN 1 ELSE 0 END as acima_media
    FROM despesas_consolidadas d
    CROSS JOIN media_geral mg
    WHERE ABS(d.valor_despesas) < 1000000000
),
contagem_operadora AS (
    SELECT 
        cnpj,
        SUM(acima_media) as trimestres_acima_media,
        COUNT(*) as total_trimestres
    FROM trimestres_acima_media
    GROUP BY cnpj
)
SELECT 
    COALESCE(o.razao_social, c.cnpj) as razao_social,
    c.trimestres_acima_media,
    c.total_trimestres
FROM contagem_operadora c
LEFT JOIN operadoras o ON TRIM(c.cnpj) = TRIM(o.registro_operadora)  -- ← MUDANÇA AQUI
WHERE c.trimestres_acima_media >= 2
ORDER BY c.trimestres_acima_media DESC, razao_social
LIMIT 20;