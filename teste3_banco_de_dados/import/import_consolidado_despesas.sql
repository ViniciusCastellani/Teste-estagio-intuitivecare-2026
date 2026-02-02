CREATE TEMP TABLE temp_despesas (
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT,
    suspeito TEXT
);

\copy temp_despesas FROM '/media/vinicius/1E32896B3289492B/Users/Vinicius/Downloads/Arquivos-SSD/PROGRAMACAO/Teste_Vinicius_Castellani_Tonello/teste3_banco_de_dados/data/consolidado_despesas.csv' DELIMITER ';' CSV HEADER ENCODING 'UTF8'

INSERT INTO despesas_consolidadas (cnpj, razao_social, trimestre, ano, valor_despesas, suspeito)
SELECT 
    TRIM(cnpj),
    TRIM(razao_social),
    trimestre::SMALLINT,
    ano::SMALLINT,
    CASE 
        WHEN REPLACE(REPLACE(valor_despesas, '.', ''), ',', '.') ~ '^-?[0-9]+\.?[0-9]*$' 
        THEN REPLACE(REPLACE(valor_despesas, '.', ''), ',', '.')::DECIMAL(20,2)
        ELSE 0.00
    END,
    CASE WHEN LOWER(suspeito) IN ('true', '1', 'sim') THEN TRUE ELSE FALSE END
FROM temp_despesas
WHERE cnpj IS NOT NULL AND cnpj != '';

DROP TABLE temp_despesas;