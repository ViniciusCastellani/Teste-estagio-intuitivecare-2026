CREATE TEMP TABLE temp_agregadas (
    razao_social TEXT,
    uf TEXT,
    total_despesas TEXT,
    media_despesas_trimestre TEXT,
    desvio_padrao_despesas TEXT
);

\copy temp_agregadas FROM '/media/vinicius/1E32896B3289492B/Users/Vinicius/Downloads/Arquivos-SSD/PROGRAMACAO/Teste_Vinicius_Castellani_Tonello/teste3_banco_de_dados/data/despesas_agregadas.csv' DELIMITER ';' CSV HEADER ENCODING 'UTF8'

INSERT INTO despesas_agregadas (razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas)
SELECT 
    TRIM(razao_social),
    UPPER(TRIM(uf)),
    REPLACE(REPLACE(total_despesas, '.', ''), ',', '.')::DECIMAL(18,2),
    REPLACE(REPLACE(media_despesas_trimestre, '.', ''), ',', '.')::DECIMAL(18,2),
    REPLACE(REPLACE(desvio_padrao_despesas, '.', ''), ',', '.')::DECIMAL(18,2)
FROM temp_agregadas;

DROP TABLE temp_agregadas;