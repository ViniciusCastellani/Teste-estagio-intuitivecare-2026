CREATE TEMP TABLE temp_operadoras (
    registro_operadora TEXT,
    cnpj TEXT,
    razao_social TEXT,
    nome_fantasia TEXT,
    modalidade TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade TEXT,
    uf TEXT,
    cep TEXT,
    ddd TEXT,
    telefone TEXT,
    fax TEXT,
    endereco_eletronico TEXT,
    representante TEXT,
    cargo_representante TEXT,
    regiao_comercializacao TEXT,
    data_registro_ans TEXT
);

\copy temp_operadoras FROM '/media/vinicius/1E32896B3289492B/Users/Vinicius/Downloads/Arquivos-SSD/PROGRAMACAO/Teste_Vinicius_Castellani_Tonello/teste3_banco_de_dados/data/operadoras_ativas_ans.csv' DELIMITER ';' CSV HEADER ENCODING 'UTF8'

INSERT INTO operadoras (
    registro_operadora,
    cnpj,
    razao_social,
    nome_fantasia,
    modalidade,
    uf,
    data_registro_ans
)
SELECT 
    TRIM(registro_operadora),
    TRIM(cnpj),
    TRIM(razao_social),
    NULLIF(TRIM(nome_fantasia), ''),
    NULLIF(TRIM(modalidade), ''),
    UPPER(TRIM(uf)),
    CASE 
        WHEN data_registro_ans LIKE '____-__-__' THEN data_registro_ans::DATE
        WHEN data_registro_ans LIKE '__/__/____' THEN TO_DATE(data_registro_ans, 'DD/MM/YYYY')
        ELSE NULL
    END
FROM temp_operadoras
WHERE registro_operadora IS NOT NULL AND registro_operadora != '';

DROP TABLE temp_operadoras;