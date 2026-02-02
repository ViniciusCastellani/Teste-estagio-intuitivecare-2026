CREATE TABLE operadoras (
    id SERIAL PRIMARY KEY,
    registro_operadora VARCHAR(20) NOT NULL,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep VARCHAR(10),
    ddd VARCHAR(5),
    telefone VARCHAR(20),
    fax VARCHAR(20),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_comercializacao INT,
    data_registro_ans DATE,
    CONSTRAINT uq_operadoras_registro UNIQUE (registro_operadora)
);

CREATE INDEX idx_operadoras_cnpj ON operadoras (cnpj);
CREATE INDEX idx_operadoras_uf ON operadoras (uf);