CREATE TABLE despesas_consolidadas (
    id BIGSERIAL PRIMARY KEY,
    cnpj VARCHAR(20) NOT NULL,
    razao_social VARCHAR(255),
    trimestre SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    ano SMALLINT NOT NULL,
    valor_despesas DECIMAL(20,2) NOT NULL,
    suspeito BOOLEAN NOT NULL
);

CREATE INDEX idx_consolidadas_cnpj ON despesas_consolidadas (cnpj);
CREATE INDEX idx_consolidadas_periodo ON despesas_consolidadas (ano, trimestre);
CREATE INDEX idx_consolidadas_suspeito ON despesas_consolidadas (suspeito);