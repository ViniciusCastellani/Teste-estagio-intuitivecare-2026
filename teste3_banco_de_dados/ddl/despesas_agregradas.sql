CREATE TABLE despesas_agregadas (
    id BIGSERIAL PRIMARY KEY,
    razao_social VARCHAR(255),
    uf CHAR(2),
    total_despesas DECIMAL(20,2),
    media_despesas_trimestre DECIMAL(20,2),
    desvio_padrao_despesas DECIMAL(20,2)  
);

CREATE INDEX idx_agregadas_razao ON despesas_agregadas (razao_social);
CREATE INDEX idx_agregadas_uf ON despesas_agregadas (uf);