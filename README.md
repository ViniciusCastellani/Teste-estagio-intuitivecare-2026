# 1. TESTE DE INTEGRAÇÃO COM API PÚBLICA - ANS

## Como Executar

```bash
pip install requests beautifulsoup4 pandas openpyxl
python main.py
```

**Saída**: `data/output/consolidado_despesas.zip`

---

## Trade-offs Técnicos

### 1. Busca Global dos Últimos 3 Trimestres
**Decisão**: Atravessar múltiplos anos para encontrar os últimos 3 trimestres disponíveis.

```python
def obter_ultimos_tres_trimestres_global():
    anos.sort(reverse=True)  # Do mais recente para o mais antigo
    
    for ano in anos:
        # Busca trimestres deste ano
        # Se já temos 3+, para de buscar
        if len(todos_trimestres) >= 3:
            break
    
    # Ordena por (ano, trimestre) e retorna os 3 primeiros
    return todos_trimestres[:3]
```

**Por quê?**
- O teste pede "últimos 3 trimestres disponíveis", não "3 trimestres do último ano"
- Se 2024 só tem 1T disponível, deve buscar 4T/2023 e 3T/2023
- Garante sempre 3 trimestres mesmo com dados incompletos

**Exemplo prático**: 
- Ano 2024: 1T ✓
- Ano 2023: 4T ✓, 3T ✓, 2T, 1T
- Resultado: 1T/2024, 4T/2023, 3T/2023

---

### 2. Processamento Incremental (não em memória)
**Decisão**: Cada trimestre é processado separadamente e salvo antes da consolidação.

**Por quê?**
- Arquivos ANS podem ter centenas de MB
- Processar tudo em memória causaria estouro em datasets >1GB
- Mais resiliente a falhas

**Trade-off**: Múltiplas operações I/O (mais lento), mas escalável.

---

### 3. Detecção Automática de Delimitadores
**Decisão**: Tenta delimitadores comuns (`,` `;` `\t` `|`) antes de usar detecção automática.

```python
for sep in [',', ';', '\t', '|']:
    df = pd.read_csv(caminho, sep=sep)
    if len(df.columns) > 1: return df
```

**Por quê?** Arquivos ANS têm formatos inconsistentes entre trimestres.

---

### 4. Identificação de Arquivos de Despesas
**Decisão**: Busca textual por "despesa" + "evento" + "sinistro" no conteúdo do arquivo.

**Por quê?** 
- Nomes de arquivo não seguem padrão
- Buscar no conteúdo é mais confiável

**Trade-off**: Overhead de ler parte do arquivo, mas garante precisão.

---

## Tratamento de Inconsistências

### CNPJs duplicados / Razões Sociais diferentes
**Solução**: Campo `RazaoSocial` = `NaN`

**Por quê?** Os arquivos não contêm Razão Social, apenas Registro ANS. Melhor marcar como ausente que inventar dado.

---

### Valores zerados ou negativos
**Solução**: Mantém dados + adiciona coluna `Suspeito = True`

```python
valores_suspeitos = df_final["ValorDespesas"] <= 0
df_final["Suspeito"] = valores_suspeitos | datas_suspeitas
```

**Por quê?**
- Valores negativos podem ser estornos (válidos)
- Valores zero podem ser ausência de movimentação (válido)
- Preserva dados originais para auditoria
- Flag permite filtrar depois se necessário

---

### Datas inconsistentes com trimestre
**Solução**: Valida campo `DATA` contra ano/trimestre do nome do arquivo. Inconsistências → `Suspeito = True`

```python
df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
inconsistencias = (df["ano_data"] != ano_arquivo) | (df["trimestre_data"] != trimestre_arquivo)
```

**Por quê?** 
- Nome do arquivo é fonte mais confiável
- `errors="coerce"` evita quebra do pipeline em datas inválidas
- Não descarta dados automaticamente

---

## Arquivo Final

**Formato**: `consolidado_despesas.csv` (separador `;`)

| Coluna | Descrição |
|--------|-----------|
| CNPJ | Registro ANS |
| RazaoSocial | NaN (não disponível) |
| Trimestre | 1-4 |
| Ano | YYYY |
| ValorDespesas | `VL_SALDO_FINAL - VL_SALDO_INICIAL` |
| Suspeito | `True` se valor ≤ 0 ou data inconsistente |

---

## Estrutura do Código

```
├── main.py                          # Pipeline completo
├── ans_download.py                  # Download recursivo dos ZIPs
├── ans_processar.py                 # Extração e filtragem
└── ans_consolidar_trimestres.py    # Consolidação + flag Suspeito
```