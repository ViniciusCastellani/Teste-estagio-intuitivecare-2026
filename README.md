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
**Decisão**: Tenta delimitadores comuns `,` `;` `\t` `|`) antes de usar detecção automática.
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

---
---

# 2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS

## Como Executar
```bash
cd teste2_validacao_dados/src
pip install requests beautifulsoup4 pandas openpyxl
python main.py
```
**Entrada**: `teste1_api_ans/data/output/consolidado_despesas.csv`  
**Saída**: `data/zip/Teste_Vinicius_Castellani_Tonello.zip`

O pipeline executa 3 etapas em sequência:

| Etapa | Script | Entrada | Saída |
|-------|--------|---------|-------|
| 1 — Validação | `validar_dados.py` | `consolidado_despesas.csv` | `dados_validados_completo.csv` |
| 2 — Enriquecimento | `enriquecer_dados.py` | `dados_validados_completo.csv` + cadastro ANS | `dados_enriquecidos.csv` |
| 3 — Agregação | `agregar_dados.py` | `dados_enriquecidos.csv` | `despesas_agregadas.csv` + `.zip` |

---

## Trade-offs Técnicos

### 1. Estratégia de Validação: Flag vs. Descarte
**Decisão**: Registros inválidos são **mantidos** com flags booleanas, nunca descartados.
```python
df["Registro_ANS_Valido"] = df["CNPJ"].apply(validar_registro_ans)
df["Valor_Valido"]        = df["Valor_Despesas"].apply(validar_valor_positivo)
df["Validacao_OK"]        = df["Registro_ANS_Valido"] & df["Valor_Valido"]
```
**Estratégias consideradas:**

| Estratégia | Pros | Contras |
|------------|------|---------|
| **Descartar inválidos** | Dados mais limpos para etapas seguintes | Perda de dados, impossível auditar |
| **Corrigir automaticamente** | Sem dados ausentes | Risco de inventar dados incorretos |
| **Manter com flag** ✓ | Preserva auditoria, decisão adiada para downstream | Etapas seguintes precisam considerar flags |

**Por quê essa?**
- Preserva rastreabilidade total — é possível identificar exatamente o que foi inválido
- Não inventa dados (sem risco de silenciar erros)
- Downstream pode filtrar ou tratar conforme necessidade específica
- As flags são removidas após o enriquecimento (`remover_colunas_validacao`), mantendo o CSV final limpo

---

### 2. Validação por Registro ANS (não por CNPJ com dígitos verificadores)
**Decisão**: A coluna `CNPJ` do teste 1 contém o **Registro ANS** (6 dígitos numéricos), não um CNPJ no formato clássico (14 dígitos com mod-11). A validação aplica-se ao formato do Registro ANS.
```python
def validar_registro_ans(reg_ans):
    reg_ans_str = str(reg_ans).strip()
    return reg_ans_str.isdigit() and len(reg_ans_str) == 6
```
**Por quê?**
- O campo originado no teste 1 é o identificador ANS, não o CNPJ fiscal da empresa
- Aplicar mod-11 (algoritmo de CNPJ) nesse campo produziria falsos negativos massivos
- O CNPJ real da operadora só aparece após o enriquecimento com o cadastro ANS

---

### 3. Join: LEFT JOIN vs INNER JOIN
**Decisão**: Usar `LEFT JOIN` no enriquecimento, preservando todos os registros do consolidado.
```python
df_final = df_consolidado.merge(
    df_operadoras[colunas_join],
    left_on="CNPJ_norm",
    right_on="Registro_ANS_norm",
    how="left"
)
```
**Estratégias consideradas:**

| Estratégia | Pros | Contras |
|------------|------|---------|
| **INNER JOIN** | Só dados com match completo | Perde registros sem cadastro — dados de despesas válidos sumem |
| **LEFT JOIN** ✓ | Preserva todos os registros de despesas | Campos do cadastro ficam `NaN` sem match |
| **RIGHT JOIN** | Preserva cadastro completo | Perde despesas sem match no cadastro |

**Por quê essa?**
- Despesas são o dado principal — não devem ser descartadas por ausência no cadastro
- Campos `NaN` após join são transparentes e auditáveis
- Consistente com a filosofia do teste 1 de preservar dados originais

---

### 4. Detecção Dinâmica de Colunas no Cadastro ANS
**Decisão**: Detectar os nomes das colunas em tempo de execução ao invés de hardcodar.
```python
def detectar_colunas_cadastro(df_operadoras):
    for col in colunas:
        if 'REGISTRO' in col.upper(): col_registro = col
        if 'CNPJ' in col.upper():     col_cnpj = col
        if 'RAZAO' in col.upper():    col_razao = col
        # ...
```
**Por quê?**
- O CSV da ANS é atualizado periodicamente e pode mudar nomes de colunas
- Hardcodar exigiria manutenção toda vez que a fonte mudar
- Busca por substring (`'REGISTRO' in col.upper()`) é robusta a variações

---

### 5. Resolução de Colunas Duplicadas após Merge
**Decisão**: Priorizar dados do cadastro ANS (`_y`) sobre os do consolidado (`_x`) usando `combine_first`.
```python
def resolver_colunas_duplicadas(df):
    for col_x in colunas_x:
        col_y = col_base + '_y'
        df[col_base] = df[col_y].combine_first(df[col_x])  # cadastro > consolidado
```
**Por quê?**
- O cadastro ANS é fonte primária e mais completa para dados da operadora
- `combine_first` preenche `NaN` do cadastro com o valor do consolidado — não perde dados quando o cadastro não tem registro
- Evita duplicatas sem perda de informação

---

### 6. Agregação em Memória (pandas groupby)
**Decisão**: Agregar diretamente com `groupby` do pandas, sem uso de banco de dados ou processamento em chunks.
```python
agregados_df = df.groupby(["Razao_Social", "UF"], as_index=False).agg(
    Total_Despesas            = ("Valor_Despesas", "sum"),
    Media_Despesas_Trimestre  = ("Valor_Despesas", "mean"),
    Desvio_Padrao_Despesas    = ("Valor_Despesas", "std")
)
```
**Estratégias consideradas:**

| Estratégia | Pros | Contras |
|------------|------|---------|
| **groupby em memória** ✓ | Simples, rápido para dados até ~1GB | Limitado pela RAM disponível |
| **Chunked processing** | Suporta datasets maiores | Complexidade para combinar parciais (soma ok, std não) |
| **Banco de dados (SQL)** | Escala ilimitada | Overhead de setup, overkill para este volume |

**Por quê essa?**
- O dataset consolidado (3 trimestres de operadoras) é da ordem de dezenas de MB — bem dentro da capacidade de memória
- `std` (desvio padrão) requer acesso a todos os valores do grupo simultaneamente, tornando chunked processing significativamente mais complexo
- Ordenação final (`sort_values`) também beneficia-se de tudo em memória

---

## Tratamento de Inconsistências

### Registros sem match no cadastro ANS
**Solução**: Mantidos no resultado com campos do cadastro como `NaN` (consequência do LEFT JOIN).

**Por quê?** Descartar despesas válidas por ausência no cadastro significaria perda de dados financeiros auditáveis. O campo `NaN` já sinaliza a ausência.

---

### CNPJs múltiplos no cadastro com dados diferentes
**Solução**: O cadastro da ANS é baixado como-está e o merge usa a primeira correspondência encontrada pelo pandas.

**Por quê?**
- Duplicatas no cadastro ANS são raras e geralmente refletem operadoras em situações especiais (fusões, incorporações)
- Manter lógica simples evita inventar regras de prioridade sem contexto de negócio
- Pode ser refinado se o downstream indicar problemas específicos

---

### Notação científica em campos numéricos
**Solução**: Forçar conversão para string após o merge para campos como `CNPJ_Real`.
```python
def corrigir_cnpj_notacao_cientifica(df):
    if "CNPJ_Real" in df.columns:
        df["CNPJ_Real"] = df["CNPJ_Real"].astype(str)
```
**Por quê?** Pandas pode interpretar CNPJs (14 dígitos) como float e convertê-los em notação científica (ex: `1.234e+13`), corrompendo o valor.

---

## Arquivo Final
**Formato**: `despesas_agregadas.csv` (separador `;`)

| Coluna | Descrição |
|--------|-----------|
| Razao_Social | Nome da operadora (do cadastro ANS) |
| UF | Estado de registro |
| Total_Despesas | Soma das despesas por operadora/UF |
| Media_Despesas_Trimestre | Média por trimestre para cada operadora/UF |
| Desvio_Padrao_Despesas | Desvio padrão das despesas (variabilidade) |

Ordenado por `Total_Despesas` decrescente.

---

## Estrutura do Código
```
teste2_validacao_dados/
├── src/
│   ├── main.py                 # Pipeline orquestrador (etapas 1→2→3)
│   ├── validar_dados.py        # Validação + flags
│   ├── enriquecer_dados.py     # Download cadastro + join + limpeza
│   └── agregar_dados.py        # Agregação + compactação ZIP
└── data/
    ├── raw/                    # operadoras_ativas_ans.csv (baixado automaticamente)
    ├── validado/               # dados_validados_completo.csv
    ├── enriquecido/            # dados_enriquecidos.csv
    ├── agregado/               # despesas_agregadas.csv
    └── zip/                    # Teste_Vinicius_Castellani_Tonello.zip
```