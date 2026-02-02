#!/bin/bash

# Pega o diretório onde o script está
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configurações
DB_NAME="ans_test"

echo "==================================="
echo "TESTE COMPLETO - BANCO DE DADOS ANS"
echo "==================================="

# 1. Criar banco (apaga se já existe)
echo ""
echo "[1/8] Recriando banco de dados..."
dropdb --if-exists $DB_NAME
createdb $DB_NAME
echo "✓ Banco criado"

# 2. Criar tabelas
echo ""
echo "[2/8] Criando tabelas..."
psql -d $DB_NAME -f "$SCRIPT_DIR/ddl/consolidado_despesas.sql"
psql -d $DB_NAME -f "$SCRIPT_DIR/ddl/despesas_agregradas.sql"
psql -d $DB_NAME -f "$SCRIPT_DIR/ddl/operadoras.sql"
echo "✓ Tabelas criadas"

# 3. Verificar estrutura
echo ""
echo "[3/8] Verificando estrutura das tabelas..."
psql -d $DB_NAME -c "\dt"
echo ""

# 4. Importar dados
echo "[4/8] Importando despesas consolidadas..."
psql -d $DB_NAME -f "$SCRIPT_DIR/import/import_consolidado_despesas.sql"

echo ""
echo "[5/8] Importando despesas agregadas..."
psql -d $DB_NAME -f "$SCRIPT_DIR/import/import_despesas_agregradas.sql"

echo ""
echo "[6/8] Importando operadoras..."
psql -d $DB_NAME -f "$SCRIPT_DIR/import/import_operadoras.sql"

# 5. Validar importação
echo ""
echo "[7/8] Validando importação..."
psql -d $DB_NAME << 'EOF'
SELECT 
    'despesas_consolidadas' as tabela,
    COUNT(*) as total_linhas
FROM despesas_consolidadas
UNION ALL
SELECT 
    'despesas_agregadas',
    COUNT(*)
FROM despesas_agregadas
UNION ALL
SELECT 
    'operadoras',
    COUNT(*)
FROM operadoras;
EOF

# 6. Executar queries analíticas
echo ""
echo "[8/8] Executando queries analíticas..."

echo ""
echo "--- Query 1: Top 5 operadoras com maior crescimento ---"
psql -d $DB_NAME -f "$SCRIPT_DIR/queries_analiticas/operadoras_crescimento.sql"

echo ""
echo "--- Query 2: Distribuição de despesas por UF ---"
psql -d $DB_NAME -f "$SCRIPT_DIR/queries_analiticas/distribuicao_despesas_uf.sql"

echo ""
echo "--- Query 3: Operadoras acima da média em 2+ trimestres ---"
psql -d $DB_NAME -f "$SCRIPT_DIR/queries_analiticas/operadoras_despesas_acima_media.sql"

echo ""
echo "==================================="
echo "✓ TESTE COMPLETO FINALIZADO"
echo "==================================="