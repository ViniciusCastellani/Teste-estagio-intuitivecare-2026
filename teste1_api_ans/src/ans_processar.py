import zipfile
import os
import pandas as pd
from ans_download import extrair_ano_trimestre


def extrair_arquivos(caminho_zip, pasta_output):
    os.makedirs(pasta_output, exist_ok = True)

    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(pasta_output)
    
    arquivos_extraidos = []
    
    for f in os.listdir(pasta_output):
        caminho_completo = os.path.join(pasta_output, f)
        if os.path.isfile(caminho_completo):
            arquivos_extraidos.append(caminho_completo)

    return arquivos_extraidos


def ler_arquivo(caminho):
    try:
        if caminho.endswith(".xlsx") or caminho.endswith(".xls"):
            return pd.read_excel(caminho)
        else:
            for sep in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(caminho, sep = sep, encoding = "utf-8")
                    if len(df.columns) > 1:
                        return df
                except:
                    continue
            return pd.read_csv(caminho, sep = None, engine = "python", encoding = "utf-8")
    except Exception as e:
        print(f"Erro ao ler {caminho}: {e}")
        return None


def eh_arquivo_despesas_eventos(df):
    for coluna in df.columns:
        if df[coluna].dtype == "object":
            texto = ' '.join(df[coluna].astype(str).values)
            texto = texto.lower().replace("/", " ").replace("-", " ")
            
            if ("despesa" in texto or "despesas" in texto) and "evento" in texto and "sinistro" in texto:
                return True
    return False


def normalizar_colunas(df):
    novo_nome = {}

    for col in df.columns:
        col_lower = str(col).lower().strip()

        if 'data' in col_lower or col_lower.startswith('dt'):
            novo_nome[col] = 'data'

        elif 'conta' in col_lower:
            novo_nome[col] = 'codigo_conta'

        elif 'descricao' in col_lower or 'desc' in col_lower:
            novo_nome[col] = 'descricao'

        elif 'reg' in col_lower and 'ans' in col_lower:
            novo_nome[col] = 'registro_ans'

        elif 'saldo' in col_lower and 'inicial' in col_lower:
            novo_nome[col] = 'saldo_inicial'

        elif 'saldo' in col_lower and 'final' in col_lower:
            novo_nome[col] = 'saldo_final'

    return df.rename(columns=novo_nome)


def processar_arquivos(lista_arquivos, pasta_saida="../data/csv_trimestres"):
    os.makedirs(pasta_saida, exist_ok=True)

    for arquivo in lista_arquivos:
        df = ler_arquivo(arquivo)

        if df is None:
            continue

        if eh_arquivo_despesas_eventos(df):
            df = normalizar_colunas(df)
            ano, trimestre = extrair_ano_trimestre(os.path.basename(arquivo))
            
            if ano and trimestre:
                nome_csv = f"eventos_sinistros_{ano}_{trimestre}T.csv"
            else:
                nome_csv = os.path.splitext(os.path.basename(arquivo))[0] + ".csv"
            
            caminho_csv = os.path.join(pasta_saida, nome_csv)
            
            df.to_csv(caminho_csv, index=False)


def main():
    pasta_zip = "../data/zips"
    pasta_destino = "../data/extraidos"
    todos_arquivos_extraidos = []

    for nome_arquivo in os.listdir(pasta_zip):
        caminho_arquivo = os.path.join(pasta_zip, nome_arquivo)

        if os.path.isfile(caminho_arquivo) and nome_arquivo.lower().endswith(".zip"):
            arquivos_extraidos = extrair_arquivos(caminho_arquivo, pasta_destino)
            todos_arquivos_extraidos.extend(arquivos_extraidos)

    processar_arquivos(todos_arquivos_extraidos)