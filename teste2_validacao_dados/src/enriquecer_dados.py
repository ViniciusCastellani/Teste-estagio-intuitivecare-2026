import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import traceback

URL_OPERADORAS_ANS = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
CAMINHO_CONSOLIDADO = "../data/validado/dados_validados_completo.csv"
CAMINHO_OPERADORAS = "../data/raw/operadoras_ativas_ans.csv"
PASTA_SAIDA = "../data/enriquecido"


def obter_html(url):
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.text
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar {url}: {e}")
        return None


def baixar_csv_operadoras(caminho_destino):
    pasta = os.path.dirname(caminho_destino)
    os.makedirs(pasta, exist_ok=True)
    
    html = obter_html(URL_OPERADORAS_ANS)
    
    if not html:
        raise RuntimeError("Não foi possível acessar o site da ANS.")
    
    soup = BeautifulSoup(html, "html.parser")
    nome_arquivo = None
    
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.lower().endswith(".csv"):
            nome_arquivo = href
            break
    
    if not nome_arquivo:
        raise RuntimeError("Não foi possível localizar o CSV de operadoras.")
    
    url_arquivo = f"{URL_OPERADORAS_ANS}{nome_arquivo}"
    print(f"Baixando: {url_arquivo}")
    
    resposta = requests.get(url_arquivo, stream=True)
    resposta.raise_for_status()
    
    with open(caminho_destino, "wb") as f:
        for chunk in resposta.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    print(f"Arquivo baixado: {caminho_destino}")


def carregar_csv_consolidado(caminho_csv):
    if not os.path.exists(caminho_csv):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_csv}")
    
    print(f"Carregando dados validados...")
    df = pd.read_csv(caminho_csv, sep=";")
    print(f"{len(df):,} registros")
    
    return df


def carregar_csv_operadoras(caminho_csv):
    if not os.path.exists(caminho_csv):
        baixar_csv_operadoras(caminho_csv)
    
    print(f"Carregando cadastro de operadoras...")
    
    try:
        df = pd.read_csv(caminho_csv, sep=";", dtype=str)
        print(f"  {len(df):,} operadoras")
        return df
    except Exception as e:
        print(f"ERRO {e}")
        
    raise RuntimeError("Não foi possível ler o arquivo de operadoras")


def detectar_colunas_cadastro(df_operadoras):
    colunas = df_operadoras.columns.tolist()
    
    col_registro = None
    for col in colunas:
        if 'REGISTRO' in col.upper():
            col_registro = col
            break
    
    if not col_registro:
        raise ValueError(f"Coluna de Registro ANS não encontrada\nColunas: {colunas}")
    
    col_cnpj = None
    col_razao = None
    col_modalidade = None
    col_uf = None
    
    for col in colunas:
        col_upper = col.upper()
        if 'CNPJ' in col_upper and col != col_registro:
            col_cnpj = col
        elif 'RAZAO' in col_upper or 'SOCIAL' in col_upper:
            col_razao = col
        elif 'MODALIDADE' in col_upper:
            col_modalidade = col
        elif col_upper == 'UF':
            col_uf = col
    
    return {
        'registro': col_registro,
        'cnpj': col_cnpj,
        'razao': col_razao,
        'modalidade': col_modalidade,
        'uf': col_uf
    }


def fazer_join(df_consolidado, df_operadoras, colunas_cadastro):
    df_consolidado["CNPJ_norm"] = df_consolidado["CNPJ"].astype(str).str.strip()
    df_operadoras["Registro_ANS_norm"] = df_operadoras[colunas_cadastro['registro']].astype(str).str.strip()
    
    colunas_join = ["Registro_ANS_norm"]
  
    for key in ['cnpj', 'razao', 'modalidade', 'uf']:
        if colunas_cadastro[key]:
            colunas_join.append(colunas_cadastro[key])
    
    df_final = df_consolidado.merge(
        df_operadoras[colunas_join],
        left_on="CNPJ_norm",
        right_on="Registro_ANS_norm",
        how="left"
    )
    
    return df_final


def renomear_colunas(df, colunas_cadastro):
    renomear = {
        "Registro_ANS_norm": "Registro_ANS"
    }
    
    if colunas_cadastro['cnpj']:
        renomear[colunas_cadastro['cnpj']] = "CNPJ_Real"
    if colunas_cadastro['razao']:
        renomear[colunas_cadastro['razao']] = "RazaoSocial_Cadastro"
    if colunas_cadastro['modalidade']:
        renomear[colunas_cadastro['modalidade']] = "Modalidade"
    if colunas_cadastro['uf']:
        renomear[colunas_cadastro['uf']] = "UF"
    
    df.rename(columns=renomear, inplace=True)
    
    if "CNPJ_norm" in df.columns:
        df.drop(columns=["CNPJ_norm"], inplace=True)
    
    return df


def atualizar_razao_social(df):
    if "RazaoSocial_Cadastro" in df.columns:
        if "RazaoSocial" in df.columns:
            df["RazaoSocial"] = df["RazaoSocial_Cadastro"].combine_first(df["RazaoSocial"])
        else:
            df["RazaoSocial"] = df["RazaoSocial_Cadastro"]
    
    return df


def remover_colunas_validacao(df):
    colunas_para_remover = [
        'Registro_ANS_Valido', 
        'Valor_Valido', 
        'Validacao_OK',
        'RazaoSocial_Valida',
        'Sem_Match_Cadastro',
        'RazaoSocial_Cadastro'
    ]
    
    df.drop(columns=[c for c in colunas_para_remover if c in df.columns], inplace=True)
    
    return df


def resolver_colunas_duplicadas(df):
    colunas_x = [c for c in df.columns if c.endswith('_x')]
    
    for col_x in colunas_x:
        col_base = col_x[:-2]  # Remove '_x'
        col_y = col_base + '_y'
        
        if col_y in df.columns:
            # Priorizar _y (dados do cadastro)
            df[col_base] = df[col_y].combine_first(df[col_x])
            df.drop(columns=[col_x, col_y], inplace=True)
    
    return df


def corrigir_cnpj_notacao_cientifica(df):
    if "CNPJ_Real" in df.columns:
        df["CNPJ_Real"] = df["CNPJ_Real"].astype(str)
    return df


def limpar_coluna_cnpj_duplicada(df):
    if 'CNPJ_x' in df.columns and 'CNPJ_y' in df.columns:
        df['CNPJ'] = df['CNPJ_y'].combine_first(df['CNPJ_x'])
        df.drop(columns=['CNPJ_x', 'CNPJ_y'], inplace=True)

    return df


def reorganizar_colunas(df):
    colunas_ordem = []
    
    for col in ['Registro_ANS', 'CNPJ', 'CNPJ_Real', 'RazaoSocial']:
        if col in df.columns:
            colunas_ordem.append(col)
    
    for col in ['Modalidade', 'UF']:
        if col in df.columns:
            colunas_ordem.append(col)
    
    for col in ['Ano', 'Trimestre']:
        if col in df.columns:
            colunas_ordem.append(col)
    
    for col in ['Valor_Despesas', 'Suspeito']:
        if col in df.columns:
            colunas_ordem.append(col)
    
    colunas_restantes = [c for c in df.columns if c not in colunas_ordem]
    colunas_ordem.extend(colunas_restantes)
    
    df = df[colunas_ordem]
    
    return df


def enriquecer_dados(df_consolidado, df_operadoras):
    print("Enriquecendo dados...")
    
    colunas_cadastro = detectar_colunas_cadastro(df_operadoras)
    df_final = fazer_join(df_consolidado, df_operadoras, colunas_cadastro)
    df_final = renomear_colunas(df_final, colunas_cadastro) 
    df_final = atualizar_razao_social(df_final)
    
    print("Limpando colunas...")
    
    df_final = remover_colunas_validacao(df_final)
    df_final = resolver_colunas_duplicadas(df_final)
    df_final = corrigir_cnpj_notacao_cientifica(df_final)
    df_final = limpar_coluna_cnpj_duplicada(df_final)
    
    df_final = reorganizar_colunas(df_final)
    
    print(f"  {len(df_final):,} registros")
    print(f"  {len(df_final.columns)} colunas: {', '.join(df_final.columns[:8])}...")
    
    return df_final


def salvar_csv(df, pasta_saida, nome_arquivo):
    os.makedirs(pasta_saida, exist_ok=True)
    caminho = os.path.join(pasta_saida, nome_arquivo)
    df.to_csv(caminho, sep=";", index=False)
    print(f"Salvo: {caminho}")
    return caminho


def main():
    try:
        df_consolidado = carregar_csv_consolidado(CAMINHO_CONSOLIDADO)
        df_operadoras = carregar_csv_operadoras(CAMINHO_OPERADORAS)
        df_enriquecido = enriquecer_dados(df_consolidado, df_operadoras)
        salvar_csv(df_enriquecido, PASTA_SAIDA, "dados_enriquecidos.csv")
        
        print("\nConcluído!")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()