import os
import pandas as pd
import numpy as np
import zipfile
from ans_download import extrair_ano_trimestre


def carregar_csvs_trimestrais(pasta):
    arquivos = []
    for nome in os.listdir(pasta):
        if nome.lower().endswith(".csv"):
            arquivos.append(os.path.join(pasta, nome))
    return arquivos


def calcular_valor_despesa(df):
    df["VL_SALDO_INICIAL"] = (
        df["VL_SALDO_INICIAL"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )
    df["VL_SALDO_FINAL"] = (
        df["VL_SALDO_FINAL"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )
    df["ValorDespesas"] = df["VL_SALDO_FINAL"] - df["VL_SALDO_INICIAL"]
    return df


def verificar_inconsistencia_trimestre(df, ano_arquivo, trimestre_arquivo):
    if 'DATA' not in df.columns:
        return pd.Series([False] * len(df))
    
    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
    df["ano_data"] = df["DATA"].dt.year
    df["trimestre_data"] = df["DATA"].dt.quarter
    inconsistencias = (df["ano_data"] != int(ano_arquivo)) | (df["trimestre_data"] != int(trimestre_arquivo))
    return inconsistencias


def consolidar_trimestres(pasta_csvs, pasta_saida):
    os.makedirs(pasta_saida, exist_ok=True)
    todos = []
    arquivos = carregar_csvs_trimestrais(pasta_csvs)
    
    print(f"CONSOLIDANDO DADOS DOS 3 TRIMESTRES")
    
    for caminho in arquivos:
        nome_arquivo = os.path.basename(caminho)
        ano, trimestre = extrair_ano_trimestre(nome_arquivo)
        
        print(f"{nome_arquivo} (Ano: {ano}, Trimestre: {trimestre})")
        
        df = pd.read_csv(caminho, sep=";", encoding="utf-8")
        df = calcular_valor_despesa(df)
        
        df_final = pd.DataFrame()
        df_final["CNPJ"] = df["REG_ANS"]
        df_final["RazaoSocial"] = np.nan
        df_final["Trimestre"] = trimestre
        df_final["Ano"] = ano
        df_final["ValorDespesas"] = df["ValorDespesas"]
        
        valores_suspeitos = df_final["ValorDespesas"] <= 0
        datas_suspeitas = verificar_inconsistencia_trimestre(df, ano, trimestre)
        df_final["Suspeito"] = valores_suspeitos | datas_suspeitas
        todos.append(df_final)
    
    # Concatenar
    consolidado = pd.concat(todos, ignore_index=True)    
    
    caminho_csv = os.path.join(pasta_saida, "consolidado_despesas.csv")
    consolidado.to_csv(caminho_csv, index=False, sep=";")
    print(f"CSV salvo: {caminho_csv}\n")
    
    return caminho_csv


def compactar_csv(caminho_csv, pasta_saida):
    if caminho_csv is None:
        return None
    
    zip_path = os.path.join(pasta_saida, "consolidado_despesas.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(caminho_csv, arcname="consolidado_despesas.csv")
    
    print(f"ZIP criado: {zip_path}\n")
    return zip_path


def main():
    pasta_csvs = "../data/extraidos"
    pasta_saida = "../data/output"
    
    caminho_csv = consolidar_trimestres(pasta_csvs, pasta_saida)
    
    if caminho_csv:
        zip_final = compactar_csv(caminho_csv, pasta_saida)    
    else:
        print("\nPROCESSO FALHOU\n")
