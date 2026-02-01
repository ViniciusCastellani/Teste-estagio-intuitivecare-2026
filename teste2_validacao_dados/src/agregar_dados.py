import os
import zipfile
import pandas as pd

DADOS_ENRIQUECIDOS = "../data/enriquecido/dados_enriquecidos.csv"
DADOS_AGREGADOS = "../data/agregado/despesas_agregadas.csv"
PASTA_SAIDA = "../data/zip"


def compactar_csv(caminho_csv, pasta_saida, nome_zip):
    if not caminho_csv:
        return None

    os.makedirs(pasta_saida, exist_ok = True)

    zip_path = os.path.join(pasta_saida, nome_zip)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(caminho_csv, arcname = os.path.basename(caminho_csv))    

    print(f"ZIP criado: {zip_path}")
    return zip_path


def carregar_dados(caminho):
    return pd.read_csv(caminho, sep=";")


def preparar_dados(df):
    df["Valor_Despesas"] = pd.to_numeric(df["Valor_Despesas"], errors = 'coerce')
    return df


def agregar_dados(df):
    agregados_df = df.groupby(["Razao_Social", "UF"], as_index = False)
    
    agregados_df = agregados_df.agg(
        Total_Despesas = ("Valor_Despesas", "sum"),
        Media_Despesas_Trimestre=("Valor_Despesas", "mean"),
        Desvio_Padrao_Despesas=("Valor_Despesas", "std")
    )

    agregados_df = agregados_df.sort_values(by = "Total_Despesas", ascending = False)

    return agregados_df


def salvar_dados(df, caminho):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    df.to_csv(caminho, sep=";", index=False, float_format = "%.2f")


def main():
    df = carregar_dados(DADOS_ENRIQUECIDOS)
    df = preparar_dados(df)
    df_agregado = agregar_dados(df)
    salvar_dados(df_agregado, DADOS_AGREGADOS)
    compactar_csv(DADOS_AGREGADOS, PASTA_SAIDA, "Teste_Vinicius_Castellani_Tonello.zip")


if __name__ == "__main__":
    main()