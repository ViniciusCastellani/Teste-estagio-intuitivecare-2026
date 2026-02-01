import os
import pandas as pd

def validar_registro_ans(reg_ans):
    reg_ans_str = str(reg_ans).strip()
    return reg_ans_str.isdigit() and len(reg_ans_str) == 6


def validar_valor_positivo(valor):
    try:
        return float(valor) > 0
    except (ValueError, TypeError):
        return False


def aplicar_validacoes(df):
    df["Registro_ANS_Valido"] = df["CNPJ"].apply(validar_registro_ans)
    df["Valor_Valido"] = df["Valor_Despesas"].apply(validar_valor_positivo)
    df["Validacao_OK"] = df["Registro_ANS_Valido"] & df["Valor_Valido"]
    return df


def main():
    caminho_csv = "../../teste1_api_ans/data/output/consolidado_despesas.csv"
    
    if not os.path.exists(caminho_csv):
        print(f"Arquivo não encontrado: {caminho_csv}")
        return

    df = pd.read_csv(caminho_csv, sep=";")
    df = aplicar_validacoes(df)

    pasta_saida = "../data/validado"
    os.makedirs(pasta_saida, exist_ok=True)

    # Salvar CSV completo com flags
    caminho_completo = os.path.join(pasta_saida, "dados_validados_completo.csv")
    df.to_csv(caminho_completo, sep=";", index=False)

    print(f"Arquivos salvos em: {pasta_saida}")


if __name__ == "__main__":
    main()
