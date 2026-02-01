import sys
import traceback

import validar_dados
import enriquecer_dados
import agregar_dados


def main():
    print("=" * 70)
    print("PIPELINE DE VALIDAÇÃO, ENRIQUECIMENTO E AGREGAÇÃO - ANS")
    print("=" * 70)
    print()

    try:
        # Etapa 1: Validação dos dados
        print("ETAPA 1/3: VALIDAÇÃO DOS DADOS")
        print("-" * 70)
        validar_dados.main()
        print()

        # Etapa 2: Enriquecimento com cadastro da ANS
        print("ETAPA 2/3: ENRIQUECIMENTO DOS DADOS")
        print("-" * 70)
        enriquecer_dados.main()
        print()

        # Etapa 3: Agregação e compactação
        print("ETAPA 3/3: AGREGAÇÃO E COMPACTAÇÃO")
        print("-" * 70)
        agregar_dados.main()
        print()

        # Resumo final
        print("=" * 70)
        print("PIPELINE EXECUTADO COM SUCESSO!")
        print("=" * 70)
        print("\nArquivos gerados:")
        print("../data/validado/dados_validados_completo.csv")
        print("../data/enriquecido/dados_enriquecidos.csv")
        print("../data/agregado/despesas_agregadas.csv")
        print("../data/zip/Teste_Vinicius_Castellani_Tonello.zip")
        print("=" * 70)
        print()

    except Exception as e:
        print()
        print("=" * 70)
        print("ERRO NO PIPELINE")
        print("=" * 70)
        print(f"Erro: {e}")
        print()
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
