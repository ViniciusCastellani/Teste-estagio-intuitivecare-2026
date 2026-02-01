import sys
import os
import ans_download
import ans_processar
import ans_consolidar_trimestres
import traceback

def main():
    print("="*70)
    print("PIPELINE DE PROCESSAMENTO DE DADOS ANS")
    print("="*70)
    print()
    
    try:
        # Etapa 1: Download dos arquivos
        print("ETAPA 1/3: DOWNLOAD DOS ARQUIVOS ZIP")
        print("-"*70)
        ans_download.main()
        print()
        
        # Etapa 2: Extração e processamento
        print("ETAPA 2/3: EXTRAÇÃO E PROCESSAMENTO")
        print("-"*70)
        ans_processar.main()
        print()
        
        # Etapa 3: Consolidação dos trimestres
        print("ETAPA 3/3: CONSOLIDAÇÃO DOS TRIMESTRES")
        print("-"*70)
        ans_consolidar_trimestres.main()
        print()
        
        # Resumo final
        print("="*70)
        print("PIPELINE COMPLETO EXECUTADO COM SUCESSO!")
        print("="*70)
        print("\nArquivos gerados:")
        print("../data/zips/ - Arquivos ZIP baixados")
        print("../data/extraidos/ - Arquivos extraídos e filtrados")
        print("../data/output/consolidado_despesas.csv - CSV consolidado")
        print("../data/output/consolidado_despesas.zip - ZIP final")
        print("="*70)
        print()
        
    except Exception as e:
        print()
        print("="*70)
        print("ERRO NO PIPELINE")
        print("="*70)
        print(f"Erro: {e}")
        print()
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()