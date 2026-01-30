import requests
from bs4 import BeautifulSoup
import re
import os

URL_ANS = "https://dadosabertos.ans.gov.br/FTP/PDA/"
ENDPOINT_DEMONSTRACOES = "/demonstracoes_contabeis"


def obter_html(url):
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.text
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar {url}")
        print(e)
        return None
    

def obter_anos_disponiveis():
    url = f"{URL_ANS}{ENDPOINT_DEMONSTRACOES}"
    html = obter_html(url)
    anos = []

    if not html:
        return anos
    
    soup = BeautifulSoup(html, "html.parser")

    for link in soup.find_all('a'):
        try:
            href = link.get("href", "")
           
            if href.endswith("/") and href[:-1].isnumeric():
                ano = int(href[:-1])
                anos.append(ano)
        
        except Exception as e:
            print(f"Erro ao processar link: {link}")
            print(e)
    
    anos.sort()
    return anos


def obter_zips_de_uma_pasta(url):
    html = obter_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    zips = []

    for link in soup.find_all("a"):
        href = link.get("href", "")

        if href.endswith(".zip"):
            zips.append(href)

        elif href.endswith("/") and href not in ("../",) and not href.startswith("/"):
            sub_url = f"{url}/{href}".replace("//", "/").replace("https:/", "https://")
            zips.extend(obter_zips_de_uma_pasta(sub_url))

    return zips


def extrair_ano_trimestre(nome_arquivo, ano_padrao=None):
    nome = nome_arquivo.lower().replace(".zip", "")
    partes = nome.replace("-", "_").replace(".", "_").split("_")

    ano = None
    trimestre = None

    for i, parte in enumerate(partes):
        # Ano
        if parte.isnumeric() and len(parte) == 4:
            ano = parte

        # Formato: 1t, 2t
        if len(parte) == 2 and parte[0].isdigit() and parte[1] == "t":
            trimestre = parte[0]

        # Formato: 1t2023
        if parte.isnumeric() is False and "t" in parte:
            idx = parte.find("t")
            if idx == 1 and parte[0].isdigit():
                trimestre = parte[0]
                resto = parte[2:]
                if resto.isnumeric() and len(resto) == 4:
                    ano = resto

        # Formato: 3_trimestre
        if parte.isnumeric() and i + 1 < len(partes):
            if partes[i + 1].startswith("trimestre"):
                trimestre = parte

    if ano is None and ano_padrao is not None:
        ano = str(ano_padrao)

    return ano, trimestre


def obter_ultimos_tres_trimestres_global():
    anos = obter_anos_disponiveis()
    anos.sort(reverse=True)  # Do mais recente para o mais antigo
    
    todos_trimestres = []
    
    for ano in anos:
        url = f"{URL_ANS}{ENDPOINT_DEMONSTRACOES}/{ano}"
        zips = obter_zips_de_uma_pasta(url)
        
        trimestres_ano = {}
        for arquivo in zips:
            ano_arquivo, trimestre = extrair_ano_trimestre(arquivo, ano)
            if ano_arquivo == str(ano) and trimestre:
                trimestres_ano.setdefault(trimestre, []).append(arquivo)
        
        for trim, arquivos in trimestres_ano.items():
            todos_trimestres.append((ano, trim, arquivos))
        
        if len(todos_trimestres) >= 3:
            break
    
    # Ordenar por ano e trimestre (decrescente) e pegar os 3 primeiros
    todos_trimestres.sort(key=lambda x: (int(x[0]), int(x[1])), reverse=True)
    return todos_trimestres[:3]


def baixar_zip(url, pasta_destino="../data/zips"):
    os.makedirs(pasta_destino, exist_ok = True)

    nome_arquivo = url.split("/")[-1]
    caminho = os.path.join(pasta_destino, nome_arquivo)

    resposta = requests.get(url, stream = True)
    resposta.raise_for_status()

    with open(caminho, "wb") as f:
        for chunk in resposta.iter_content(chunk_size = 8192):
            if chunk:
                f.write(chunk)
    

def main():
    print("Buscando últimos 3 trimestres disponíveis globalmente...")
    trimestres = obter_ultimos_tres_trimestres_global()

    if not trimestres:
        print("Nenhum trimestre disponível encontrado.")
        return

    print(f"\nÚltimos 3 trimestres encontrados:")
    for ano, trimestre, arquivos in trimestres:
        print(f"{trimestre}T{ano} - {len(arquivos)} arquivo(s)")
    
    print("\nIniciando download dos arquivos:")

    for ano, trimestre, arquivos in trimestres:
        print(f"Trimestre {trimestre}T{ano}:")
        
        for arquivo in arquivos:
            print(f"  Baixando: {arquivo}")
            url_arquivo = f"{URL_ANS}{ENDPOINT_DEMONSTRACOES}/{ano}/{arquivo}"
            try:
                baixar_zip(url_arquivo)
                print(f"Baixado com sucesso")
            except Exception as e:
                print(f"Erro ao baixar {arquivo}: {e}")