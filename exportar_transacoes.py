import os
import time
import logging
import requests
import shutil
import tarfile
from datetime import datetime, timedelta
from dotenv import load_dotenv

# === Carregamento de variáveis sensíveis (.env) ===
# MOVINGPAY_EMAIL e MOVINGPAY_PASSWORD são usados para login na API
load_dotenv()

# === Configuração de logging ===
# Toda a execução será registrada no arquivo 'exportacoes.log'
logging.basicConfig(
    filename='exportacoes.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def obter_datas_referencia():
    """
    Define o intervalo de datas com base no dia atual:
    - Segunda-feira: de sexta a domingo
    - Outros dias: apenas o dia anterior
    Retorna as datas no formato YYYY-MM-DD.
    """
    hoje = datetime.now().date()
    if hoje.weekday() == 0:  # Segunda
        inicio = hoje - timedelta(days=3)
        fim = hoje - timedelta(days=1)
    else:
        inicio = hoje - timedelta(days=1)
        fim = hoje - timedelta(days=1)
    return inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d")

def autenticar():
    """
    Autentica na API da MovingPay.
    Retorna:
    - token (str): para autenticação em chamadas futuras
    - customer_id e user_id: necessários para requisições de relatório
    """
    url = "https://api.movingpay.com.br/api/v3/acessar"
    headers = {
        "Content-Type": "application/json",
        "x-mvpay-origin": "web"
    }
    payload = {
        "email": os.getenv("MOVINGPAY_EMAIL"),
        "password": os.getenv("MOVINGPAY_PASSWORD")
    }

    resposta = requests.post(url, json=payload, headers=headers)
    resposta.raise_for_status()
    logging.info(f"Resposta do login: {resposta.text}")
    dados = resposta.json()
    return dados["access_token"], dados["customer_id"], dados["user_id"]

def solicitar_relatorio(token, customer_id, user_id, data_inicio, data_fim):
    """
    Solicita a geração de um relatório contábil na MovingPay.
    O arquivo será gerado de forma assíncrona.
    """
    url = "https://api-reports.movingpay.com.br/excel/contabil"
    headers = {
        "Authorization": f"Bearer {token}",
        "customer": str(customer_id),
        "x-mvpay-origin": "web"
    }

    payload = {
        "cancelToken": {"promise": {}},
        "startDate": f"{data_inicio} 00:00:00",
        "finishDate": f"{data_fim} 23:59:59",
        "customerId": customer_id,
        "userId": user_id,
        "acquirerId": "",
        "includesStatusCaptures": [],
        "removePixCaptures": False,
        "removeSplitCaptures": True,
        "reportsSelected": [],
        "onlyWithTransactions": "onlyWithTransactions",
        "distribuidorId": [],
        "codigoUnidadeNegocios": 0,
        "newReports": True,
        "extension": "csv"
    }

    resposta = requests.post(url, json=payload, headers=headers)
    resposta.raise_for_status()
    logging.info("Relatório contábil solicitado com sucesso.")

def buscar_arquivo_compativel(token, customer_id, data_inicio, data_fim):
    """
    Busca na API o arquivo .tar.gz compatível com o intervalo solicitado.
    Retorna o mais recente dentre os válidos.
    """
    hoje = datetime.now()
    ontem = hoje - timedelta(days=1)

    url = (
        f"https://api.movingpay.com.br/api/v3/arquivos?"
        f"start_date={ontem.strftime('%Y-%m-%d')}+00:00:00&"
        f"finish_date={hoje.strftime('%Y-%m-%d')}+23:59:59&"
        f"referencia=RELATORIOS&view_my_reports=1&limit=100"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "customer": str(customer_id),
        "x-mvpay-origin": "web"
    }

    resposta = requests.get(url, headers=headers)
    resposta.raise_for_status()
    arquivos = resposta.json().get("data", [])

    # Formato do nome esperado no nome do arquivo
    data_inicio_fmt = datetime.strptime(data_inicio, "%Y-%m-%d").strftime("%d.%m.%Y")
    data_fim_fmt = datetime.strptime(data_fim, "%Y-%m-%d").strftime("%d.%m.%Y")
    intervalo_str = f"{data_inicio_fmt}A{data_fim_fmt}"

    # Filtra arquivos válidos
    arquivos_validos = [
        arq for arq in arquivos
        if arq["arquivo"].startswith("CONTABIL")
        and arq["arquivo"].endswith(".tar.gz")
        and intervalo_str in arq["arquivo"]
    ]

    if not arquivos_validos:
        return None

    for arq in arquivos_validos:
        logging.info(f"Arquivo disponível: {arq['arquivo']}")

    return max(arquivos_validos, key=lambda a: a["id"])

def baixar_arquivo(token, arquivo, customer_id, destino="exportacoes"):
    """
    Baixa o arquivo compactado da S3 da MovingPay para a pasta destino.
    """
    os.makedirs(destino, exist_ok=True)  # Cria a pasta se não existir
    nome = arquivo["arquivo"]
    diretorio = arquivo["diretorio"].replace("/", "%2F")

    url_geracao = (
        f"https://api.movingpay.com.br/api/v3/arquivos/download"
        f"?nome={nome}&diretorio={diretorio}&disco=s3"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "customer": str(customer_id),
        "x-mvpay-origin": "web"
    }

    resposta = requests.get(url_geracao, headers=headers)
    resposta.raise_for_status()
    dados = resposta.json()

    url_s3 = dados.get("url")
    if not url_s3:
        raise Exception("URL de download da S3 não encontrada.")

    resposta_s3 = requests.get(url_s3)
    resposta_s3.raise_for_status()

    caminho = os.path.join(destino, nome)
    with open(caminho, "wb") as f:
        f.write(resposta_s3.content)

    logging.info(f"Arquivo baixado com sucesso: {nome}")
    return caminho

def extrair_e_limpar(caminho_tar_gz, destino="exportacoes"):
    """
    Extrai o arquivo .tar.gz diretamente para a pasta destino.
    Move o primeiro CSV encontrado da pasta 'capturas/' para a raiz da pasta.
    Em seguida, remove pastas residuais da extração.
    """
    # Extrai tudo
    with tarfile.open(caminho_tar_gz, "r:gz") as tar_gz:
        tar_gz.extractall(destino)

    os.remove(caminho_tar_gz)  # Remove o .tar.gz

    capturas_path = os.path.join(destino, "capturas")
    if not os.path.isdir(capturas_path) or not os.listdir(capturas_path):
        logging.warning("Pasta capturas/ está vazia ou não existe.")
        return

    # Move o primeiro CSV da pasta capturas/ para a pasta exportacoes/
    for item in os.listdir(capturas_path):
        if item.endswith(".csv"):
            csv_origem = os.path.join(capturas_path, item)
            csv_destino = os.path.join(destino, item)
            shutil.move(csv_origem, csv_destino)
            logging.info(f"Arquivo CSV movido: {item}")
            break

    # Remove todas as subpastas dentro de exportacoes/
    for nome in os.listdir(destino):
        path = os.path.join(destino, nome)
        if os.path.isdir(path):
            shutil.rmtree(path)

def main():
    """
    Pipeline principal:
    1. Define o intervalo de datas
    2. Autentica na MovingPay
    3. Solicita geração do relatório
    4. Aguarda 60 segundos
    5. Busca o arquivo gerado
    6. Baixa e extrai o CSV
    """
    try:
        logging.info("Iniciando exportação contábil da MovingPay...")

        data_inicio, data_fim = obter_datas_referencia()
        token, customer_id, user_id = autenticar()
        solicitar_relatorio(token, customer_id, user_id, data_inicio, data_fim)

        logging.info("Aguardando 60 segundos antes de buscar o relatório...")
        time.sleep(60)

        arquivo = buscar_arquivo_compativel(token, customer_id, data_inicio, data_fim)
        if not arquivo:
            raise Exception("Nenhum arquivo compatível encontrado.")

        logging.info(f"Arquivo contábil encontrado: {arquivo['arquivo']} (ID: {arquivo['id']})")
        caminho = baixar_arquivo(token, arquivo, customer_id)
        extrair_e_limpar(caminho)

    except Exception as e:
        logging.critical(f"Erro durante a execução: {e}")

# Ponto de entrada do script
if __name__ == "__main__":
    main()
