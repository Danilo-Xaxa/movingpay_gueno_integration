import os
import time
import logging
import requests
import shutil
import tarfile
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


load_dotenv()

for var in ['MOVINGPAY_EMAIL', 'MOVINGPAY_PASSWORD']:
    if not os.getenv(var):
        raise EnvironmentError(f"Variável de ambiente obrigatória não definida: {var}")

REQUEST_TIMEOUT = (10, 60)

EXPORTACOES_DIR = "exportacoes"
CAPTURAS_DIR = os.path.join(EXPORTACOES_DIR, "capturas")
FICHA_CADASTRAL_DIR = os.path.join(EXPORTACOES_DIR, "ficha_cadastral")

# === Configuração de logging ===
# Toda a execução será registrada no arquivo 'exportacoes.log'
logging.basicConfig(
    filename='exportacoes.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

@retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def request_get(url, **kwargs):
    try:
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logging.critical(f"Timeout em GET: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logging.critical(f"Falha em GET: {url} - {e}")
        raise

@retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def request_post(url, **kwargs):
    try:
        resp = requests.post(url, **kwargs)
        resp.raise_for_status()
        return resp
    except requests.exceptions.Timeout:
        logging.critical(f"Timeout em POST: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logging.critical(f"Falha em POST: {url} - {e}")
        raise

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

    resposta = request_post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()
    logging.info(f"Resposta do login: {resposta.text}")
    dados = resposta.json()
    return dados["access_token"], dados["customer_id"], dados["user_id"]

def solicitar_relatorio_capturas(token, customer_id, user_id, data_inicio, data_fim):
    """
    Solicita a geração de um relatório de capturas na MovingPay.
    O arquivo será gerado de forma assíncrona.
    """
    url = "https://api-reports.movingpay.com.br/csv/customized/gueno/capturas"
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
        "reportsSelected": [ 1 ],
        "onlyWithTransactions": "onlyWithTransactions",
        "distribuidorId": [],
        "codigoUnidadeNegocios": 0,
        "newReports": False,
        "extension": "csv",
        "tipoRelatorioGueno": "contabil_capturas"
    }

    resposta = request_post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()
    logging.info("Relatório de capturas solicitado com sucesso.")

def solicitar_relatorio_ficha_cadastral(token, customer_id, user_id, data_inicio, data_fim):
    """
    Solicita a geração de um relatório de ficha cadastral na MovingPay.
    O arquivo será gerado de forma assíncrona.
    """
    url = "https://api-reports.movingpay.com.br/csv/customized/gueno/estabelecimentos/ficha-cadastral"
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
        "codigoUnidadeNegocios": 0,
        "newReports": False,
        "extension": "csv",
        "tipoRelatorioGueno": "ficha_cadastral"
    }

    resposta = request_post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()
    logging.info("Relatório de ficha cadastral solicitado com sucesso.")

def buscar_arquivo_compativel(token, customer_id, prefixo):
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

    resposta = request_get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()
    arquivos = resposta.json().get("data", [])

    # Filtra arquivos válidos
    arquivos_validos = [
        arq for arq in arquivos
        if arq["arquivo"].startswith(prefixo)
        and arq["arquivo"].endswith(".tar.gz")
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
    os.makedirs(destino, exist_ok=True)
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

    resposta = request_get(url_geracao, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()
    dados = resposta.json()

    url_s3 = dados.get("url")
    if not url_s3:
        raise Exception("URL de download da S3 não encontrada.")

    resposta_s3 = request_get(url_s3, timeout=REQUEST_TIMEOUT)
    resposta_s3.raise_for_status()

    caminho = os.path.join(destino, nome)
    with open(caminho, "wb") as f:
        f.write(resposta_s3.content)

    logging.info(f"Arquivo baixado com sucesso: {nome}")
    return caminho

def extrair_e_limpar(caminho_tar_gz, destino="exportacoes"):
    """
    Extrai o único arquivo .csv contido no .tar.gz para a pasta destino.
    Antes de extrair, remove qualquer .csv existente para evitar duplicidade.
    Garante segurança contra path traversal.
    """
    # Garante que a pasta exista
    os.makedirs(destino, exist_ok=True)

    # Limpa CSVs antigos na pasta
    for f in os.listdir(destino):
        if f.lower().endswith(".csv"):
            try:
                os.remove(os.path.join(destino, f))
                logging.info(f"Arquivo CSV antigo removido: {f}")
            except Exception as e:
                logging.warning(f"Falha ao remover {f}: {e}")

    # Extrai apenas o CSV seguro
    with tarfile.open(caminho_tar_gz, "r:gz") as tar:
        # Procura membros seguros e que sejam CSV
        membros = [
            m for m in tar.getmembers()
            if m.name.lower().endswith(".csv")
            and not os.path.isabs(m.name)
            and ".." not in os.path.normpath(m.name).split(os.sep)
        ]

        if not membros:
            raise Exception("Nenhum arquivo CSV válido encontrado no .tar.gz.")

        membro = membros[0]
        nome_destino = os.path.join(destino, os.path.basename(membro.name))

        # Extração segura: abre o membro como stream e grava no disco
        with tar.extractfile(membro) as src, open(nome_destino, "wb") as out:
            shutil.copyfileobj(src, out)

        logging.info(f"Arquivo CSV extraído com sucesso para: {nome_destino}")

    # Remove o .tar.gz baixado
    os.remove(caminho_tar_gz)

def main():
    """
    Pipeline principal:
    1. Define o intervalo de datas
    2. Autentica na MovingPay
    3. Solicita geração dos relatórios (ficha cadastral primeiro)
    4. Aguarda 60 segundos
    5. Busca os arquivos gerados
    6. Baixa e extrai os CSVs nas subpastas corretas
    """
    try:
        logging.info("Iniciando exportações da MovingPay...")

        data_inicio, data_fim = obter_datas_referencia()
        token, customer_id, user_id = autenticar()

        # Ficha cadastral primeiro
        solicitar_relatorio_ficha_cadastral(token, customer_id, user_id, data_inicio, data_fim)
        solicitar_relatorio_capturas(token, customer_id, user_id, data_inicio, data_fim)

        logging.info("Aguardando 60 segundos antes de buscar os relatórios...")
        time.sleep(60)

        # Ficha cadastral
        arquivo_ficha_cadastral = buscar_arquivo_compativel(token, customer_id, "GUENO.FICHACADASTRAL")
        if arquivo_ficha_cadastral:
            logging.info(f"Arquivo de ficha cadastral encontrado: {arquivo_ficha_cadastral['arquivo']} (ID: {arquivo_ficha_cadastral['id']})")
            caminho = baixar_arquivo(token, arquivo_ficha_cadastral, customer_id, destino=FICHA_CADASTRAL_DIR)
            extrair_e_limpar(caminho, destino=FICHA_CADASTRAL_DIR)
        else:
            logging.warning("Nenhum arquivo de ficha cadastral encontrado.")

        # Capturas
        arquivo_capturas = buscar_arquivo_compativel(token, customer_id, "GUENO.CAPTURAS")
        if arquivo_capturas:
            logging.info(f"Arquivo de capturas encontrado: {arquivo_capturas['arquivo']} (ID: {arquivo_capturas['id']})")
            caminho = baixar_arquivo(token, arquivo_capturas, customer_id, destino=CAPTURAS_DIR)
            extrair_e_limpar(caminho, destino=CAPTURAS_DIR)
        else:
            raise Exception("Nenhum arquivo de capturas compatível encontrado.")

        logging.shutdown()

    except Exception as e:
        logging.critical(f"Erro durante a execução: {e}")
        logging.shutdown()

# Ponto de entrada do script
if __name__ == "__main__":
    main()
