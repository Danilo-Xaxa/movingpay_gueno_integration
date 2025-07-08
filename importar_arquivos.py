import os
import logging
import requests
import time
import traceback
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


load_dotenv()

for var in ['GUENO_EMAIL', 'GUENO_PASSWORD', 'GUENO_CLIENT_KEY']:
    if not os.getenv(var):
        raise EnvironmentError(f"Variável de ambiente obrigatória não definida: {var}")    

REQUEST_TIMEOUT = (10, 60)

# === Configuração de logging ===
# Cria um log em 'importacoes.log' para registrar autenticação, envio de arquivo
# e eventuais falhas na importação para a Gueno.
logging.basicConfig(
    filename='importacoes.log',
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
    
def autenticar_gueno():
    """
    Realiza login na API da Gueno e retorna um token JWT válido.

    Requer:
    - GUENO_EMAIL e GUENO_PASSWORD no arquivo .env

    Retorna:
    - access_token (str): usado para autenticar chamadas subsequentes na API da Gueno
    """
    url = "https://api-gueno.prd.gueno.com/api/auth/login"
    headers = {
        "Accept": "application/json, application/xml",
        "Content-Type": "application/json"
    }
    payload = {
        "email": os.getenv("GUENO_EMAIL"),
        "password": os.getenv("GUENO_PASSWORD")
    }

    resposta = request_post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    resposta.raise_for_status()  # Lança erro se status != 200
    dados = resposta.json()

    logging.info("Autenticado com sucesso na API da Gueno.")
    return dados["access_token"]

def enviar_arquivo_gueno(token, caminho_arquivo_csv, tipo):
    """
    Envia o arquivo CSV para a API da Gueno responsável por importar arquivos.

    Parâmetros:
    - token (str): token JWT retornado por autenticar_gueno()
    - caminho_arquivo_csv (str): caminho absoluto ou relativo do arquivo .csv a ser enviado
    - tipo (str): 'transactions' ou 'users'
    """
    if tipo not in ["transactions", "users"]:
        raise ValueError("Tipo inválido. Use 'transactions' ou 'users'.")

    url = f"https://api-gueno.prd.gueno.com/api/kyt-import/{tipo}"

    headers = {
        "Authorization": f"Bearer {token}",
        "client-key": os.getenv("GUENO_CLIENT_KEY"),
        "x-gueno-type-product": "DASHBOARD"
    }

    with open(caminho_arquivo_csv, 'rb') as f:
        files = {
            'file': (os.path.basename(caminho_arquivo_csv), f, 'text/csv')
        }
        resposta = request_post(url, headers=headers, files=files, timeout=REQUEST_TIMEOUT)

    if resposta.status_code in [200, 201]:
        logging.info(f"Arquivo de '{tipo}' importado na Gueno com sucesso.")
    else:
        logging.critical(f"Erro ao importar arquivo de '{tipo}' na Gueno: {resposta.status_code} - {resposta.text}")
        resposta.raise_for_status()

def obter_item_id_gueno(token, nome_arquivo_csv):
    """
    Faz GET na lista de imports da Gueno e retorna o _id do item
    cujo originalName bate com o CSV enviado.
    """
    url = "https://api-gueno.prd.gueno.com/api/kyt-import?page=0&limit=10"
    headers = {
        "Authorization": f"Bearer {token}",
        "client-key": os.getenv("GUENO_CLIENT_KEY"),
        "x-gueno-type-product": "DASHBOARD"
    }

    resposta = request_get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    dados = resposta.json()

    items = dados.get("data", {}).get("items", [])
    for item in items:
        if item.get("originalName") == nome_arquivo_csv:
            logging.info(f"Arquivo de capturas encontrado na lista de imports: ID {item['_id']}.")
            return item["_id"]

    raise Exception(f"Arquivo '{nome_arquivo_csv}' enviado não encontrado na lista de imports da Gueno.")

def processar_arquivo_gueno(token, item_id):
    """
    Dispara o processamento do arquivo já importado na Gueno via POST /verify.
    """
    now_ms = int(time.time() * 1000)
    url = f"https://api-gueno.prd.gueno.com/api/kyt/transactions/all/verify?itemId={item_id}&fromDate={now_ms}&toDate={now_ms}"

    headers = {
        "Authorization": f"Bearer {token}",
        "client-key": os.getenv("GUENO_CLIENT_KEY"),
        "content-type": "application/json",
        "x-gueno-type-product": "DASHBOARD"
    }

    resposta = request_post(url, headers=headers, json={}, timeout=REQUEST_TIMEOUT)

    if resposta.status_code in [200, 201]:
        logging.info(f"Processamento do arquivo de capturas realizado com sucesso: itemId {item_id}.")
    else:
        logging.critical(f"Erro ao processar arquivo na Gueno: {resposta.status_code} - {resposta.text}")
        resposta.raise_for_status()

def main():
    """
    Pipeline principal da importação:
    1. Busca os dois arquivos mais recentes em 'exportacoes/' (ficha cadastral e capturas)
    2. Autentica na Gueno
    3. Envia primeiro o arquivo de ficha cadastral (users)
    4. Se bem-sucedido, envia o arquivo de capturas (transactions)
    5. Busca o ID do arquivo de capturas importado
    6. Dispara o processamento do arquivo de capturas
    """
    try:
        if not os.path.isdir("exportacoes"):
            logging.critical("A pasta 'exportacoes/' não existe.")
            return

        csv_files = []
        for root, dirs, files in os.walk("exportacoes"):
            for f in files:
                if f.lower().endswith(".csv"):
                    csv_files.append(os.path.join(root, f))

        csv_files = sorted(
            csv_files,
            key=lambda f: os.path.getmtime(f),
            reverse=True
        )

        if len(csv_files) < 2:
            logging.critical("É necessário ter pelo menos dois arquivos CSV em 'exportacoes/': um para ficha cadastral (users) e outro para capturas (transactions).")
            logging.shutdown()
            raise FileNotFoundError

        caminho_captura_csv = csv_files[0]
        caminho_ficha_csv = csv_files[1]

        nome_captura_csv = os.path.basename(caminho_captura_csv)
        nome_ficha_csv = os.path.basename(caminho_ficha_csv)

        logging.info(f"Iniciando pipeline. Ficha Cadastral: {nome_ficha_csv} | Capturas: {nome_captura_csv}")

        # Autenticação
        token = autenticar_gueno()

        # Envia ficha cadastral primeiro
        try:
            logging.info(f"Enviando arquivo de ficha cadastral (users): {nome_ficha_csv}")
            enviar_arquivo_gueno(token, caminho_ficha_csv, tipo="users")
            logging.info("Importação de ficha cadastral concluída com sucesso.")
        except Exception as e:
            logging.critical(f"Falha ao importar arquivo de ficha cadastral: {e}")
            logging.critical("Abortando pipeline.")
            logging.shutdown()
            return

        # Só continua para capturas se ficha cadastral deu certo
        try:
            logging.info(f"Enviando arquivo de capturas (transactions): {nome_captura_csv}")
            enviar_arquivo_gueno(token, caminho_captura_csv, tipo="transactions")

            # Busca ID do arquivo de capturas importado
            item_id = obter_item_id_gueno(token, nome_captura_csv)

            # Dispara processamento do arquivo de capturas
            processar_arquivo_gueno(token, item_id)

            logging.info("Importação e processamento de capturas concluídos com sucesso.")
        except Exception as e:
            logging.critical(f"Falha ao importar ou processar arquivo de capturas: {e}")
            logging.critical("Stack trace:")
            logging.critical(traceback.format_exc())
            logging.shutdown()
            return

        logging.info("Pipeline completo concluído com sucesso!")
        logging.shutdown()

    except Exception as e:
        logging.critical(f"Erro geral na execução do pipeline: {e}")
        logging.critical("Stack trace:")
        logging.critical(traceback.format_exc())
        logging.shutdown()

# Roda o main apenas se este arquivo for o executado diretamente
if __name__ == "__main__":
    main()
