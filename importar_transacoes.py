import os
import logging
import requests
from dotenv import load_dotenv

# === Configuração de logging ===
# Cria um log em 'importacoes.log' para registrar autenticação, envio de arquivo
# e eventuais falhas na importação para a Gueno.
logging.basicConfig(
    filename='importacoes.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Carrega variáveis de ambiente do .env (GUENO_EMAIL, GUENO_PASSWORD, GUENO_CLIENT_KEY)
load_dotenv()

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

    resposta = requests.post(url, json=payload, headers=headers)
    resposta.raise_for_status()  # Lança erro se status != 200
    dados = resposta.json()

    logging.info("Autenticado com sucesso na API da Gueno.")
    return dados["access_token"]

def enviar_arquivo_gueno(token, caminho_arquivo_csv):
    """
    Envia o arquivo CSV para a API da Gueno responsável por importar transações.

    Parâmetros:
    - token (str): token JWT retornado por autenticar_gueno()
    - caminho_arquivo_csv (str): caminho absoluto ou relativo do arquivo .csv a ser enviado

    Requer:
    - GUENO_CLIENT_KEY no .env
    """
    url = "https://api-gueno.prd.gueno.com/api/kyt-import/transactions"

    headers = {
        "Authorization": f"Bearer {token}",
        "client-key": os.getenv("GUENO_CLIENT_KEY"),
        "x-gueno-type-product": "DASHBOARD"
    }

    # Abre o arquivo em modo binário e o envia via multipart/form-data
    with open(caminho_arquivo_csv, 'rb') as f:
        files = {
            'file': (os.path.basename(caminho_arquivo_csv), f, 'text/csv')
        }
        resposta = requests.post(url, headers=headers, files=files)

    if resposta.status_code == 200:
        logging.info("Arquivo enviado com sucesso para a Gueno.")
    else:
        logging.critical(f"Erro ao enviar arquivo para a Gueno: {resposta.status_code} - {resposta.text}")
        resposta.raise_for_status()

def main():
    """
    Pipeline principal da importação:
    1. Encontra o primeiro arquivo .csv dentro da pasta 'exportacoes/'
    2. Autentica na Gueno
    3. Envia o arquivo para a API de importação
    """
    try:
        # Pega o primeiro arquivo .csv encontrado na pasta exportacoes/
        nome_arquivo_csv = next(
            f for f in os.listdir("exportacoes") if f.lower().endswith(".csv")
        )
        caminho_arquivo_csv = os.path.join("exportacoes", nome_arquivo_csv)

        logging.info(f"Iniciando envio do arquivo: {nome_arquivo_csv}")

        token = autenticar_gueno()
        enviar_arquivo_gueno(token, caminho_arquivo_csv)

    except StopIteration:
        # Nenhum .csv encontrado
        logging.critical("Nenhum arquivo CSV encontrado na pasta 'exportacoes/'.")
    except Exception as e:
        # Qualquer outra exceção (autenticação, rede, etc.)
        logging.critical(f"Erro durante a importação para a Gueno: {e}")

# Roda o main apenas se este arquivo for o executado diretamente
if __name__ == "__main__":
    main()
