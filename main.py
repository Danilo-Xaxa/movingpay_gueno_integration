import subprocess
import logging
import traceback
import sys
from datetime import datetime

# === Configuração centralizada de LOG ===
# Cria um arquivo 'main.log' com logs formatados, codificados em UTF-8
# Os níveis registrados vão de INFO até CRITICAL
log_path = "main.log"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)

def executar_script(nome_script):
    """
    Executa um script Python externo de forma segura e registra
    todas as saídas (stdout, stderr) no log.

    Parâmetros:
    - nome_script (str): Nome do script Python a ser executado.
    """
    try:
        logging.info(f"Iniciando execução do script: {nome_script}")

        # Executa o script com o mesmo Python que está rodando este arquivo
        resultado = subprocess.run(
            [sys.executable, nome_script],
            capture_output=True,     # Captura stdout e stderr
            text=True,               # Trata a saída como texto (não binário)
            encoding="utf-8",        # Usa codificação UTF-8
            check=True               # Lança exceção se o código de retorno for != 0
        )

        logging.info(f"Script {nome_script} executado com sucesso.")

        # Se houver algo na saída padrão, registra no log
        if resultado.stdout:
            logging.info(f"[stdout - {nome_script}]\n{resultado.stdout.strip()}")

        # Se houver algo na saída de erro, registra como warning
        if resultado.stderr:
            logging.warning(f"[stderr - {nome_script}]\n{resultado.stderr.strip()}")

    except subprocess.CalledProcessError as e:
        # Se o script falhar (retornar erro), registra todos os detalhes
        logging.critical(f"Erro ao executar {nome_script}: {e}")
        logging.critical(f"[stderr - {nome_script}]\n{e.stderr.strip() if e.stderr else 'Nenhuma saída de erro'}")
        logging.critical("Stack trace:")
        logging.critical(traceback.format_exc())
        raise  # Relevanta o erro para ser tratado no nível principal (main)

def main():
    """
    Executa os dois scripts do pipeline em sequência:
    1. exportar_transacoes.py — exporta arquivo da MovingPay
    2. importar_transacoes.py — importa arquivo para a Gueno

    Se qualquer etapa falhar, a execução é interrompida com logs detalhados.
    """
    logging.info(f"==== Início da execução integrada em {datetime.now().isoformat()} ====")

    try:
        executar_script("exportar_transacoes.py")
    except Exception:
        logging.critical("Execução interrompida por erro crítico ao EXPORTAR transações.")
        logging.critical("Traceback:")
        logging.critical(traceback.format_exc())
        return

    try:
        executar_script("importar_transacoes.py")
    except Exception:
        logging.critical("Execução interrompida por erro crítico ao IMPORTAR transações.")
        logging.critical("Traceback:")
        logging.critical(traceback.format_exc())
        return

    logging.info("==== Execução integrada finalizada com sucesso ====")
    

# Executa o pipeline apenas se este arquivo for o principal
if __name__ == "__main__":
    main()
