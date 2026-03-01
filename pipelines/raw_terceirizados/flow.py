from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pathlib import Path
from datetime import datetime
import dotenv
import os
import sys
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import yaml
import logging
import time




# PIPELINE DATE
REF_DATE = datetime().strftime("%Y-%m")

# PATHS
ROOT_DIR = Path(__file__).resolve().parents[2] 
CONFIG_PATH = ROOT_DIR / "config" / "sync_gcs_config.yml"
DOWNLOAD_DIR = ROOT_DIR / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# PROJECT BUCKETS
RAW_BUCKET = "gs://dw-bucket-storage/raw"

# --- CONFIGURAÇÕES ---
BASE_URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/"
PAGE_SIZE = 20

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))  # Adiciona o diretório pai ao sys.path

MONTHS_MAP = {
    "01": "janeiro", "02": "fevereiro", "03": "marco", "04": "abril",
    "05": "maio", "06": "junho", "07": "julho", "08": "agosto",
    "09": "setembro", "10": "outubro", "11": "novembro", "12": "dezembro"
}

def load_config():
    # Tenta carregar do caminho absoluto ancorado na raiz
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config não encontrada em: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_secure_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
    })
    retries = Retry(total=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def parse_human_input(user_input):
    year_match = re.search(r'(\d{4})', user_input)
    # Remove o ano da string para não confundir o mês com os dígitos do ano
    clean_input = user_input.replace(year_match.group(0), "") if year_match else user_input
    month_match = re.search(r'(\d{1,2})', clean_input)
    
    year = year_match.group(0) if year_match else None
    month_num = month_match.group(0).zfill(2) if month_match else None
    
    if not month_num:
        for num, name in MONTHS_MAP.items():
            if name in user_input.lower() or name[:3] in user_input.lower():
                month_num = num
                break
                
    return year, month_num, MONTHS_MAP.get(month_num)

# -- BUSCA --

def fetch_candidates(session, user_input):
    logger = get_run_logger()
    year, m_num, m_name = parse_human_input(user_input)
    logger.info(f"[FETCH] Buscando candidatos para: {m_name or m_num}/{year}")

    candidates = []
    start = 0
    
    while True:
        url = f"{BASE_URL}?b_start:int={start}"
        response = session.get(url, timeout=30)
        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("article", class_="entry")
        
        if not articles: break

        for article in articles:
            a_tag = article.find("a", href=True)
            if not a_tag: continue
            
            href = a_tag['href'].lower()
          
            is_file = ".csv" in href or ".xlsx" in href
            match_num = f"{year}{m_num}" in href if (year and m_num) else False
            match_text = (m_name in href and year in href) if (m_name and year) else False
            
            if is_file and (match_num or match_text):
                candidates.append(a_tag['href'])

        if len(articles) < PAGE_SIZE: break
        start += PAGE_SIZE
        time.sleep(0.5)
        
    return list(set(candidates))

# -- FILTRO --

def filter_latest_version(session, candidates):
    logger = get_run_logger()
    """Analisa os candidatos via HEAD e retorna apenas o link mais recente."""
    if not candidates:
        return None

    logger.info(f"[FILTER] Analisando metadados de {len(candidates)} arquivos...")
    best_link = None
    latest_date = datetime.min

    for link in candidates:
        try:
            # Força o endpoint de download para pegar o Last-Modified real do arquivo
            target = link.replace("/view", "/@@download/file")
            head = session.head(target, timeout=20, allow_redirects=True)
            mod_header = head.headers.get("Last-Modified")
            
            if mod_header:
                # Converte string de data do servidor para objeto datetime
                mod_date = datetime.strptime(mod_header, "%a, %d %b %Y %H:%M:%S GMT")

                if mod_date > latest_date:
                    latest_date = mod_date
                    best_link = link
            else:
                # Se o servidor não responder a data, mantemos o primeiro como fallback
                if not best_link: best_link = link
        except Exception as e:
            logger.error(f"Erro ao checar {link}: {e}")
            if not best_link: best_link = link

    return best_link

# -- DOWNLOAD --
def download_with_retry(session, file_url, max_attempts=3):

    logger = get_run_logger()
  
    download_target = file_url.replace("/view", "/@@download/file")
    filename = file_url.replace("/view", "").split("/")[-1]
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[DOWNLOAD] {filename} - Tentativa {attempt}/{max_attempts}")
            with session.get(download_target, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536): # 64KB
                        if chunk: f.write(chunk)

            logger.info(f"[SUCESSO] Download concluído: {filename}")
            return

        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            logger.warning(f"[AVISO] Conexão interrompida na tentativa {attempt}. Erro: {e}")
            time.sleep(5) # Espera um pouco antes de tentar a próxima vez

    logger.error(f"[ERRO] Não foi possível baixar {filename} após {max_attempts} tentativas.")

# --- CONVERSÃO DE TIPO DE DADOS 
@task(name="Convert to Parquet")
def convert_to_parquet(file_path,periodo):
    logger = get_run_logger()
    date = datetime.strptime(periodo, "%Y-%m")
    date_str = date.strftime("%Y-%m")
    type_file = file_path.split(".")[-1].lower()
    if "csv" in type_file:
        df_tmp = pd.read_csv(file_path,delimiter=";", encoding="latin-1")
        colunas = df_tmp.columns.tolist()
        dtype = {col: "string" for col in colunas}
        df = df_tmp.astype(dtype,errors="ignore")
        parquet_path = f"terceirizados_{date_str}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path, compression='snappy')
        logger.info(f"[CONVERSÃO] {file_path} convertido para {parquet_path}")
        return parquet_path
    elif "xlsx" in type_file:
        df_tmp = pd.read_excel(file_path)
        colunas = df_tmp.columns.tolist()
        dtype = {col: "string" for col in colunas}
        df = df_tmp.astype(dtype,errors='ignore')
        parquet_path = f"terceirizados_{periodo}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path, compression='snappy')
        logger.info(f"[CONVERSÃO] {file_path} convertido para {parquet_path}")
        return parquet_path
    else:
        raise ValueError(f"Tipo de arquivo não suportado para conversão: {type_file}")
    

# -- SEND TO GCS --
@task(name="Send to GCS")
def send_to_gcs(file_path, config):
    bucket_name = config["bucket_name"]
    destination_blob_name = f"raw/{file_path}"
    logger = get_run_logger()

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        logger.info(f"[GCS] Arquivo enviado para {bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"[ERRO] Falha ao enviar para GCS: {e}")

@task(name="Fetch and download data")
def fetch_and_download_data(periodo: str):
    # Passo 1: Busca todos os possíveis
    data = periodo.strip()
    session = get_secure_session()
    candidates = fetch_candidates(session, data)
    logger = get_run_logger()
    
    if not candidates:
        logger.error("[ERRO] Nenhum arquivo encontrado.")
        return

    # Passo 2: Filtra pela data de modificação
    target_link = filter_latest_version(session, candidates)
    
    if target_link:
        logger.info(f"\n[SUCESSO] Arquivo mais recente identificado:")
        logger.info(f" > {target_link}")
        download_with_retry(session, target_link)
    else:
        logger.error("[ERRO] Não foi possível determinar o melhor arquivo.")

    # Passo 3: Converter para Parquet
    filename = target_link.replace("/view", "").split("/")[-1]
    local_file_path = os.path.join(DOWNLOAD_DIR, filename)
    return local_file_path, data

# --- EXECUÇÃO ---
@flow(name="pipeline-raw-terceirizados")
def raw_terceirizados_flow(periodo: str):
    logger = get_run_logger()
    bucket_name, _ = RAW_BUCKET.replace("gs://", "").split("/")
    blob_target = f"raw/terceirizados_{periodo}.parquet"

    # 1. Verificação de Existência (Eficiente)
    dotenv.load_dotenv("/app/.env")  # Carrega as variáveis de ambiente do arquivo .env
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    if bucket.blob(blob_target).exists():
        logger.warning(f"[AVISO] O arquivo {blob_target} já existe. Encerrando flow.")
        return

    # 2. Processamento (Só ocorre se o arquivo NÃO existir)
    logger.info(f"[INICIO] Processando dados para o período: {periodo}")
    
    try:
        # Busca e Download
        local_file_path, data = fetch_and_download_data(periodo)
        
        if not local_file_path:
            logger.error("[ERRO] Falha ao obter o arquivo para download.")
            return

        # Conversão e Upload
        parquet_path = convert_to_parquet(local_file_path, data)
        
        # Dica: Passe o bucket ou o client já criado para o send_to_gcs
        config = load_config()
        send_to_gcs(parquet_path, config)

        logger.info(f"[SUCESSO] Arquivo {blob_target} enviado com sucesso.")

    except Exception as e:
        logger.error(f"[FALHA] Erro crítico no processamento: {str(e)}")
        raise e 


if __name__ == "__main__":
    raw_terceirizados_flow(
       periodo = REF_DATE
    )
