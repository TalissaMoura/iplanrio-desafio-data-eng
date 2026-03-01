import argparse
import os
import requests
import time
import re
import sys
import yaml
from google.cloud import storage
from bs4 import BeautifulSoup
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logging.basicConfig(level=logging.INFO)
filename = os.path.basename(__file__)
logger = logging.getLogger(filename)

# --- CONFIGURAÇÕES ---
BASE_URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/"
DOWNLOAD_DIR = "downloads"
CONTROL_FILE = "file_control_log.txt"
PAGE_SIZE = 20

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))  # Adiciona o diretório pai ao sys.path

def load_config():
    with open("config/sync_gcs_config.yml", "r") as f:
        return yaml.safe_load(f)

MONTHS_MAP = {
    "01": "janeiro", "02": "fevereiro", "03": "marco", "04": "abril",
    "05": "maio", "06": "junho", "07": "julho", "08": "agosto",
    "09": "setembro", "10": "outubro", "11": "novembro", "12": "dezembro"
}

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
def convert_to_parquet(file_path,periodo):
    date = datetime.strptime(periodo, "%Y-%m")
    date_str = date.strftime("%Y-%m")
    type_file = file_path.split(".")[-1].lower()
    if "csv" in type_file:
        df_tmp = pd.read_csv(file_path)
        colunas = df_tmp.columns.tolist()
        dtype = {col: "string" for col in colunas}
        df = df_tmp.astype(dtype)
        parquet_path = f"terceirizados_{date_str}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path, compression='snappy')
        logger.info(f"[CONVERSÃO] {file_path} convertido para {parquet_path}")
        return parquet_path
    elif "xlsx" in type_file:
        df_tmp = pd.read_excel(file_path)
        colunas = df_tmp.columns.tolist()
        dtype = {col: "string" for col in colunas}
        df = df_tmp.astype(dtype)
        parquet_path = f"terceirizados_{periodo}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path, compression='snappy')
        logger.info(f"[CONVERSÃO] {file_path} convertido para {parquet_path}")
        return parquet_path
    else:
        raise ValueError(f"Tipo de arquivo não suportado para conversão: {type_file}")
    

# -- SEND TO GCS --
def send_to_gcs(file_path, config):
    bucket_name = config["bucket_name"]
    destination_blob_name = "raw"

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        logger.info(f"[GCS] Arquivo enviado para {bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"[ERRO] Falha ao enviar para GCS: {e}")

# --- EXECUÇÃO ---

def main(periodo=None):
    parser = argparse.ArgumentParser(description="Download terceirizados data in a period.")
    parser.add_argument("--periodo", type=str, help="Mês e ano do arquivo (ex: 'março 2024' ou '03/2024')")
    args = parser.parse_args()
    data = args.periodo.strip() if not periodo else periodo.strip()


    # Passo 1: Busca todos os possíveis
    session = get_secure_session()
    candidates = fetch_candidates(session, data)
    
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
    parquet_path = convert_to_parquet(local_file_path, data)

    # Passo 4: Enviar para GCS 
    # Carrega configuração do GCS
    config = load_config()
    send_to_gcs(parquet_path, config)    

    logger.info("[SUCESSO] Processo concluído com sucesso.")

if __name__ == "__main__":
    main()