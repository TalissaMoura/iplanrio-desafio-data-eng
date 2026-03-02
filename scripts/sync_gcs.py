import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
from google.cloud import storage
import sys
import os

sys.path.append(
    os.path.join(os.path.dirname(__file__), "..")
)  # Adiciona o diretório pai ao sys.path


def load_config():
    with open("config/sync_gcs_config.yml", "r") as f:
        return yaml.safe_load(f)


def create_client():
    # Usa GOOGLE_APPLICATION_CREDENTIALS automaticamente
    try:
        client = storage.Client()
        return client
    except Exception as e:
        print("❌ Erro ao criar cliente GCS:", e)
        sys.exit(1)


def validate_bucket(client, bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"✅ Conectado ao bucket: {bucket_name}")
        return bucket
    except Exception as e:
        print(f"❌ Erro ao acessar bucket {bucket_name}:", e)
        sys.exit(1)


def download_layer(bucket, prefix, local_base_path, incremental=False):
    blobs = bucket.list_blobs(prefix=prefix)

    total_downloaded = 0
    total_skipped = 0

    for blob in blobs:
        if blob.name.endswith("/"):
            continue

        local_path = Path(local_base_path) / blob.name
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if incremental and local_path.exists():
            local_modified = datetime.fromtimestamp(
                local_path.stat().st_mtime, tz=timezone.utc
            )

            if blob.updated <= local_modified:
                print(f"[SKIP] {blob.name}")
                total_skipped += 1
                continue

        blob.download_to_filename(local_path)
        print(f"[DOWNLOADED] {blob.name}")
        total_downloaded += 1

    print("\n📊 Resumo:")
    print(f"   ⬇️  Baixados: {total_downloaded}")
    print(f"   ⏭️  Ignorados: {total_skipped}")


def main():
    parser = argparse.ArgumentParser(description="Sync GCS bucket to local")
    parser.add_argument("--layer", help="bronze | prata | ouro | all", required=True)
    parser.add_argument("--mode", default="incremental", help="full | incremental")

    args = parser.parse_args()

    config = load_config()
    bucket_name = config["bucket_name"]
    layers = config["layers"]

    client = create_client()
    bucket = validate_bucket(client, bucket_name)

    local_base_path = "data"
    incremental = args.mode == "incremental"

    print("\n🚀 Iniciando sincronização")
    print(f"📦 Modo: {args.mode}")
    print("====================================")

    if args.layer == "all":
        for layer_name, prefix in layers.items():
            print(f"\n🔄 Sincronizando camada: {layer_name}")
            download_layer(bucket, prefix, local_base_path, incremental)
    else:
        prefix = layers.get(args.layer)
        if not prefix:
            print("❌ Layer inválida. Use bronze, prata, ouro ou all.")
            sys.exit(1)

        print(f"\n🔄 Sincronizando camada: {args.layer}")
        download_layer(bucket, prefix, local_base_path, incremental)

    print("\n✅ Sync finalizado com sucesso!")


if __name__ == "__main__":
    main()
