import os
import sys
import logging
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    # Conexão ao Elasticsearch
    es = Elasticsearch(
        ["http://es_cidacs:9200"],
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True
    )

    if es.ping():
        logging.info("Conexão Elasticsearch Estabelecida")
    else:
        logging.error("Falha ao conectar ao Elasticsearch")
        sys.exit(1)

    nome_indice = "indice_cnefe_bahia"

    # Estrutura do Mapping
    properties_mapping = {
        "properties": {
            "codigo_unico_endereco": {"type": "keyword"},
            "codigo_setor_censitario": {"type": "keyword"},
            "codigo_municipio": {"type": "keyword"},
            "logradouro": {
                "type": "text",
                "analyzer": "standard"
            },
            "numero": {"type": "keyword"},
            "bairro": {
                "type": "text",
                "analyzer": "standard"
            },
            "cep": {"type": "keyword"},
            "latitude": {"type": "double"},
            "longitude": {"type": "double"}
        }
    }

    #Criar Índice se não existir
    if not es.indices.exists(index=nome_indice):
        es.indices.create(index=nome_indice, mappings=properties_mapping, request_timeout=60)
        logging.info(f"Índice '{nome_indice}' criado com sucesso!")
    else:
        logging.info(f"O índice '{nome_indice}' já existe e está pronto para uso.")

    # Leitura e Carregamento
    caminho_parquet_dir = "/app/data/processado/cnefe_limpo.parquet"
    
    if not os.path.exists(caminho_parquet_dir):
        logging.error(f"Diretório {caminho_parquet_dir} não encontrado. Execute 01_eda_limpeza.ipynb")
        sys.exit(1)

    arquivos_parquet = [
        os.path.join(caminho_parquet_dir, f) 
        for f in os.listdir(caminho_parquet_dir) 
        if f.endswith(".parquet") and not f.startswith(".")
    ]

    logging.info(f"Localizados {len(arquivos_parquet)} arquivo(s) Parquet.")

    sucesso_total = 0
    falhas_total = 0
    processados = 0

    for idx, arq in enumerate(arquivos_parquet, 1):
        logging.info(f"Processando parte {idx}/{len(arquivos_parquet)}: {os.path.basename(arq)}...")
        parquet_file = pq.ParquetFile(arq)
        
        for batch in parquet_file.iter_batches(batch_size=10000):
            df_bloco = batch.to_pandas()

            def gerar_acoes(df):
                for record in df.to_dict(orient="records"):
                    record_clean = {k: v for k, v in record.items() if v == v and v is not None}
                    yield {
                        "_index": nome_indice,
                        "_id": record_clean.get("codigo_unico_endereco"),
                        "_source": record_clean
                    }

            sucesso, falhas = bulk(
                es,
                gerar_acoes(df_bloco),
                chunk_size=5000,
                request_timeout=120,
                stats_only=True
            )

            sucesso_total += sucesso
            falhas_total += falhas
            processados += len(df_bloco)

            logging.info(f"  -> Progresso acumulado: {processados:,} registros processados...")

    es.indices.refresh(index=nome_indice)
    total_confirmado = es.count(index=nome_indice)["count"]

    logging.info("INDEXAÇÃO CONCLUÍDA COM SUCESSO!")
    logging.info(f"Documentos confirmados no índice: {total_confirmado:,}")

if __name__ == "__main__":
    main()