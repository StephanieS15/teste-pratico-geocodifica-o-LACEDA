import logging
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def consultar_elasticsearch(row_dict, es, nome_indice="indice_cnefe_bahia"):
    should_conditions = []

    logradouro = row_dict.get("logradouro")
    if logradouro and str(logradouro).strip():
        should_conditions.append({
            "match": {
                "logradouro": {
                    "query": str(logradouro),
                    "boost": 3.0,
                    "fuzziness": "AUTO"
                }
            }
        })

    municipio = row_dict.get("codigo_municipio")
    if municipio and str(municipio).strip():
        should_conditions.append({
            "term": {
                "codigo_municipio": {
                    "value": str(municipio),
                    "boost": 2.0
                }
            }
        })

    bairro = row_dict.get("bairro")
    if bairro and str(bairro).strip():
        should_conditions.append({
            "match": {
                "bairro": {
                    "query": str(bairro),
                    "boost": 1.5,
                    "fuzziness": "AUTO"
                }
            }
        })

    numero = row_dict.get("numero")
    if numero and str(numero).strip():
        should_conditions.append({
            "term": {
                "numero": {
                    "value": str(numero),
                    "boost": 1.0
                }
            }
        })

    fallback_result = {
        "consulta_logradouro": row_dict.get("logradouro"),
        "consulta_numero": row_dict.get("numero"),
        "consulta_bairro": row_dict.get("bairro"),
        "consulta_cep": row_dict.get("cep"),
        "consulta_complemento": row_dict.get("complemento"),
        "consulta_municipio": row_dict.get("codigo_municipio"),
        "setor_censitario_encontrado": None,
        "match_score": None,
        "latitude": None,
        "longitude": None
    }

    if not should_conditions:
        return fallback_result

    query_body = {
        "bool": {
            "should": should_conditions,
            "minimum_should_match": 1
        }
    }

    try:
        resposta = es.search(index=nome_indice, query=query_body, size=1)
        hits = resposta["hits"]["hits"]
        if hits:
            melhor = hits[0]
            fonte = melhor["_source"]
            
            raw_setor = str(fonte.get("codigo_setor_censitario") or "")
            setor_limpo = "".join(filter(str.isdigit, raw_setor))

            return {
                "consulta_logradouro": row_dict.get("logradouro"),
                "consulta_numero": row_dict.get("numero"),
                "consulta_bairro": row_dict.get("bairro"),
                "consulta_cep": row_dict.get("cep"),
                "consulta_complemento": row_dict.get("complemento"),
                "consulta_municipio": row_dict.get("codigo_municipio"),
                "setor_censitario_encontrado": setor_limpo,
                "match_score": melhor["_score"],
                "latitude": fonte.get("latitude"),
                "longitude": fonte.get("longitude")
            }
    except Exception as e:
        logging.warning(f"Erro ao consultar registro {row_dict}: {e}")

    return fallback_result

def main():
    spark = SparkSession.builder \
        .appName("Geocodificacao_Lote") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    es = Elasticsearch(["http://es_cidacs:9200"], request_timeout=30)

    caminho_busca = "/app/data/processado/busca_limpa.parquet"
    
    logging.info(f"Lendo base de busca com PySpark: {caminho_busca}")
    df_busca = spark.read.parquet(caminho_busca)
    
    registros = [row.asDict() for row in df_busca.collect()]
    logging.info(f"Total de registros a geocodificar: {len(registros)}")

    resultados = []
    for idx, reg in enumerate(registros, 1):
        res = consultar_elasticsearch(reg, es)
        resultados.append(res)
        if idx % 100 == 0 or idx == len(registros):
            logging.info(f"Progresso: {idx}/{len(registros)} processados.")

    df_resultado = spark.createDataFrame(resultados)
    
    #Exportação em Parquet 
    caminho_parquet = "/app/data/processado/resultado_geocodificado.parquet"
    df_resultado.write.mode("overwrite").parquet(caminho_parquet)
    logging.info(f"Resultado Parquet salvo em: {caminho_parquet}")

    #Exportação em CSV
    caminho_csv = "/app/data/processado/resultado_geocodificado.csv"
    df_resultado.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminho_csv)
    logging.info(f"Resultado CSV consolidado salvo em: {caminho_csv}")

    logging.info("GEOCODIFICAÇÃO E EXPORTAÇÃO CONCLUÍDAS COM SUCESSO!")
    spark.stop()

if __name__ == "__main__":
    main()