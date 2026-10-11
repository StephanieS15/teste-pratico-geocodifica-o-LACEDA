# Geocodificação de Endereços via Elasticsearch e PySpark (CNEFE-BA)

**Autora:** Stephanie Sacramento

Solução desenvolvida para processamento, indexação e geocodificação em lote dos dados do CNEFE (IBGE - Bahia), atribuindo o setor censitário e coordenadas geográficas (latitude e longitude) a uma base de endereços de consulta.

---

## 1. Obtenção das Bases de Dados

Como a pasta `data/` é mantida fora do repositório Git por questões de limite de tamanho, os arquivos brutos devem ser baixados nos links abaixo e posicionados na pasta `data/bruto/`:

### 1.1 CNEFE Bahia (`29_BA.csv`)
Base de referência contendo os endereços e seus respectivos setores censitários:
* **URL para download 1 (IBGE):** [https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html?=&t=downloads](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html?=&t=downloads)
* **URL alternativo para download:** [https://fiocruzbr-my.sharepoint.com/:u:/g/personal/robespierre_pita_fiocruz_br/IQBffC24BhqdR6Yxlj-uwH3cAZQO3HzIQLBZocKpMmR7PkE?e=RAynjl](https://fiocruzbr-my.sharepoint.com/:u:/g/personal/robespierre_pita_fiocruz_br/IQBffC24BhqdR6Yxlj-uwH3cAZQO3HzIQLBZocKpMmR7PkE?e=RAynjl)

### 1.2 Base de busca (`2026_teste_tecnico_relinke_cidacs.xlsx`)
Arquivo contendo 100 endereços com diferentes níveis de qualidade (completos, incompletos, padronizados e não padronizados):
* **URL para download:** [https://fiocruzbr-my.sharepoint.com/:x:/g/personal/robespierre_pita_fiocruz_br/IQBk08ehW4alRIjamLlzG_w1AbZuys15b1-WD5Q2l4ni3hw?e=MZ6mD5](https://fiocruzbr-my.sharepoint.com/:x:/g/personal/robespierre_pita_fiocruz_br/IQBk08ehW4alRIjamLlzG_w1AbZuys15b1-WD5Q2l4ni3hw?e=MZ6mD5)

---

## 2. Instalação e Configuração Inicial 

### Pré-requisitos
- **Docker Engine** e **Docker Compose** instalados na máquina host.
- **VS Code** instalado com as extensões:
  - Dev Containers (`ms-vscode-remote.remote-containers`)
  - Python e Jupyter (ativadas automaticamente dentro do container).

### Passo a Passo de Instalação

1. **Clonar/Abrir o Repositório:**

   Certifique-se de que a estrutura do projeto esteja organizada da seguinte forma:

   ```text
   teste-pratico-geocodificacao-laceda/
   ├── data/
   │   └── bruto/
   │       ├── 29_BA.csv
   │       └── 2026_teste_tecnico_relinke_cidacs.xlsx
   ├── notebooks/
   │   └── 01_eda_limpeza.ipynb
   ├── scripts/
   │   ├── 02_indexacao.py
   │   └── 03_geocodificacao.py
   ├── docker-compose.yml
   ├── Dockerfile.spark
   ├── requirements.txt
   └── README.md

```

2. **Inicializar a Infraestrutura Containerizada:**
No terminal do sistema host (na raiz do projeto), execute:
```bash
docker compose up -d --build

```


*Isso irá subir o container `spark_app` (PySpark e ambiente Python) e o container `es_cidacs` (Elasticsearch v8.11).*
3. **Acessar o Terminal do Container:**
No VS Code, abra o projeto dentro do Dev Container ou abra o terminal apontando para o container ativo:
```bash
docker exec -it teste-pratico-geocodificacao-laceda-spark_app-1 bash

```



---

## 3. Ordem de Execução

O pipeline deve ser executado obrigatoriamente na ordem: `01_eda_limpeza.ipynb` -> `02_indexacao.py` -> `03_geocodificacao.py`.

```text
[01_eda_limpeza.ipynb]  ──> Gera Arquivos Parquet Limpos
          │
[02_indexacao.py]      ──> Carga dos 8,5M de registros no Elasticsearch
          │
[03_geocodificacao.py]  ──> Consulta Fuzzy + Exportação (Parquet + CSV)

```

### Etapa 01: Limpeza e Normalização dos Dados (`01_eda_limpeza.ipynb`)

* **O que o código faz:**
* Carrega a base bruta CNEFE (`29_BA.csv`) e a planilha de consulta (`.xlsx`) via PySpark/Pandas.
* Padroniza os textos: converte para maiúsculas, remove acentos, pontuações e espaços extras.
* Trata os códigos IBGE de município (6 dígitos) e os setores censitários.
* Exporta os dados limpos no formato Parquet otimizado para leitura em lotes.


* **Como Executar:**
* *Via Interface Gráfica (VS Code):* Abra o arquivo `notebooks/01_eda_limpeza.ipynb`, selecione o kernel Python do container e clique em **Run All**.
* *Via Terminal no Container:*
```bash
python3 -c "import jupyter"

```




* **Saídas Geradas em `data/processado/`:**
* `data/processado/cnefe_limpo.parquet/`
* `data/processado/busca_limpa.parquet/`
* `data/processado/busca_limpa.csv`



---

### Etapa 02: Indexação no Elasticsearch (`02_indexacao.py`)

* **O que o código faz:**
* Conecta ao Elasticsearch (`http://es_cidacs:9200`).
* Cria o índice `indice_cnefe_bahia` com mapeamento explícito:
* `keyword`: Para códigos de município, setor, CEP, número e ID único (busca exata).
* `text` (*standard analyzer*): Para logradouro e bairro (busca textual).


* Lê o diretório `cnefe_limpo.parquet` em lotes (*batches* de 10.000 registros) via `pyarrow` para consumo eficiente de RAM.
* Ingesta 8.528.506 registros no motor de busca usando a API de Carga Massiva (*Bulk API*).


* **Como Executar:**
No terminal dentro do container `spark_app`, rode:
```bash
python3 /app/scripts/02_indexacao.py

```


* **Saída Gerada:**
* Criação do índice e dados persistidos no volume Docker do Elasticsearch.



---

### Etapa 03: Geocodificação e Matching Fuzzy (`03_geocodificacao.py`)

* **O que o código faz:**
* Carrega a base de busca limpa (`busca_limpa.parquet`).
* Realiza consultas com pontuação ponderada (`bool.should`) e tolerância a erros (`fuzziness AUTO`):
* **Logradouro (Boost 3.0):** Busca *fuzzy* principal.
* **Município (Boost 2.0):** Filtro de convergência por código IBGE.
* **Bairro (Boost 1.5):** Busca *fuzzy* secundária para desempate.
* **Número Predial (Boost 1.0):** Filtro exato de numeração.


* Captura o melhor resultado (*Top 1*), extrai a latitude, longitude e padroniza o setor censitário removendo o sufixo "P" (garantindo exatos 15 dígitos numéricos).
* Renomeia as colunas conforme o gabarito oficial (`consulta_*` e `setor_censitario_encontrado`).
* Exporta o resultado em Parquet e em CSV.


* **Como Executar:**
No terminal dentro do container `spark_app`, rode:
```bash
python3 /app/scripts/03_geocodificacao.py

```


* **Saídas Geradas:**
* `data/processado/resultado_geocodificado.parquet`
* `data/processado/resultado_geocodificado.csv`



---

## 4. Validação do Resultado Final

Para visualizar o schema e o preenchimento de todas as colunas do resultado final, rode o comando abaixo no terminal:

```bash
python3 -c "
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/app/data/processado/resultado_geocodificado.parquet')

print('ESTRUTURA COMPLETA DO SCHEMA:')
df.printSchema()

print('AMOSTRA DE DADOS:')
df.show(5, truncate=False)
"

```

---

## 5. Dependências Utilizadas

* **`pyspark`**: Leitura distribuída e manipulação de DataFrames.
* **`elasticsearch` (>=8.11.0)**: Cliente nativo de conexão e consultas.
* **`pyarrow`**: Ingestão iterativa de Parquet em lotes sem estouro de memória.
* **`pandas` / `openpyxl**`: Suporte para conversão de planilhas Excel.

---

## 6. Limitações Conhecidas

* **Endereços SN (Sem Número):** Registros sem numeração predial possuem score de relevância reduzido porque perdem o peso da condição de busca exata do número, dependendo exclusivamente do match textual de bairro e município.
* **Escalabilidade:** O script de geocodificação realiza requisições HTTP individuais e sequenciais ao Elasticsearch, o que gera uma latência proporcional à quantidade de registros da base de consulta.