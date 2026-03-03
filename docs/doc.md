
# Data Warehouse de Terceirizados do Governo

Este projeto tem como objetivo construir um Data Warehouse simples a partir dos dados públicos de terceirizados do governo federal.

Ele cobre todo o fluxo de dados:

- Extração dos dados públicos

- Armazenamento em Data Lake

- Transformações com DBT

Exposição dos dados por meio de uma API

## Visão Geral da Arquitetura

O projeto é dividido em:

- Data Lake (S3 / GCS)

- Transformações com DBT

- Orquestração com Prefect

- API para consumo final

Os dados são organizados em camadas, seguindo uma arquitetura de data lake de medalhão (gold, silver e bronze).

<p align="center">
  <img src="https://github.com/TalissaMoura/iplanrio-desafio-data-eng/blob/master/docs/diagrama_arq_projeto_datalake.svg" width="700"/>
</p>

## Modelos de dados
 _em /DW_

  - Staging: É a nossa camada bronze. Ela ler o arquivo parquet de um ANO-MES da camada raw do S3 e
  faz as transformações básicas de nomes e casting de dados. Os dados presentes aqui são quase identicos
  ao da raw.

  - Core: É dividida em `dimensions` e `facts`. As dimensões contem os modelos para as entidades de categoria profissional,contratos, orgaos, orgaos superiores, periodo e tercerizados, cada tabela reune as colunas que as caracterizam.

  A fato contém apenas a tabela `fact_contratos_terceirizados` e vai ser resumo das métricas de `salario_valor` e `custo_valor`. Cada terceirizado tem um contrato, orgao, orgao supeiror e uma data que possui algum valor de salario e custo.

  A camada core nesse projeto apenas separa os dados da camada bronze e os organiza de maneira mais legivel
  e fácil para consulta posterior.

  - Mart: Contem o modelo de app_tercerizados que reune as informações básicas de cada tercerizado (id_tercerizado, cnpj, cpf e sigla do orgão superior) e alimenta a API final.


  ```bash
  # Estrutura do projeto DW
      ├── dbt_packages
      ├── logs
      ├── models
      │   ├── core
      │   │   ├── dimensions
      │   │   │   ├── dim_categoria_profissional
      │   │   │   ├── dim_contratos
      │   │   │   ├── dim_orgaos
      │   │   │   ├── dim_orgaos_superiores
      │   │   │   ├── dim_periodo
      │   │   │   └── dim_terceirizados
      │   │   └── facts
      │   ├── mart
      │   └── staging
      └── target
  ```

## Pipelines:
_em pipelines/_

- raw_terceirizados (_pipelines/raw_terceirizados_) é uma pipeline apenas de extração e carga. Executa um script que
pega os dados de `https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos` e exporta para um bucket raw do google cloud platfform.

<p align="center">
  <img src="https://github.com/TalissaMoura/iplanrio-desafio-data-eng/blob/master/docs/layers_gcp_projeto.png" width="480"/>
</p>

- gov_terceirizados (_pipelines/gov_terceirizados_)): É a pipeline de extração, carregamento e transformação. Ela que constroi as
camadas do datalake (gold, silver e bronze). Ela utiliza das definições dos modelos e do DBT para
fazer as transformações e testes dos dados. Cada camada é uma pasta no bucket S3.

<p align="center">
  <img src="https://github.com/TalissaMoura/iplanrio-desafio-data-eng/blob/master/docs/dados_da_camada_raw.png" width="480"/>
</p>

## API
_em /api_

A API expõe os dados da camada Gold e possui dois endpoints:

- GET /terceirizados
Retorna a lista de terceirizados do mês mais recente disponível.

```json
--- terceirizados/
    {
      "cnpj": "31546484000798",
      "cpf": "***.606.726-**",
      "id_terceirizado": 9048072,
      "orgao_superior_sigla": "BACEN-OR"
    },
    {
      "cnpj": "31546484000798",
      "cpf": "***.986.316-**",
      "id_terceirizado": 9048073,
      "orgao_superior_sigla": "BACEN-OR"
    },
    ....
    {
      "cnpj": "31546484000798",
      "cpf": "***.183.786-**",
      "id_terceirizado": 9048074,
      "orgao_superior_sigla": "BACEN-OR"
    }

```

- GET /terceirizados/{id}
Retorna os dados de um único terceirizado pelo id_terceirizado.
```json
--- terceirizados/9048074
{
  "id_terceirizado": 9048074,
  "cpf": "***.183.786-**",
  "cnpj": "31546484000798",
  "sigla_orgao_supeior": "BACEN_OR",
}

```

### Paginação

O endpoint /terceirizados aceita os seguintes parâmetros:

   - b_start: define o offset da listagem (ex: 0, 20, 40...).

   - limit: define o número máximo de registros retornados.

Exemplo:

   ```python
   /terceirizados?b_start=20&limit=10
   ```

Retorna 10 registros a partir do 21º item da lista.

## Como rodar esse projeto?
1. Em ambiente local carregue:
         - `.env` na raiz do repositório
         - Adicione as credenciais do GCP como um arquivo json em `config/.credentials/credentials.json` e `api/config/.credentials/credentials.json`.
         - Adicione o arquivo profile.yml em `dw/.dbt/profiles.yml`

 - Para as pipelines:
    1. Para rodar a pipeline, primeiro crie o worker com `make prefect-worker`

    2. Depois, rode o comando `make prefect-deployment --DEPOLY_NAME={nome_do_deploy} e --DEPLOY_FLOW_DIR={entrypoint/path}`. Para a pipeline de `gov-tercerizados` temos `make prefect-deployment  DEPLOY_FLOW_DIR="pipelines/gov_terceirizados/flow.py:gov_terceirizados_flow" DEPLOY_NAME=gov-terceirizados`.

    3. Basta agora seguir o comando do prefect para rodar as pipelines. Adicione parametros
    para determinar o ano-mes que quer construir os dados: `prefect deployment run 'pipeline-raw-terceirizados/raw-terceirizados' --param periodo=2024-09` ou
    `prefect deployment run 'pipelines/gov_terceirizados/flow.py:gov_terceirizados_flow' --param partition=2024-09`

 - Para a API:
    1. Vá em `./api` e depois rode:
    ````bash
    -- build da imaegm
     `docker build image -t terceirizados-api . `
    -- rodar o container
      docker run \
      -p 8000:8000 \
      -v /home/talissa/iplanrio-desafio-data-eng/api/config/credentials.json:/tmp/credentials.json:ro \
      -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials.json \
      terceirizados-api

    ````
    2. Como é uma api local ela estará exposta em : `localhost:8000/terceirizados`.
    3. A doc da api está em `localhost:8000/apidocs`.

 - Para scripts:
    - `fetch_terceirizados_data.py`: É um script de que fazer o download dos dados de terceirizados localmente sem depender da pipeline. É util caso se precise rodar algo manualmente.
    - `sync_gcs`: É o script que adquire os dados do bucket S3 para o local. Pode fazer o download
    de todos os dados de todas as camadas ou apenas uma única camada sendo carregada de maneira incremental
    isto é apenas uma partição da camada.
       1. Para executar, faça o export da variavel de ambiente GOOGLE_APPLICATION_CREDENTIALS: `export
       GOOGLE_APPLICATION_CREDENTIALS=~/iplanrio-desafio-data-eng/config/.credentials/credentials.json`
       2. Para fazer o download de todas as layers e todos os dados, vá até a raiz do projeto e execute: `python sync_gcs --layers all --mode full`
       3. Para fazer o download de algumas layers , vá até a raiz do projeto e execute: `python sync_gcs --layers <nome_da_layer> --mode incremental`

## Futuras melhorias
- Adicionar mais testes de qualidade de dados: Os testes dos modelos são os básicos que podemos
definir no proprio dbt (como valores únicos, não negativos ou nulos). Futuramente, pode ser necessário
acrecentar testes que podem testar melhor o relacionamento de tabelas e também de completude dos dados.
- Melhorar a estrutura do modulo de pipelines: Possívelmente construir modulos que podem ser reutilizáveis entre as pipelines e que garantaram melhor o modelo de dados esperado para produção.
-
- Adicionar git workflows para manter a qualidade de entrega do projeto.
