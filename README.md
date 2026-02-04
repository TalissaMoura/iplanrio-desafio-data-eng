# Desafio de Data Engineer - IPLANRIO

Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados.

## Descrição do desafio

Neste desafio, você deverá desenvolver uma pipeline de ELT capaz de capturar, armazenar e transformar dados provenientes de uma API em tempo real.
A API disponibiliza informações de GPS dos veículos do BRT, retornando, a cada consulta, o último sinal transmitido por cada veículo ativo.

O objetivo é construir uma solução que capture os dados minuto a minuto, gere um único arquivo CSV contendo 10 minutos de dados, armazene-o no Google Cloud Storage (GCS), e disponibilize os dados no BigQuery por meio de uma tabela externa e de uma view transformada.

A solução deve seguir a arquitetura Medallion (Bronze → Silver → Gold).
A pipeline deverá ser construída subindo uma instância local do Prefect (em Python). Utilize a versão *1.4.1* do Prefect.

Passos adicionais e aprimoramentos são bem-vindos e estão descritos na seção [Etapas](#etapas)

## O que iremos avaliar

- Completude: A solução proposta atende a todos os requisitos do desafio?
- Simplicidade: A solução proposta é simples e direta? É fácil de entender e trabalhar?
- Organização: A solução proposta é organizada e bem documentada? É fácil de navegar e encontrar o que se procura?
- Criatividade: A solução proposta é criativa? Apresenta uma abordagem inovadora para o problema proposto?
- Arquitetura: A solução proposta implementa a arquitetura Medallion corretamente?
- Boas práticas: A solução proposta segue boas práticas de Python, Git, Docker, etc.?

## Atenção

- A solução do desafio deve estar num repositório público do Github  
- O código enviado será testado localmente, portanto organize e documente todas as etapas do projeto.
- O Google Cloud Platform oferece camada grauita para os serviços utilizados no desafio. Leia atentamente a documentação para evitar cobranças indesejadas.
- Caso avance para a próxima etapa, você deverá apresentar sua solução, explicando suas decisões técnicas.

## Etapas

1. Instalar e configurar o Prefect Server locamente com um Docker Agent
2. Construir a pipeline de captura da API do BRT com os dados minuto a minuto
3. Gerar um arquivo CSV contendo 10 minutos de dados capturados e enviá-lo ao Google Cloud Storage (GCS).
4. Criar uma tabela externa no BigQuery utilizando o DBT (instale o pacote o `dbt_external_tables`)
5. Particionar tabela gerada da forma que achar mais conveniente
6. Criar view no Google BigQuery a partir da tabela externa, aplicando as transformações necessárias

## Extras
1. Commits seguindo o padrão Conventional Commits
2. Arquivos .yml contendo descrições detalhadas de cada modelo e campo e propagação automática para o BigQuery
3. Testes de qualidade de dados no DBT
4. Estrutura de pastas e código organizada e legível
5. Instruções claras de execução no README.md

## Entregáveis esperados

O repositório público no GitHub deve conter:

### 1. Pipeline Prefect
- Código completo com **tasks** e **flows** responsáveis por:  
  - Captura de dados da API minuto a minuto  
  - Geração e envio do arquivo CSV para o GCS  
  - Execução dos modelos DBT dentro da pipeline (tabela externa e transformações)

### 2. Projeto DBT
- Modelos e `schema.yml` devidamente configurados  
- Descrições detalhadas de tabelas e campos, com **propagação automática para o BigQuery** (`+persist_docs`)

### 3. CSV de exemplo
- Arquivo gerado ou instruções claras de como reproduzir o CSV

### 4. Documentação
- `README.md` com instruções para:  
  - Iniciar o Prefect Server e Docker Agent localmente  
  - Executar o flow principal

### 5. Configuração de ambiente
- Nome do bucket GCS utilizado ou instruções para apontar para um bucket local

### 6. (Opcional)
- Capturas de tela do BigQuery mostrando:  
  - Tabela externa criada  
  - Descrições das colunas persistidas a partir do DBT

## Links de referência / utilidades

- [Google Cloud Platform - Nível gratuito](https://cloud.google.com/free?hl=pt-br)
- [Documentação Prefect v1](https://docs-v1.prefect.io/)
- [Documentação DBT](https://docs.getdbt.com/docs/introduction)
- [Instalar e configurar o Prefect Server](https://docs-v1.prefect.io/orchestration/getting-started/install.html)
- [Docker Agent - Prefect](https://docs-v1.prefect.io/orchestration/agents/docker.html)
- [API do BRT](https://www.data.rio/documents/PCRJ::transporte-rodovi%C3%A1rio-api-de-gps-do-brt/about?path=)
- [Conventional Commits](https://www.conventionalcommits.org/pt-br/v1.0.0/)
- [Pipelines rj-iplanrio](https://github.com/prefeitura-rio/pipelines_rj_iplanrio/)

## Dúvidas?

Fale conosco pelo e-mail que foi utilizado para o envio desse desafio.

