{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','dimension'],
    unique_key='id_orgao',
    incremental_strategy='merge'
)
}}

SELECT 
    hash(orgao_nome,orgao_sigla) as id_orgao,
    hash(unidade_gestora_nome, unidade_gestora_codigo) as id_orgao_superior,
    orgao_nome,
    orgao_sigla,
    orgao_codigo_siafi,
    orgao_codigo_siape
FROM {{ ref('brutos_terceirizados') }}