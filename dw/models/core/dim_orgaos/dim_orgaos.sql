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
    orgao_nome,
    orgao_sigla,
    orgao_codigo_siafi,
    orgao_codigo_siape,
    hash(orgao_nome, orgao_sigla) AS id_orgao,
    hash(unidade_gestora_nome, unidade_gestora_codigo) AS id_orgao_superior
FROM {{ ref('brutos_terceirizados') }}
