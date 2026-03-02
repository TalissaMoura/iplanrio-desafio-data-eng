{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','dimension'],
    unique_key='id_orgao_superior',
    incremental_strategy='merge'
)
}}

SELECT
    orgao_superior_sigla,
    unidade_gestora_codigo,
    unidade_gestora_nome,
    hash(unidade_gestora_nome, unidade_gestora_codigo) AS id_orgao_superior
FROM {{ ref('brutos_terceirizados') }}
