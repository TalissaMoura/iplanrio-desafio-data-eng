{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','dimension'],
    unique_key='id_contrato',
    incremental_strategy='merge'
)
}}

SELECT
    numero_contrato,
    hash(numero_contrato) AS id_contrato
FROM {{ ref('brutos_terceirizados') }}
