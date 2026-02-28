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
  hash(numero_contrato) as id_contrato,
  numero_contrato
FROM {{ ref('brutos_terceirizados') }}