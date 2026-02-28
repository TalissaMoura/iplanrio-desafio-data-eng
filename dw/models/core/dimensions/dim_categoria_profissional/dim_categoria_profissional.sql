{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','dimension'],
    unique_key='id_categoria_profissional',
    incremental_strategy='merge'
)
}}

SELECT 
  id_categoria_profissional,
  categoria_profissional_nome
FROM {{ ref('brutos_terceirizados') }}