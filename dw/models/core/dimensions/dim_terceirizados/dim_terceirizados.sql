{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','dimension'],
    unique_key='id_terceirizado',
    incremental_strategy='merge'
)
}}

SELECT
  id_terceirizado, 
  cpf,
  cnpj,
  terceirizado_nome,
  razao_social,
  id_categoria_profissional,
  escolaridade
FROM {{ ref('brutos_terceirizados') }}