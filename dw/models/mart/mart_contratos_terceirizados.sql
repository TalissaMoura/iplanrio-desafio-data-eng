{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core','fact'],
    incremental_strategy='merge',
    unique_key=['id_terc','id_contrato','id_orgao','id_tempo']
)
}}

select
    id_terc,
    id_contrato,
    id_orgao,
    id_tempo,
    custo_mensal_valor as valor_custo,
    salario_mensal_valor as valor_salario

from {{ ref('brutos_terceirizados') }}
