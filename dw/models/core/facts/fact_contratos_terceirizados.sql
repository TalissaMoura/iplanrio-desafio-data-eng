{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core', 'fact'],
    incremental_strategy='merge',
    unique_key=['id_fato'] 
)
}}

with source as (
    select
    
        id_terceirizado,
        (ano * 100 + mes_numero)::int as id_tempo,
        salario_mensal_valor,
        custo_mensal_valor,
        data_processamento,


        hash(numero_contrato) as id_contrato,
        hash(orgao_nome, orgao_sigla) as id_orgao,


        hash(unidade_gestora_nome, unidade_gestora_codigo) as id_orgao_superior

    from {{ ref('brutos_terceirizados') }}
)

select
    s.*,
    hash(s.id_terceirizado, s.id_contrato, s.id_orgao, s.id_tempo) as id_fato
from source as s

{% if is_incremental() %}
    -- No modo incremental, pegamos apenas dados novos para processar
    where
        (s.ano * 100 + s.mes_numero)
        >= (select max(ano * 100 + mes_numero) from {{ this }})
{% endif %}
