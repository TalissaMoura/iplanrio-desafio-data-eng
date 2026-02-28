{{
config(
    materialized='incremental',
    schema='prata',
    tags=['core', 'fact'],
    incremental_strategy='merge',
    unique_key=['id_terceirizado_pk', 'id_tempo'] 
)
}}

SELECT 
    hash(id_terceirizado, mes_numero, ano) as id_fato,

    -- Dimensões (Chaves Estrangeiras)
    id_terceirizado,
    hash(numero_contrato) as id_contrato,
    hash(orgao_nome, orgao_sigla) as id_orgao,
    hash((ano * 100 + mes_numero)::text) as id_tempo,

    -- Métricas (Fatos)
    cast(salario_mensal_valor as float) as salario_mensal_valor, 
    cast(custo_mensal_valor as float) as custo_mensal_valor,

    -- Auditoria
    data_processamento

FROM {{ ref('brutos_terceirizados') }}

{% if is_incremental() %}
  -- No modo incremental, pegamos apenas dados novos para processar
  WHERE (ano * 100 + mes_numero) >= (SELECT max(ano * 100 + mes_numero) FROM {{ this }})
{% endif %}