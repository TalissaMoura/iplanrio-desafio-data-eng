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
  SELECT 
      -- Dimensões (Chaves Estrangeiras)
      id_terceirizado,
      hash(numero_contrato) as id_contrato,
      hash(orgao_nome, orgao_sigla) as id_orgao,
      hash(unidade_gestora_nome,unidade_gestora_codigo) as id_orgao_superior,
      (ano * 100 + mes_numero)::int as id_tempo,

      -- Métricas (Fatos)
      cast(salario_mensal_valor as float) as salario_mensal_valor, 
      cast(custo_mensal_valor as float) as custo_mensal_valor,

      -- Auditoria
      data_processamento

  FROM {{ ref('brutos_terceirizados') }}
)


select 
  hash(id_terceirizado, id_contrato, id_orgao,id_tempo) as id_fato,
  s.*
from source s

{% if is_incremental() %}
  -- No modo incremental, pegamos apenas dados novos para processar
  WHERE (ano * 100 + mes_numero) >= (SELECT max(ano * 100 + mes_numero) FROM {{ this }})
{% endif %}