{{
config(
    materialized='table',
    schema='ouro',
    tags=['mart','app_terceirizados'],
)
}}

with

ultima_particao_id as (
    select max(id_tempo) as ultima_particao_id
    from {{ ref('fact_contratos_terceirizados') }}
),

orgaos_superiores_terceirizado as (
    select
        fct_contratos.id_terceirizado,
        org_sup.unidade_gestora_nome
    from {{ ref('fact_contratos_terceirizados') }} as fct_contratos
    inner join ultima_particao_id as tempo
        on fct_contratos.id_tempo = tempo.ultima_particao_id
    left join {{ ref('dim_orgaos_superiores') }} as org_sup
        on fct_contratos.id_orgao_superior = org_sup.id_orgao_superior
)

select
    dim_terc.id_terc,
    org_sup.unidade_gestora_nome,
    dim_terc.cnpj,
    dim_terc.cpf
from {{ ref('dim_terceirizados') }} as dim_terc
left join orgaos_superiores_terceirizado as org_sup
    on dim_terc.id_terc = org_sup.id_terceirizado
