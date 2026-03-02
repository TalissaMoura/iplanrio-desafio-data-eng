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
        dim_terc.cnpj,
        dim_terc.cpf,
        dim_orgaos_superiores.orgao_superior_sigla
    from {{ ref('fact_contratos_terceirizados') }} as fct_contratos
    inner join ultima_particao_id as tempo
        on fct_contratos.id_tempo = tempo.ultima_particao_id
    inner join {{ ref('dim_terceirizados') }} as dim_terc
        on fct_contratos.id_terceirizado = dim_terc.id_terceirizado
    inner join {{ ref('dim_orgaos_superiores') }} as dim_orgaos_superiores
        on
            fct_contratos.id_orgao_superior
            = dim_orgaos_superiores.id_orgao_superior
)

select
    id_terceirizado,
    cnpj,
    cpf,
    orgao_superior_sigla
from orgaos_superiores_terceirizado
