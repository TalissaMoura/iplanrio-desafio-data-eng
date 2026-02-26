{{ config(
    materialized='table',
    schema='ouro'
) }}

{% set parquet_path = var('raw_base_path') ~ '/*.csv' %}


with source_data as (

    select *
    from read_csv('{{ parquet_path }}',delim=";",header=true,store_rejects=true)

),

renamed_and_typed as (

    select

        -- Identificador principal
        cast(id_terc as integer)                                 as id_terceirizado,

        -- Órgão superior
        sg_orgao_sup_tabela_ug                                    as orgao_superior_sigla,

        -- Unidade gestora
        cast(cd_ug_gestora as integer)                             as unidade_gestora_codigo,
        nm_ug_tabela_ug                                            as unidade_gestora_nome,
        sg_ug_gestora                                              as unidade_gestora_sigla,

        -- Contrato
        nr_contrato                                                as numero_contrato,

        -- Empresa
        cast(nr_cnpj as varchar)                                   as cnpj,
        nm_razao_social                                            as razao_social,

        -- Terceirizado
        cast(nr_cpf as varchar)                                    as cpf,
        nm_terceirizado                                            as nome_terceirizado,

        -- Categoria profissional (ex: "519940 - LEITURISTA")
        cast(split_part(nm_categoria_profissional, ' - ', 1) 
            as integer)                                             as id_categoria_profissional,

        trim(split_part(nm_categoria_profissional, ' - ', 2))       as categoria_profissional_nome,

        -- Escolaridade
        nm_escolaridade                                             as escolaridade,

        -- Jornada
        cast(nr_jornada as string)                                 as jornada_horas,

        -- Unidade de prestação
        nm_unidade_prestacao                                        as unidade_prestacao,

        -- Valores
        cast(vl_mensal_salario as decimal(18,2))                    as salario_mensal_valor,
        cast(vl_mensal_custo as decimal(18,2))                      as custo_mensal_valor,

        -- Competência
        cast(Num_Mes_Carga as integer)                              as mes_numero,
        Mes_Carga                                                   as mes_nome,
        cast(Ano_Carga as integer)                                  as ano,

        -- Órgão
        sg_orgao                                                    as orgao_sigla,
        nm_orgao                                                    as orgao_nome,
        cast(cd_orgao_siafi as integer)                             as orgao_codigo_siafi,
        cast(cd_orgao_siape as integer)                             as orgao_codigo_siape,

        -- Auditoria
        current_timestamp                                           as data_processamento

    from source_data

)

select *
from renamed_and_typed
