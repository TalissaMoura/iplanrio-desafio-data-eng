{{
    config(
        materialized='table',
        schema='prata',
        tags=['core','dimension']
    )
}}

with calendario as (
    select t.data_mes
    from generate_series(
        date '2020-01-01', 
        date '2030-12-01', 
        interval '1 month'
    ) as t(data_mes)
)
select
    (extract(year from data_mes) * 100 + extract(month from data_mes))::int as id_tempo,
    extract(month from data_mes) as mes_numero,
    strftime(data_mes, '%B') as mes_nome, 
    extract(year from data_mes) as ano
from calendario