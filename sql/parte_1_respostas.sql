-- 1.1 Campeonato
with pontuacao as (
    select
        mandante_time as time_id,
        case
            when mandante_gols > visitante_gols then 3
            when mandante_gols = visitante_gols then 1
            else 0
        end as pontos
    from jogos
    union all
    select
        visitante_time as time_id,
        case
            when visitante_gols > mandante_gols then 3
            when visitante_gols = mandante_gols then 1
            else 0
        end as pontos
    from jogos
)
select
    t.time_id,
    t.time_nome,
    coalesce(sum(p.pontos), 0) as num_pontos
from times t
left join pontuacao p on p.time_id = t.time_id
group by t.time_id, t.time_nome
order by num_pontos desc, t.time_id;

-- 1.2 Comissoes
-- Se um vendedor tem ate 3 comissoes cuja soma chega a 1024, entao a soma das suas
-- 3 maiores comissoes tambem chega a 1024.
with rank_comissoes as (
    select
        vendedor,
        valor,
        row_number() over (partition by vendedor order by valor desc) as rn
    from comissoes
)
select vendedor
from rank_comissoes
where rn <= 3
group by vendedor
having sum(valor) >= 1024
order by vendedor;

-- 1.3 Organizacao Empresarial
with recursive hierarquia as (
    select
        c.id as colaborador_id,
        c.salario as colaborador_salario,
        l.id as lider_id,
        l.salario as lider_salario,
        1 as distancia
    from colaboradores c
    left join colaboradores l on l.id = c.lider_id

    union all

    select
        h.colaborador_id,
        h.colaborador_salario,
        l.id as lider_id,
        l.salario as lider_salario,
        h.distancia + 1 as distancia
    from hierarquia h
    join colaboradores chefe_atual on chefe_atual.id = h.lider_id
    join colaboradores l on l.id = chefe_atual.lider_id
)
select
    c.id,
    (
        select h.lider_id
        from hierarquia h
        where h.colaborador_id = c.id
          and h.lider_salario >= 2 * c.salario
        order by h.distancia asc
        limit 1
    ) as lider_id
from colaboradores c
order by c.id;

