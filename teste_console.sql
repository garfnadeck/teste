-- Transformar a tabela de vendas particionada por ano. Lembre-se de
-- verificar todos os anos possíveis para criar as partições de forma
-- correta;
create table public.sale_read
(
    id          integer      not null,
    id_customer integer      not null,
    id_branch   integer      not null,
    id_employee integer      not null,
    date        timestamp(6) not null,
    created_at  timestamp    not null,
    modified_at timestamp    not null,
    active      boolean      not null
) partition by range (date);

do
$$
    declare
        ano     integer;
        comando varchar;
    begin
        for ano in 1970..2021
            loop
                comando := format('create table sale_read_%s partition of sale_read for values from (%s) to (%s);',
                                  ano,
                                  quote_literal(concat(ano::varchar, '-01-01 00:00:00.000000')),
                                  quote_literal(concat(ano::varchar, '-12-31 23:59:59.999999'))
                    );
                execute comando;
            end loop;
    end;
$$;

create or replace function fn_popular_sale_read() returns trigger as
$$
begin
    insert into sale_read(id, id_customer, id_branch, id_employee, date, created_at, modified_at, active)
    values (new.id, new.id_customer, new.id_branch, new.id_employee, new.date, new.created_at, new.modified_at,
            new.active);
    return new;
end;
$$
    language plpgsql;

create trigger tg_popular_sale_read_update
    after update
    on sale
    for each row
execute function fn_popular_sale_read();

do
$$
    declare
        consulta record;
    begin
        for consulta in select * from sale
            loop
                update sale set id_customer = id_customer where id = consulta.id;
            end loop;
    end;
$$;

--------------------------------------------------------------------------------------
-- 1 - Criar o banco de dados;
CREATE DATABASE delegacia;


--2 Criando o DDL para estrutura das tabelas
CREATE TABLE pessoa
(
    id              serial primary key not null,
    nome            varchar(104)       not null,
    cpf             varchar(11)        not null,
    telefone        varchar(11)        not null,
    data_nascimento DATE               not null,
    endereco        varchar(256)       not null,
    ativo           boolean            not null,
    criado_em       timestamp          not null,
    modificado_em   timestamp          not null
);

CREATE TABLE arma
(
    id            serial primary key not null,
    numero_serie  varchar(104),
    descricao     varchar(256)       not null,
    tipo          varchar(1)         not null,
    ativo         boolean            not null,
    criado_em     timestamp          not null,
    modificado_em timestamp          not null
);

CREATE TABLE tipo_crime
(
    id                  serial primary key not null,
    nome                varchar(104)       not null,
    tempo_minimo_prisao smallint,
    tempo_maximo_prisao smallint,
    tempo_prescricao    smallint,
    ativo               boolean            not null,
    criado_em           timestamp          not null,
    modificado_em       timestamp          not null
);

CREATE TABLE crime
(
    id            serial primary key not null,
    id_tipo_crime integer            not null,
    data          TIMESTAMP          not null,
    local         varchar(256)       not null,
    observacao    text,
    ativo         boolean            not null,
    criado_em     timestamp          not null,
    modificado_em timestamp          not null,
    constraint fk_crime_tipo_crime foreign key (id_tipo_crime) references tipo_crime (id)
);

CREATE TABLE crime_pessoa
(
    id            serial primary key not null,
    id_pessoa     integer            not null,
    id_crime      integer            not null,
    tipo          varchar(1)         not null,
    ativo         boolean            not null,
    criado_em     timestamp          not null,
    modificado_em timestamp          not null,
    constraint fk_pessoa_crime_pessoa foreign key (id_pessoa) references pessoa (id),
    constraint fk_crime_crime_pessoa foreign key (id_crime) references crime (id)
);

CREATE TABLE crime_arma
(
    id            serial primary key not null,
    id_arma       integer            not null,
    id_crime      integer            not null,
    ativo         boolean            not null,
    criado_em     timestamp          not null,
    modificado_em timestamp          not null,
    constraint fk_arma_crime_arma foreign key (id_arma) references arma (id),
    constraint fk_crime_crime_arma foreign key (id_crime) references crime (id)
);


-- 3 Criar um script para criar armas de forma automática, seguindo os
-- seguintes critérios: O número de série da arma deve ser gerado por o UUID,
-- os tipos de armas são, 0 - Arma de fogo, 1 - Arma branca, 2 - Outros.

create extension if not exists "uuid-ossp";

INSERT INTO arma (numero_serie, descricao, tipo, ativo, criado_em, modificado_em)
VALUES (uuid_generate_v4(), 'gun', random() * 3, true, now(), now());

do
$$
    declare
        id integer := 0;

    begin
        case armas_fogo then ('metralhadora', 'desert eagle', 'rocket launcher');
        armas_brancas then ('faca','canivete','machado');
        else('caco de vidro');
        loop
            gun_type[armas_fogo, armas_brancas, outros];
            return 'INSERT INTO' || arma ||
                   '(numero_serie, descricao, tipo, ativo, criado_em, modificado_em) VALUES  (uuid_generate_v4, random(gun_type) ,random()*3,true, now(), now())';
            id := 1;
        end loop;

    end;

$$
language plpgsql;

do
$$
    declare
        consulta1 record;
        consulta2 record;
    begin
        for consulta1 in (
            (select *
             from dblink('dbname=sale port=5433 host=localhost user=postgres password=123456',
                         'select * from customer',
                         true) as (id integer,
                                   id_district integer,
                                   id_marital_status integer,
                                   name varchar,
                                   income numeric,
                                   gender varchar,
                                   created_at timestamp,
                                   modified_at timestamp,
                                   active boolean
                 )))
            loop
                insert into pessoa (nome, cpf, telefone, data_nascimento, endereco)
                values (consulta2.name, (SELECT floor(random() * 99999999999)),
                        (SELECT floor(random() * 99999999999)),
                        (SELECT current_date + round(random() * 365)::int * '1 day'::interval AS data),
                        uuid_generate_v4()::varchar);
            end loop;
        for consulta2 in (
            select *
            from dblink('dbname=sale port=5433 host=localhost user=postgres password=123456',
                        'select * from employee',
                        true) as (id integer,
                                  id_department integer,
                                  id_district integer,
                                  id_marital_status integer,
                                  name varchar,
                                  salary numeric,
                                  admission_date date,
                                  birth_date date,
                                  gender varchar,
                                  created_at timestamp,
                                  modified_at timestamp,
                                  active boolean
                ))
            loop
                insert into pessoa (nome, cpf, telefone, data_nascimento, endereco)
                values (consulta2.name, (SELECT floor(random() * 99999999999)),
                        (SELECT floor(random() * 99999999999)),
                        (SELECT current_date + round(random() * 365)::int * '1 day'::interval AS data),
                        uuid_generate_v4()::varchar);
            end loop;
    end;
$$
language plpgsql;


SELECT chr((round(random() * 94) + 32)::int);

SELECT (random() * 10 + 1);


SELECT random_between(1, 100)
FROM generate_series(1, 5);

    (SELECT floor(random() * 99999999999)