drop view if exists cart_enriched cascade;
drop view if exists cart_item_summary cascade;
drop view if exists cart_items cascade;

drop table if exists tb_cartentries cascade;
drop table if exists tb_carts cascade;
drop table if exists tb_addresses cascade;
drop table if exists tb_regions cascade;
drop table if exists tb_paymentinfos cascade;
drop table if exists tb_paymentmodes cascade;
drop table if exists tb_users cascade;
drop table if exists tb_cmssitelp cascade;

create table tb_paymentmodes (
    pk bigint primary key,
    p_code text
);

create table tb_paymentinfos (
    pk bigint primary key,
    p_installments integer
);

create table tb_users (
    pk bigint primary key,
    p_uid text
);

create table tb_regions (
    pk bigint primary key,
    p_isocode text,
    p_isocodeshort text
);

create table tb_addresses (
    pk bigint primary key,
    p_region bigint,
    p_postalcode text
);

create table tb_cmssitelp (
    itempk bigint primary key,
    p_name text
);

create table tb_carts (
    pk bigint primary key,
    createdts timestamp,
    p_totalprice numeric(18, 4),
    p_paymentaddress bigint,
    p_paymentinfo bigint,
    p_paymentmode bigint,
    p_user bigint,
    p_site bigint
);

create table tb_cartentries (
    pk bigint primary key,
    createdts timestamp,
    p_order bigint,
    p_entrynumber integer,
    p_product bigint,
    p_quantity numeric(18, 4),
    p_totalprice numeric(18, 4)
);

create index idx_tb_cartentries_order on tb_cartentries(p_order);
create index idx_tb_cartentries_product_month on tb_cartentries(p_product, createdts);
create index idx_tb_carts_createdts on tb_carts(createdts);
create index idx_tb_carts_paymentaddress on tb_carts(p_paymentaddress);
create index idx_tb_addresses_region on tb_addresses(p_region);
