create or replace view cart_items as
select
    ce.p_order as cart_id,
    ce.pk as entry_id,
    ce.p_product as product_id,
    coalesce(ce.p_quantity, 0) as quantity,
    coalesce(ce.p_totalprice, 0) as total_price,
    ce.p_entrynumber as entry_number,
    ce.createdts as entry_created_ts,
    date_trunc('month', ce.createdts)::date as month
from tb_cartentries ce
where ce.p_order is not null;

create or replace view cart_item_summary as
select
    cart_id,
    sum(quantity) as item_quantity_sum,
    count(entry_id) as item_count
from cart_items
group by cart_id;

create or replace view cart_enriched as
select
    c.pk as cart_id,
    c.createdts as created_ts,
    c.createdts::date as cart_date,
    date_trunc('month', c.createdts)::date as month,
    coalesce(c.p_totalprice, 0) as cart_total_price,
    u.p_uid as user_uid,
    pm.p_code as payment_mode_code,
    pi.p_installments as installments,
    site.p_name as site_name,
    nullif(a.p_postalcode, 'nan') as postal_code,
    r.p_isocodeshort as state,
    coalesce(s.item_quantity_sum, 0) as item_quantity_sum,
    coalesce(s.item_count, 0) as item_count
from tb_carts c
left join cart_item_summary s on s.cart_id = c.pk
left join tb_users u on u.pk = c.p_user
left join tb_paymentmodes pm on pm.pk = c.p_paymentmode
left join tb_paymentinfos pi on pi.pk = c.p_paymentinfo
left join tb_cmssitelp site on site.itempk = c.p_site
left join tb_addresses a on a.pk = c.p_paymentaddress
left join tb_regions r on r.pk = a.p_region;
