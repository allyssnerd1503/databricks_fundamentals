TOP_ABANDONED_PRODUCTS = """
select
    product_id,
    count(distinct cart_id) as abandoned_carts,
    sum(quantity) as abandoned_items,
    sum(total_price) as unrealized_revenue
from cart_items
group by product_id
order by abandoned_carts desc, unrealized_revenue desc, product_id
limit {limit}
"""

TOP_ABANDONED_PRODUCT_PAIRS = """
with cart_products as (
    select distinct cart_id, product_id
    from cart_items
    where product_id is not null
),
pairs as (
    select
        least(a.product_id, b.product_id) as product_a,
        greatest(a.product_id, b.product_id) as product_b,
        a.cart_id
    from cart_products a
    join cart_products b
      on a.cart_id = b.cart_id
     and a.product_id < b.product_id
)
select
    product_a,
    product_b,
    count(*) as abandoned_carts
from pairs
group by product_a, product_b
order by abandoned_carts desc, product_a, product_b
limit {limit}
"""

PRODUCT_ABANDONMENT_INCREASE = """
with monthly as (
    select
        product_id,
        month,
        count(distinct cart_id) as abandoned_carts
    from cart_items
    group by product_id, month
),
ranked as (
    select
        product_id,
        month,
        abandoned_carts,
        lag(abandoned_carts) over (partition by product_id order by month) as previous_month_carts
    from monthly
)
select
    product_id,
    month,
    previous_month_carts,
    abandoned_carts,
    abandoned_carts - previous_month_carts as cart_increase,
    round(
        100.0 * (abandoned_carts - previous_month_carts) / nullif(previous_month_carts, 0),
        2
    ) as pct_increase
from ranked
where previous_month_carts is not null
  and abandoned_carts > previous_month_carts
order by cart_increase desc, pct_increase desc nulls last, product_id, month
limit {limit}
"""

NEW_PRODUCTS_FIRST_MONTH = """
with first_month as (
    select product_id, min(month) as launch_month
    from cart_items
    where product_id is not null
    group by product_id
)
select
    f.product_id,
    f.launch_month,
    count(distinct i.cart_id) as first_month_abandoned_carts,
    sum(i.quantity) as first_month_abandoned_items,
    sum(i.total_price) as first_month_unrealized_revenue
from first_month f
join cart_items i
  on i.product_id = f.product_id
 and i.month = f.launch_month
group by f.product_id, f.launch_month
order by f.launch_month, first_month_abandoned_carts desc, f.product_id
"""

ABANDONMENT_BY_STATE = """
select
    coalesce(state, 'SEM_UF') as state,
    count(distinct cart_id) as abandoned_carts,
    sum(item_quantity_sum) as abandoned_items,
    sum(cart_total_price) as unrealized_revenue
from cart_enriched
group by coalesce(state, 'SEM_UF')
order by abandoned_carts desc, unrealized_revenue desc, state
"""

MONTHLY_PRODUCT_REPORT = """
select
    product_id,
    month,
    count(distinct cart_id) as abandoned_carts,
    sum(quantity) as abandoned_items,
    sum(total_price) as unrealized_revenue
from cart_items
group by product_id, month
order by month, product_id
"""

DAILY_REPORT = """
select
    cart_date,
    count(distinct cart_id) as abandoned_carts,
    sum(item_quantity_sum) as abandoned_items,
    sum(cart_total_price) as unrealized_revenue
from cart_enriched
group by cart_date
order by cart_date
"""

TOP_CARTS_EXPORT = """
with ranked_carts as (
    select *
    from cart_enriched
    order by cart_total_price desc nulls last, cart_id
    limit {limit}
)
select
    r.cart_id,
    r.created_ts,
    r.cart_total_price,
    r.user_uid,
    r.payment_mode_code,
    r.installments,
    r.site_name,
    r.postal_code,
    r.item_quantity_sum,
    r.item_count,
    i.product_id,
    i.quantity,
    i.total_price,
    i.entry_number
from ranked_carts r
left join cart_items i on i.cart_id = r.cart_id
order by r.cart_total_price desc nulls last, r.cart_id, i.entry_number, i.product_id
"""
