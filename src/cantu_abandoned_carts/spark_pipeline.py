from __future__ import annotations

from pathlib import Path


CSV_LOADS = [
    ("tb_regions", "tb_regions.csv"),
    ("tb_paymentmodes", "tb_paymentmodes.csv"),
    ("tb_cmssitelp", "tb_cmssitelp.csv"),
    ("tb_users", "tb_users.csv"),
]

PARQUET_LOADS = [
    ("tb_paymentinfos", "tb_paymentinfos.parquet"),
    ("tb_addresses", "tb_addresses.parquet"),
    ("tb_carts", "tb_carts.parquet"),
    ("tb_cartentries", "tb_cartentries.parquet"),
]

REPORT_QUERIES = {
    "top_produtos_abandonados": """
        select
            product_id,
            count(distinct cart_id) as abandoned_carts,
            sum(quantity) as abandoned_items,
            sum(total_price) as unrealized_revenue
        from cart_items
        group by product_id
        order by abandoned_carts desc, unrealized_revenue desc, product_id
        limit {limit}
    """,
    "top_duplas_produtos_abandonados": """
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
    """,
    "produtos_com_aumento_abandono": """
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
            round(100.0 * (abandoned_carts - previous_month_carts) / previous_month_carts, 2) as pct_increase
        from ranked
        where previous_month_carts is not null
          and previous_month_carts <> 0
          and abandoned_carts > previous_month_carts
        order by cart_increase desc, pct_increase desc, product_id, month
        limit {limit}
    """,
    "produtos_novos_primeiro_mes": """
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
    """,
    "abandonos_por_estado": """
        select
            coalesce(state, 'SEM_UF') as state,
            count(distinct cart_id) as abandoned_carts,
            sum(item_quantity_sum) as abandoned_items,
            sum(cart_total_price) as unrealized_revenue
        from cart_enriched
        group by coalesce(state, 'SEM_UF')
        order by abandoned_carts desc, unrealized_revenue desc, state
    """,
    "relatorio_produtos_mes": """
        select
            product_id,
            month,
            count(distinct cart_id) as abandoned_carts,
            sum(quantity) as abandoned_items,
            sum(total_price) as unrealized_revenue
        from cart_items
        group by product_id, month
        order by month, product_id
    """,
    "relatorio_diario": """
        select
            cart_date,
            count(distinct cart_id) as abandoned_carts,
            sum(item_quantity_sum) as abandoned_items,
            sum(cart_total_price) as unrealized_revenue
        from cart_enriched
        group by cart_date
        order by cart_date
    """,
}

TOP_CARTS_QUERY = """
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


def path_join(base: str, filename: str) -> str:
    if base.startswith("dbfs:/") or base.startswith("/"):
        return f"{base.rstrip('/')}/{filename}"
    return str(Path(base) / filename)


def load_source_tables(spark, data_dir: str) -> None:
    for table, filename in CSV_LOADS:
        (
            spark.read.option("header", True)
            .option("sep", "|")
            .option("quote", '"')
            .csv(path_join(data_dir, filename))
            .createOrReplaceTempView(table)
        )

    for table, filename in PARQUET_LOADS:
        spark.read.parquet(path_join(data_dir, filename)).createOrReplaceTempView(table)


def create_prepared_views(spark) -> None:
    spark.sql("""
        create or replace temporary view prepared_tb_regions as
        select pk, p_isocode, p_isocodeshort
        from (
            select
                cast(PK as bigint) as pk,
                p_isocode,
                p_isocodeshort,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_regions
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_paymentmodes as
        select pk, p_code
        from (
            select
                cast(PK as bigint) as pk,
                p_code,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_paymentmodes
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_cmssitelp as
        select itempk, p_name
        from (
            select
                cast(ITEMPK as bigint) as itempk,
                p_name,
                row_number() over (partition by cast(ITEMPK as bigint) order by cast(ITEMPK as bigint)) as rn
            from tb_cmssitelp
            where ITEMPK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_paymentinfos as
        select pk, p_installments
        from (
            select
                cast(PK as bigint) as pk,
                cast(p_installments as int) as p_installments,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_paymentinfos
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_users as
        select pk, p_uid
        from (
            select
                cast(PK as bigint) as pk,
                p_uid,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_users
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_addresses as
        select pk, p_region, p_postalcode
        from (
            select
                cast(PK as bigint) as pk,
                cast(p_region as bigint) as p_region,
                p_postalcode,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_addresses
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_carts as
        select pk, createdts, p_totalprice, p_paymentaddress, p_paymentinfo, p_paymentmode, p_user, p_site
        from (
            select
                cast(PK as bigint) as pk,
                cast(createdTS as timestamp) as createdts,
                cast(p_totalprice as decimal(18, 4)) as p_totalprice,
                cast(p_paymentaddress as bigint) as p_paymentaddress,
                cast(p_paymentinfo as bigint) as p_paymentinfo,
                cast(p_paymentmode as bigint) as p_paymentmode,
                cast(p_user as bigint) as p_user,
                cast(p_site as bigint) as p_site,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_carts
            where PK is not null
        )
        where rn = 1
    """)
    spark.sql("""
        create or replace temporary view prepared_tb_cartentries as
        select pk, createdts, p_order, p_entrynumber, p_product, p_quantity, p_totalprice
        from (
            select
                cast(PK as bigint) as pk,
                cast(createdTS as timestamp) as createdts,
                cast(p_order as bigint) as p_order,
                cast(p_entrynumber as int) as p_entrynumber,
                cast(p_product as bigint) as p_product,
                cast(p_quantity as decimal(18, 4)) as p_quantity,
                cast(p_totalprice as decimal(18, 4)) as p_totalprice,
                row_number() over (partition by cast(PK as bigint) order by cast(PK as bigint)) as rn
            from tb_cartentries
            where PK is not null
        )
        where rn = 1
    """)


def create_analytic_views(spark) -> None:
    spark.sql("""
        create or replace temporary view cart_items as
        select
            ce.p_order as cart_id,
            ce.pk as entry_id,
            ce.p_product as product_id,
            coalesce(ce.p_quantity, cast(0 as decimal(18, 4))) as quantity,
            coalesce(ce.p_totalprice, cast(0 as decimal(18, 4))) as total_price,
            ce.p_entrynumber as entry_number,
            ce.createdts as entry_created_ts,
            cast(date_trunc('month', ce.createdts) as date) as month
        from prepared_tb_cartentries ce
        where ce.p_order is not null
    """)
    spark.sql("""
        create or replace temporary view cart_item_summary as
        select
            cart_id,
            sum(quantity) as item_quantity_sum,
            count(entry_id) as item_count
        from cart_items
        group by cart_id
    """)
    spark.sql("""
        create or replace temporary view cart_enriched as
        select
            c.pk as cart_id,
            c.createdts as created_ts,
            cast(c.createdts as date) as cart_date,
            cast(date_trunc('month', c.createdts) as date) as month,
            coalesce(c.p_totalprice, cast(0 as decimal(18, 4))) as cart_total_price,
            u.p_uid as user_uid,
            pm.p_code as payment_mode_code,
            pi.p_installments as installments,
            site.p_name as site_name,
            nullif(a.p_postalcode, 'nan') as postal_code,
            r.p_isocodeshort as state,
            coalesce(s.item_quantity_sum, cast(0 as decimal(28, 4))) as item_quantity_sum,
            coalesce(s.item_count, 0) as item_count
        from prepared_tb_carts c
        left join cart_item_summary s on s.cart_id = c.pk
        left join prepared_tb_users u on u.pk = c.p_user
        left join prepared_tb_paymentmodes pm on pm.pk = c.p_paymentmode
        left join prepared_tb_paymentinfos pi on pi.pk = c.p_paymentinfo
        left join prepared_tb_cmssitelp site on site.itempk = c.p_site
        left join prepared_tb_addresses a on a.pk = c.p_paymentaddress
        left join prepared_tb_regions r on r.pk = a.p_region
    """)


def write_csv_report(dataframe, output_dir: str, name: str) -> None:
    (
        dataframe.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(path_join(output_dir, f"{name}.csv"))
    )


def write_top_carts_export(spark, output_dir: str, top_limit: int) -> None:
    rows = spark.sql(TOP_CARTS_QUERY.format(limit=top_limit))
    lines = []
    current_cart = None
    for row in rows.toLocalIterator():
        if row.cart_id != current_cart:
            lines.append(
                "|".join(
                    "" if value is None else str(value)
                    for value in (
                        row.cart_id,
                        row.created_ts,
                        row.cart_total_price,
                        row.user_uid,
                        row.payment_mode_code,
                        row.installments,
                        row.site_name,
                        row.postal_code,
                        row.item_quantity_sum,
                        row.item_count,
                    )
                )
            )
            current_cart = row.cart_id

        if row.product_id is not None:
            lines.append(
                "|".join(
                    "" if value is None else str(value)
                    for value in (row.product_id, row.quantity, row.total_price, "")
                )
            )

    spark.createDataFrame([(line,) for line in lines], "value string").coalesce(1).write.mode("overwrite").text(
        path_join(output_dir, "top_50_carrinhos.txt")
    )


def generate_reports(spark, output_dir: str, top_limit: int = 50) -> None:
    for name, query in REPORT_QUERIES.items():
        write_csv_report(spark.sql(query.format(limit=top_limit)), output_dir, name)
    write_top_carts_export(spark, output_dir, top_limit=top_limit)


def run(spark, data_dir: str = "doc", output_dir: str = "output", top_limit: int = 50) -> None:
    load_source_tables(spark, data_dir)
    create_prepared_views(spark)
    create_analytic_views(spark)
    generate_reports(spark, output_dir, top_limit=top_limit)
