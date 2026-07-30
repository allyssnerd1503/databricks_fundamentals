from pathlib import Path

import pandas as pd

from cantu_abandoned_carts.loader import normalize_frame, read_pipe_csv, table_key
from cantu_abandoned_carts import queries
from cantu_abandoned_carts.pipeline import copy_chunk_to_text


def test_normalize_frame_casts_fk_columns() -> None:
    frame = pd.DataFrame(
        {
            "PK": ["1", "2"],
            "createdTS": ["2024-01-10 10:00:00", None],
            "p_paymentaddress": ["10.0", None],
            "p_totalprice": ["50.25", "nan"],
        }
    )

    normalized = normalize_frame(frame)

    assert list(normalized.columns) == ["pk", "createdts", "p_paymentaddress", "p_totalprice"]
    assert str(normalized["pk"].dtype) == "Int64"
    assert str(normalized["p_paymentaddress"].dtype) == "Int64"
    assert normalized.loc[0, "p_paymentaddress"] == 10
    assert pd.isna(normalized.loc[1, "p_paymentaddress"])
    assert normalized.loc[0, "p_totalprice"] == 50.25


def test_schema_keeps_image_relationship_columns_without_strict_fks() -> None:
    assert "from cart_items" in queries.TOP_ABANDONED_PRODUCTS.lower()
    assert "from cart_enriched" in queries.ABANDONMENT_BY_STATE.lower()

    schema = Path("sql/postgres/01_schema.sql").read_text(encoding="utf-8").lower()
    assert "p_paymentaddress bigint" in schema
    assert "p_order bigint" in schema
    assert "p_site bigint" in schema
    assert "references tb_paymentinfos" not in schema


def test_read_pipe_csv_uses_pipe_delimiter(tmp_path: Path) -> None:
    csv_path = tmp_path / "tb_regions.csv"
    csv_path.write_text(
        '"PK"|"p_isocode"|"p_isocodeshort"\n1|BR-SP|SP\n',
        encoding="utf-8",
    )

    chunks = list(read_pipe_csv(csv_path, ["PK", "p_isocode", "p_isocodeshort"], batch_size=10))

    assert chunks[0].to_dict(orient="records") == [
        {"PK": "1", "p_isocode": "BR-SP", "p_isocodeshort": "SP"}
    ]


def test_table_key_for_conflict_handling() -> None:
    assert table_key("tb_cmssitelp") == "itempk"
    assert table_key("tb_carts") == "pk"


def test_copy_chunk_to_text_accepts_memoryview() -> None:
    assert copy_chunk_to_text("abc") == "abc"
    assert copy_chunk_to_text(b"abc") == "abc"
    assert copy_chunk_to_text(memoryview(b"abc")) == "abc"
