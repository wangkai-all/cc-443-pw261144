"""Parser engine: formats, conversion, validation, extras isolation."""
import pytest

from app.plugins import SpecError, parse_record, validate_spec
from tests.conftest import spec_v1, spec_v2


def test_delimited_ok():
    oc = parse_record(spec_v1(), "ORD-1|12.50|USD|a@b.c")
    assert oc.status == "ok"
    assert oc.canonical == {"order_id": "ORD-1", "amount": 12.5,
                            "currency": "USD", "email": "a@b.c"}
    assert oc.extras == {}
    assert oc.errors == []


def test_default_applied_for_missing_value():
    oc = parse_record(spec_v1(), "ORD-1|12.50||a@b.c")
    assert oc.status == "ok"
    assert oc.canonical["currency"] == "USD"


def test_column_count_mismatch_is_error():
    oc = parse_record(spec_v1(), "ORD-1|12.50|USD")
    assert oc.status == "error"
    assert "column count" in oc.errors[0]


def test_type_conversion_failure_is_error():
    oc = parse_record(spec_v1(), "ORD-1|not-a-number|USD|a@b.c")
    assert oc.status == "error"
    assert any("conversion failed" in e for e in oc.errors)


def test_validation_range_and_regex():
    oc = parse_record(spec_v1(), "ord lowercase!|−5|USD|a@b.c")
    assert oc.status == "error"
    oc2 = parse_record(spec_v1(), "ORD-1|-5|USD|a@b.c")
    assert oc2.status == "error"
    assert any("min" in e for e in oc2.errors)


def test_empty_status_when_canonical_all_blank():
    oc = parse_record(spec_v1(), "|||")
    # order_id/amount required -> error takes precedence over empty
    assert oc.status == "error"
    spec = spec_v1()
    spec["validation"] = []  # no required checks left
    for m in spec["field_mapping"].values():
        m.pop("required", None)
        m.pop("default", None)
    oc = parse_record(spec, "|||")
    assert oc.status == "empty"


def test_unmapped_raw_fields_go_to_extras_never_canonical():
    spec = spec_v1()
    spec["parser"]["columns"].append("mystery_col")
    oc = parse_record(spec, "ORD-1|12.50|USD|a@b.c|SECRET")
    assert oc.status == "ok"
    assert "mystery_col" not in oc.canonical
    assert "loyalty_id" not in oc.canonical
    assert oc.extras == {"mystery_col": "SECRET"}


def test_v2_new_field_only_in_v2_output():
    raw = "ORD-1|12.50|USD|a@b.c|LOY-9"
    oc1 = parse_record(spec_v1(), raw)   # v1: column count mismatch -> error
    assert oc1.status == "error"
    oc2 = parse_record(spec_v2(), raw)
    assert oc2.status == "ok"
    assert oc2.canonical["loyalty_id"] == "LOY-9"


def test_keyvalue_format():
    spec = {
        "parser": {"format": "keyvalue", "pair_delimiter": ";", "kv_separator": "="},
        "field_mapping": {"id": {"target": "order_id", "type": "string"}},
        "validation": [],
    }
    oc = parse_record(spec, "id=A1; other=zzz")
    assert oc.status == "ok"
    assert oc.canonical == {"order_id": "A1"}
    assert oc.extras == {"other": "zzz"}


def test_json_format():
    spec = {
        "parser": {"format": "json"},
        "field_mapping": {"amt": {"target": "amount", "type": "decimal"}},
        "validation": [],
    }
    oc = parse_record(spec, {"amt": "3.14", "junk": 1})
    assert oc.status == "ok"
    assert oc.canonical == {"amount": 3.14}
    assert oc.extras == {"junk": 1}
    assert parse_record(spec, "not-a-dict").status == "error"


def test_spec_validation_rejects_bad_specs():
    with pytest.raises(SpecError):
        validate_spec({"parser": {"format": "nope"}, "field_mapping": {}})
    with pytest.raises(SpecError):
        validate_spec({"parser": {"format": "delimited", "delimiter": "|"},
                       "field_mapping": {"a": {"target": "x"}}})
    with pytest.raises(SpecError):  # duplicate targets
        validate_spec({"parser": {"format": "json"},
                       "field_mapping": {"a": {"target": "x"}, "b": {"target": "x"}}})
    with pytest.raises(SpecError):  # rule on unmapped field
        validate_spec({"parser": {"format": "json"},
                       "field_mapping": {"a": {"target": "x"}},
                       "validation": [{"field": "nope", "rule": "required"}]})
